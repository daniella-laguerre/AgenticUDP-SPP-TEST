// Package transport implements the AgenticUDP V2 client for the System Physics
// Framework. This is the sender side of Path B:
//
//	Framework → AgenticUDP → Core AgenticUDP Receiver → Pipeline → ClickHouse
//
// The transport wraps serialized telemetry into V2 datagrams, handles
// session handshake, keepalive, ACKs, retransmission, and tier-aware
// delivery guarantees.
package transport

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	sppv1 "github.com/entropyops/entropyops-v2/pkg/sppv1"
	"google.golang.org/protobuf/proto"
)

// V2 wire protocol constants — must match C++ AgenticUDP_Shared.h and
// the Go server receiver at entropyops-v2/internal/ingest/receiver/agenticudp.go.
const (
	protoV2      = 2
	headerSize   = 14
	maxPayload   = 65000
	pktData      = 1
	pktACK       = 2
	pktNACK      = 3
	pktKeepalive = 5
	pktHandshake = 6
	pktFIN       = 7
	pktMetrics   = 10
	pktConfig    = 11

	tierGuaranteed = 0
	tierReliable   = 1
	tierBesteff    = 2

	flagProtobuf = 0x40 // payload is spp.v1.Envelope protobuf binary
)

// Tier represents an AgenticUDP reliability tier.
type Tier uint8

const (
	TierGuaranteed Tier = tierGuaranteed
	TierReliable   Tier = tierReliable
	TierBesteff    Tier = tierBesteff
)

// Client is a Go AgenticUDP V2 sender that connects to the Core's
// AgenticUDP receiver over UDP, optionally wrapped in DTLS.
type Client struct {
	conn     net.Conn     // raw UDP or DTLS-wrapped connection
	rawConn  *net.UDPConn // underlying UDP (nil when DTLS manages the socket)
	addr     *net.UDPAddr
	tenantID string

	// DTLS configuration (nil = cleartext)
	dtlsCfg *DTLSClientConfig

	// Wire format: true = protobuf (spp.v1.Envelope), false = JSON
	UseProtobuf bool

	// Session state
	established atomic.Bool
	seqCounter  atomic.Uint32

	// Inflight tracking for ACK-based tiers
	inflight   map[uint32]*inflightPkt
	inflightMu sync.Mutex

	// Config callback — called when Core pushes OTEL config
	onConfig   func(yamlBytes []byte)
	onConfigMu sync.Mutex

	// Stats
	sent        atomic.Uint64
	acked       atomic.Uint64
	retransmits atomic.Uint64
	dropped     atomic.Uint64

	// AI modules (nil = disabled, set via EnableAI)
	aiClassifier        *AIClassifier
	featureExtractor    *FeatureExtractor
	congestionPredictor *CongestionPredictor
	lastRTTMs           atomic.Int64

	// Maximum unACKed datagrams allowed in the inflight window during
	// chunked bulk sends. 0 = unbounded (legacy behavior). Without this
	// cap a chunked SendCycle of hundreds of datagrams will burst the
	// receiver's UDP socket buffer, triggering a retransmit storm. Set
	// at construction time from ENTROPYOPS_AGENT_UDP_MAX_INFLIGHT
	// (default 64). Use SetMaxInflight to change it programmatically.
	//
	// maxInflight is the EFFECTIVE cap used by waitForInflightWindow;
	// baseMaxInflight is the operator-configured ceiling. When a
	// CongestionPredictor is attached, the retransmit loop updates
	// maxInflight to a fraction of baseMaxInflight based on predicted
	// network conditions while never exceeding the configured ceiling.
	maxInflight     atomic.Int64
	baseMaxInflight atomic.Int64

	// Retransmit pacing. baseRTOMs is the first-retry delay for an
	// unACKed datagram; subsequent retries double up to maxRTOMs
	// (capped exponential backoff). maxRetransmitBudget caps how many
	// datagrams the retransmit loop is allowed to put back on the wire
	// in a single ticker tick — without this cap, a transient
	// receiver-side stall converts O(N) inflight packets into an O(N)
	// retransmit burst per tick, which is the storm pattern observed in
	// the n=1000 server-side log analysis (9,215 flushes for 5M spans;
	// avg flush 5.9s with max 70s when the original send pattern would
	// have produced ~2,500 flushes). Defaults are sourced from the
	// ENTROPYOPS_AGENT_UDP_RETRANSMIT_{MS,MAX_MS,BUDGET} env vars or
	// the built-in defaults below; SetBaseRTO / SetMaxRTO /
	// SetRetransmitBudget tweak them at runtime (e.g. from the bench
	// tool's -udp-retransmit-ms flag).
	baseRTOMs           atomic.Int64
	maxRTOMs            atomic.Int64
	maxRetransmitBudget atomic.Int64

	cancel context.CancelFunc
}

type inflightPkt struct {
	data     []byte
	seq      uint16
	streamID uint16
	tier     Tier
	sentAt   time.Time
	retries  int
}

// defaultMaxInflightFromEnv returns the value of ENTROPYOPS_AGENT_UDP_MAX_INFLIGHT
// (number of unACKed datagrams allowed at any time during chunked bulk
// sends), or 64 if unset/unparseable. Set to 0 to disable the cap.
func defaultMaxInflightFromEnv() int64 {
	raw := strings.TrimSpace(os.Getenv("ENTROPYOPS_AGENT_UDP_MAX_INFLIGHT"))
	if raw == "" {
		return 64
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return 64
	}
	return v
}

// Retransmit-loop defaults. These were chosen so a packet's lifetime in
// the inflight map is bounded:
//
//	guaranteed tier (max 10 retries): 2 + 4 + 8 + 16 + 30*6 ≈ 210s
//	reliable / besteff (max 3 retries): 2 + 4 + 8 = 14s
//
// The budget keeps the per-tick retransmit burst small so a transient
// receiver stall does not get amplified into an O(N) blast.
const (
	defaultBaseRTOMs             int64 = 2000
	defaultMaxRTOMs              int64 = 30000
	defaultMaxRetransmitsPerTick int64 = 64
)

// envInt64 reads an int64 env var with a fallback. Whitespace, empty
// strings, parse errors, and non-positive values all fall back to def.
// Non-positive values would either disable retransmits entirely
// (baseRTO=0 → divide-by-zero-ish behavior in the loop) or disable the
// budget (budget=0 → silent regression to storm semantics), so we
// prefer the safer default and let SetXxx accept programmatic
// overrides if a caller really wants 0.
func envInt64Positive(key string, def int64) int64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		return def
	}
	return v
}

func defaultBaseRTOFromEnv() int64 {
	return envInt64Positive("ENTROPYOPS_AGENT_UDP_RETRANSMIT_MS", defaultBaseRTOMs)
}

func defaultMaxRTOFromEnv() int64 {
	return envInt64Positive("ENTROPYOPS_AGENT_UDP_RETRANSMIT_MAX_MS", defaultMaxRTOMs)
}

func defaultRetransmitBudgetFromEnv() int64 {
	return envInt64Positive("ENTROPYOPS_AGENT_UDP_RETRANSMIT_BUDGET", defaultMaxRetransmitsPerTick)
}

// computeRTO returns the per-packet retransmit timeout for a datagram
// that has already been retried `retries` times (0 = first retry).
// The timeout doubles each retry and is capped at maxRTOMs; this is
// the standard "exponential backoff with ceiling" used by TCP and most
// reliable UDP protocols. It exists so the retransmit loop can ask
// "is this packet's RTO elapsed?" without tracking per-packet timers.
//
// Defensive on inputs: zero or negative base/max collapse to the
// built-in defaults so a misconfigured Client cannot accidentally
// retransmit every tick (baseRTO=0) or never retransmit (maxRTO<0).
// The shift is guarded against int overflow at retries >= 31.
func computeRTO(baseRTOMs, maxRTOMs int64, retries int) time.Duration {
	if baseRTOMs <= 0 {
		baseRTOMs = defaultBaseRTOMs
	}
	if maxRTOMs <= 0 || maxRTOMs < baseRTOMs {
		maxRTOMs = baseRTOMs
	}
	if retries < 0 {
		retries = 0
	}
	var rtoMs int64
	if retries >= 31 {
		rtoMs = maxRTOMs
	} else {
		rtoMs = baseRTOMs << uint(retries)
		if rtoMs > maxRTOMs || rtoMs < baseRTOMs {
			rtoMs = maxRTOMs
		}
	}
	return time.Duration(rtoMs) * time.Millisecond
}

// NewClient creates a new AgenticUDP transport client (cleartext).
// host is the core server address, e.g. "core.entropyops.io:4320".
func NewClient(host string, tenantID string) (*Client, error) {
	addr, err := net.ResolveUDPAddr("udp", host)
	if err != nil {
		return nil, fmt.Errorf("agenticudp: resolve %s: %w", host, err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, fmt.Errorf("agenticudp: dial %s: %w", host, err)
	}

	c := &Client{
		conn:     conn,
		rawConn:  conn,
		addr:     addr,
		tenantID: tenantID,
		inflight: make(map[uint32]*inflightPkt),
	}
	cap := defaultMaxInflightFromEnv()
	c.maxInflight.Store(cap)
	c.baseMaxInflight.Store(cap)
	c.baseRTOMs.Store(defaultBaseRTOFromEnv())
	c.maxRTOMs.Store(defaultMaxRTOFromEnv())
	c.maxRetransmitBudget.Store(defaultRetransmitBudgetFromEnv())
	return c, nil
}

// SetMaxInflight configures the upper bound on unACKed datagrams during
// chunked bulk sends. 0 disables the cap (legacy fire-and-forget burst).
// The cap protects the receiver's UDP socket buffer from overflow when the
// agent emits hundreds of chunks in tight succession.
func (c *Client) SetMaxInflight(n int) {
	if n < 0 {
		n = 0
	}
	c.maxInflight.Store(int64(n))
}

// SetBaseRTO sets the first-retry timeout (ms) for unACKed datagrams.
// Subsequent retries double the wait up to the maxRTO ceiling. ms must
// be > 0; non-positive values are silently ignored to avoid disabling
// retransmits altogether (which is what would happen with a 0 RTO —
// every packet would retransmit on every tick).
func (c *Client) SetBaseRTO(ms int) {
	if ms <= 0 {
		return
	}
	c.baseRTOMs.Store(int64(ms))
}

// SetMaxRTO sets the upper bound (ms) on per-packet retransmit timeout
// after exponential backoff. Caller must ensure ms >= baseRTO; the
// retransmit loop will silently raise maxRTO to baseRTO if not. ms must
// be > 0; non-positive values are silently ignored.
func (c *Client) SetMaxRTO(ms int) {
	if ms <= 0 {
		return
	}
	c.maxRTOMs.Store(int64(ms))
}

// SetRetransmitBudget caps how many datagrams the retransmit loop is
// allowed to put back on the wire in a single ticker tick. Smaller
// values smooth the retransmit pacing; larger values allow faster
// recovery from a brief stall at the cost of bigger bursts. Must be > 0;
// non-positive values are silently ignored to prevent accidental
// regression to the unbounded-burst behavior that produced the n=1000
// retransmit storm.
func (c *Client) SetRetransmitBudget(n int) {
	if n <= 0 {
		return
	}
	c.maxRetransmitBudget.Store(int64(n))
}

// waitForInflightWindow blocks while the number of unACKed datagrams is
// at or above the configured cap. Returns immediately when the cap is 0
// (disabled). Used between chunks of a bulk send to prevent receiver-side
// buffer overflow.
func (c *Client) waitForInflightWindow(ctx context.Context, deadline time.Time) error {
	cap := c.maxInflight.Load()
	if cap <= 0 {
		return nil
	}
	for {
		c.inflightMu.Lock()
		n := int64(len(c.inflight))
		c.inflightMu.Unlock()
		if n < cap {
			return nil
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return fmt.Errorf("agenticudp: inflight window stalled (>=%d unacked) past deadline", cap)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Microsecond):
		}
	}
}

// NewClientDTLS creates a new AgenticUDP transport client with DTLS encryption.
func NewClientDTLS(host string, tenantID string, dtlsCfg DTLSClientConfig) (*Client, error) {
	addr, err := net.ResolveUDPAddr("udp", host)
	if err != nil {
		return nil, fmt.Errorf("agenticudp: resolve %s: %w", host, err)
	}

	c := &Client{
		addr:     addr,
		tenantID: tenantID,
		dtlsCfg:  &dtlsCfg,
		inflight: make(map[uint32]*inflightPkt),
	}
	c.maxInflight.Store(defaultMaxInflightFromEnv())
	c.baseRTOMs.Store(defaultBaseRTOFromEnv())
	c.maxRTOMs.Store(defaultMaxRTOFromEnv())
	c.maxRetransmitBudget.Store(defaultRetransmitBudgetFromEnv())
	return c, nil
}

// doHandshake performs the DTLS setup (if configured) and the AgenticUDP
// session handshake. ClientV3.Connect calls this directly so both versions
// share the same session-establishment logic without duplicating it.
func (c *Client) doHandshake(ctx context.Context) error {
	if c.dtlsCfg != nil && c.dtlsCfg.Mode != DTLSModeNone {
		dtlsConn, err := dialDTLS(ctx, c.addr, *c.dtlsCfg)
		if err != nil {
			return fmt.Errorf("agenticudp: %w", err)
		}
		c.conn = dtlsConn
	}

	pkt := buildPacket(pktHandshake, 0, 0, 0, 0, 0, nil)
	if _, err := c.conn.Write(pkt); err != nil {
		return fmt.Errorf("agenticudp: handshake send: %w", err)
	}

	if dl, ok := c.conn.(interface{ SetReadDeadline(time.Time) error }); ok {
		dl.SetReadDeadline(time.Now().Add(5 * time.Second))
	}
	buf := make([]byte, headerSize+256)
	n, err := c.conn.Read(buf)
	if err != nil {
		return fmt.Errorf("agenticudp: handshake timeout: %w", err)
	}
	if n >= headerSize && buf[1] == pktHandshake {
		c.established.Store(true)
		log.Printf("agenticudp: session established with %s", c.addr)
	} else {
		return fmt.Errorf("agenticudp: unexpected handshake response (type=%d)", buf[1])
	}
	c.conn.SetReadDeadline(time.Time{})
	return nil
}

// Connect performs the DTLS handshake (if configured), then the AgenticUDP
// session handshake, and starts background loops for keepalive, ACK
// processing, retransmission, and — when a CongestionPredictor is attached —
// dynamic inflight-window tuning.
func (c *Client) Connect(ctx context.Context) error {
	ctx, c.cancel = context.WithCancel(ctx)
	if err := c.doHandshake(ctx); err != nil {
		return err
	}
	go c.ackLoop(ctx)
	go c.keepaliveLoop(ctx)
	go c.retransmitLoop(ctx)
	if c.congestionPredictor != nil {
		go c.congestionTunerLoop(ctx)
	}
	return nil
}

// congestionTunerLoop runs when a CongestionPredictor is attached. Every 5 s
// it reads the predictor's recommended action and scales maxInflight
// proportionally, closing the feedback loop between the congestion model and
// actual send pacing. The configured ceiling is never exceeded.
func (c *Client) congestionTunerLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	ceiling := c.maxInflight.Load()
	if ceiling <= 0 {
		ceiling = defaultMaxInflightFromEnv()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pred := c.congestionPredictor.Predict()
			var target int64
			switch pred.RecommendedAction {
			case "pace":
				target = ceiling / 2
			case "reduce":
				target = ceiling / 4
			case "defer":
				target = ceiling / 8
			default: // "normal": restore full window
				target = ceiling
			}
			if target < 4 {
				target = 4 // hard floor: keep pipeline from stalling completely
			}
			c.maxInflight.Store(target)
		}
	}
}

// processACK finds the inflight entry matching wireSeq+wireStreamID via linear
// scan, removes it, records RTT, and increments acked. Returns true when the
// entry was found. Duplicate ACKs (entry already removed) return false and do
// not increment acked, preventing ghost-ACK inflation of drain conditions.
func (c *Client) processACK(wireSeq, wireStreamID uint16) bool {
	c.inflightMu.Lock()
	var foundKey uint32
	var foundPkt *inflightPkt
	for k, p := range c.inflight {
		if p.seq == wireSeq && p.streamID == wireStreamID {
			foundKey = k
			foundPkt = p
			break
		}
	}
	if foundPkt != nil {
		rttMs := time.Since(foundPkt.sentAt).Milliseconds()
		c.lastRTTMs.Store(rttMs)
		if c.congestionPredictor != nil {
			c.congestionPredictor.RecordRTT(float64(rttMs))
		}
		delete(c.inflight, foundKey)
	}
	c.inflightMu.Unlock()
	if foundPkt != nil {
		c.acked.Add(1)
		return true
	}
	return false
}

// processNACK retransmits the datagram matching wireSeq+wireStreamID if it is
// still in the inflight map. This is the immediate-retransmit path; the
// retransmitLoop handles RTO-based retransmits independently.
func (c *Client) processNACK(wireSeq, wireStreamID uint16) {
	c.inflightMu.Lock()
	for _, p := range c.inflight {
		if p.seq == wireSeq && p.streamID == wireStreamID {
			c.conn.Write(p.data)
			p.retries++
			p.sentAt = time.Now()
			c.retransmits.Add(1)
			break
		}
	}
	c.inflightMu.Unlock()
}

// SetConfigHandler registers a callback invoked when the Core pushes an
// OTEL collector config over AgenticUDP (pktConfig). The yamlBytes contain
// the generated OTEL YAML.
func (c *Client) SetConfigHandler(fn func(yamlBytes []byte)) {
	c.onConfigMu.Lock()
	c.onConfig = fn
	c.onConfigMu.Unlock()
}

// EnableAI attaches AI modules to the transport client, replacing hardcoded
// tier assignment with LLM/Thompson-Sampling classification and adding
// predictive congestion management.
func (c *Client) EnableAI(classifier *AIClassifier, extractor *FeatureExtractor, predictor *CongestionPredictor) {
	c.aiClassifier = classifier
	c.featureExtractor = extractor
	c.congestionPredictor = predictor
	log.Printf("agenticudp: AI tier classification enabled (provider=%v)", classifier.Stats()["provider"])
}

// lossRate computes the current packet loss ratio from transport counters.
func (c *Client) lossRate() float64 {
	sent := c.sent.Load()
	if sent == 0 {
		return 0
	}
	return float64(c.dropped.Load()) / float64(sent)
}

// Close sends a FIN and shuts down the connection.
func (c *Client) Close() {
	if c.cancel != nil {
		c.cancel()
	}
	fin := buildPacket(pktFIN, 0, 0, 0, 0, 0, nil)
	c.conn.Write(fin)
	c.conn.Close()
}

// SendMetrics sends periodic metric telemetry as BESTEFF — fire-and-forget,
// no ACK wait. Metrics are replaced by the next push cycle, so loss is
// acceptable and speed is maximized. Zero round-trip overhead.
func (c *Client) SendMetrics(data interface{}) error {
	return c.sendSignal("metrics", TierBesteff, data)
}

// SendTraces sends trace spans as GUARANTEED — traces are high-value,
// non-replaceable records that must survive packet loss.
func (c *Client) SendTraces(data interface{}) error {
	return c.sendSignal("traces", TierGuaranteed, data)
}

// SendLogs sends log records as RELIABLE — important but periodic, worth
// a few retries but not infinite.
func (c *Client) SendLogs(data interface{}) error {
	return c.sendSignal("logs", TierReliable, data)
}

// SendEvents sends state-change events as GUARANTEED — rare, high-value
// signals that must not be lost.
func (c *Client) SendEvents(data interface{}) error {
	return c.sendSignal("logs", TierGuaranteed, data)
}

// SendFingerprint sends a fingerprint report as GUARANTEED — critical for
// onboarding, must arrive. Uses JSON envelope (Path B).
func (c *Client) SendFingerprint(data interface{}) error {
	return c.sendSignal("fingerprint", TierGuaranteed, data)
}

// SendFingerprintProto sends a fingerprint report as a protobuf Envelope
// (Path C). Used when -wire-format=proto.
func (c *Client) SendFingerprintProto(fp *sppv1.Fingerprint) error {
	env := &sppv1.Envelope{
		SignalType: sppv1.SignalType_SIGNAL_TYPE_FINGERPRINT,
		TenantId:   c.tenantID,
		Payload:    &sppv1.Envelope_Fingerprint{Fingerprint: fp},
	}
	return c.sendProtoEnvelope(env, TierGuaranteed)
}

// SendRaw sends an arbitrary payload at the given tier.
func (c *Client) SendRaw(payload []byte, tier Tier) error {
	if !c.established.Load() {
		return fmt.Errorf("agenticudp: not connected")
	}
	return c.transmit(payload, tier, 0)
}

// SendCycle batches all signal types from a single collection cycle into
// the minimum number of datagrams. Each signal type becomes one datagram
// at its appropriate tier. Large batches are auto-chunked so each
// datagram stays within the OS UDP size limit.
func (c *Client) SendCycle(metrics, traces, logs interface{}) error {
	if !c.established.Load() {
		return fmt.Errorf("agenticudp: not connected")
	}

	var firstErr error
	if metrics != nil {
		if err := c.sendChunkedSignal("metrics", TierBesteff, metrics, 30); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if logs != nil {
		if err := c.sendChunkedSignal("logs", TierReliable, logs, 10); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if traces != nil {
		if err := c.sendChunkedSignal("traces", TierGuaranteed, traces, 15); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// isOversizeDatagramErr reports whether err indicates the datagram exceeded
// the OS UDP size limit. On Linux/macOS this surfaces as syscall.EMSGSIZE
// ("message too long"); on Windows it surfaces as syscall.Errno(WSAEMSGSIZE)
// with the textual form "...larger than the internal message buffer...".
// We match by errno first (most reliable) and fall back to substring matches
// covering both wordings for resilience across Go runtime versions.
func isOversizeDatagramErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.EMSGSIZE) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "message too long") ||
		strings.Contains(msg, "message too large") ||
		strings.Contains(msg, "larger than the internal message buffer")
}

// sendChunkedSignal tries to send data as a single datagram. If the
// serialized payload exceeds the OS UDP limit, it splits slice-typed
// data into progressively smaller chunks until each fits.
func (c *Client) sendChunkedSignal(signalType string, tier Tier, data interface{}, chunkSize int) error {
	err := c.sendSignal(signalType, tier, data)
	if err == nil {
		return nil
	}
	if !isOversizeDatagramErr(err) {
		return err
	}

	raw, merr := json.Marshal(data)
	if merr != nil {
		return err
	}
	var arr []json.RawMessage
	if json.Unmarshal(raw, &arr) != nil || len(arr) == 0 {
		return err
	}

	// Progressively halve chunk size until sends succeed.
	// Between every chunk we honor the inflight window (waitForInflightWindow)
	// so a bulk send of hundreds of datagrams paces itself instead of
	// bursting the receiver's UDP socket buffer. Without this cap a single
	// SendCycle of ~5000 spans was observed to cause ~75% loss + retransmit
	// storm on loopback (see eo-ingest-bench v6 results, 2026-04-28).
	bgCtx := context.Background()
	stallDeadline := time.Now().Add(60 * time.Second)
	for chunkSize > 0 {
		var sendErr error
		for off := 0; off < len(arr); off += chunkSize {
			end := off + chunkSize
			if end > len(arr) {
				end = len(arr)
			}
			if werr := c.waitForInflightWindow(bgCtx, stallDeadline); werr != nil {
				return werr
			}
			if cerr := c.sendSignal(signalType, tier, arr[off:end]); cerr != nil {
				if isOversizeDatagramErr(cerr) {
					sendErr = cerr
					break
				}
				return cerr
			}
		}
		if sendErr == nil {
			return nil
		}
		chunkSize /= 2
	}
	// Last resort: send one record at a time
	for _, item := range arr {
		if werr := c.waitForInflightWindow(bgCtx, stallDeadline); werr != nil {
			return werr
		}
		if cerr := c.sendSignal(signalType, tier, []json.RawMessage{item}); cerr != nil {
			return cerr
		}
	}
	return nil
}

func (c *Client) sendSignal(signalType string, defaultTier Tier, data interface{}) error {
	if !c.established.Load() {
		return fmt.Errorf("agenticudp: not connected")
	}

	tier := defaultTier

	// AI tier classification: replace hardcoded tier with context-aware assignment
	if c.aiClassifier != nil && c.featureExtractor != nil {
		rttMs := float64(c.lastRTTMs.Load())
		ctx := c.featureExtractor.Extract(signalType, 0, rttMs, c.lossRate())
		classification := c.aiClassifier.Classify(ctx)
		tier = classification.Tier
	}

	// Congestion-aware traffic shaping: defer best-effort during severe congestion
	if c.congestionPredictor != nil {
		shaping := c.congestionPredictor.RecommendShaping()
		if shaping.DeferBestEffort && tier == TierBesteff {
			c.dropped.Add(1)
			return nil
		}
		if shaping.PacingIntervalMs > 0 {
			time.Sleep(time.Duration(shaping.PacingIntervalMs) * time.Millisecond)
		}
	}

	envelope := struct {
		SignalType string      `json:"signal_type"`
		TenantID   string      `json:"tenant_id"`
		Data       interface{} `json:"data"`
	}{
		SignalType: signalType,
		TenantID:   c.tenantID,
		Data:       data,
	}

	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("agenticudp: marshal %s: %w", signalType, err)
	}

	return c.transmit(payload, tier, 0)
}

// sendProtoEnvelope sends a protobuf-encoded spp.v1.Envelope datagram.
// Sets FLAG_PROTOBUF on the packet header so the receiver knows to
// proto.Unmarshal instead of JSON-decoding.
func (c *Client) sendProtoEnvelope(env *sppv1.Envelope, tier Tier) error {
	if !c.established.Load() {
		return fmt.Errorf("agenticudp: not connected")
	}

	payload, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("agenticudp: proto marshal: %w", err)
	}

	return c.transmitWithFlags(payload, tier, 0, flagProtobuf)
}

// SendCycleProto batches all signal types using protobuf serialization.
// Each signal type becomes one spp.v1.Envelope datagram at its appropriate tier.
func (c *Client) SendCycleProto(metrics *sppv1.MetricBatch, traces *sppv1.TraceBatch, logs *sppv1.LogBatch) error {
	if !c.established.Load() {
		return fmt.Errorf("agenticudp: not connected")
	}

	var firstErr error
	if metrics != nil && len(metrics.Metrics) > 0 {
		env := &sppv1.Envelope{
			TenantId:   c.tenantID,
			SignalType: sppv1.SignalType_SIGNAL_TYPE_METRICS,
			Payload:    &sppv1.Envelope_Metrics{Metrics: metrics},
		}
		if err := c.sendProtoEnvelope(env, TierBesteff); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if logs != nil && len(logs.Logs) > 0 {
		env := &sppv1.Envelope{
			TenantId:   c.tenantID,
			SignalType: sppv1.SignalType_SIGNAL_TYPE_LOGS,
			Payload:    &sppv1.Envelope_Logs{Logs: logs},
		}
		if err := c.sendProtoEnvelope(env, TierReliable); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if traces != nil && len(traces.Spans) > 0 {
		env := &sppv1.Envelope{
			TenantId:   c.tenantID,
			SignalType: sppv1.SignalType_SIGNAL_TYPE_TRACES,
			Payload:    &sppv1.Envelope_Traces{Traces: traces},
		}
		if err := c.sendProtoEnvelope(env, TierGuaranteed); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *Client) transmit(payload []byte, tier Tier, streamID uint16) error {
	return c.transmitWithFlags(payload, tier, streamID, 0)
}

func (c *Client) transmitWithFlags(payload []byte, tier Tier, streamID uint16, flags uint8) error {
	// Size pre-check: if the assembled datagram would exceed the OS UDP limit
	// declared by maxPayload (65000), short-circuit to syscall.EMSGSIZE so the
	// chunker engages without wasting a Write syscall. This also avoids
	// bumping the dropped counter for a packet we never actually transmitted.
	if headerSize+len(payload) > maxPayload {
		return syscall.EMSGSIZE
	}

	// Use the full uint32 from seqCounter as the inflight map key so the
	// key space is monotonically increasing and never repeats within a
	// session. The wire format carries only the low 16 bits (seq), which
	// wraps after 65 535 sends — a realistic event for long-running
	// production agents. Storing the full uint32 key prevents a wrapped
	// seq from silently overwriting an older inflight entry and corrupting
	// the retransmit / ACK-accounting logic.
	seqFull := c.seqCounter.Add(1)
	seq := uint16(seqFull)
	contentID := fnv1a(payload)

	pkt := buildPacket(pktData, uint8(tier), flags, seq, streamID, contentID, payload)

	if _, err := c.conn.Write(pkt); err != nil {
		c.dropped.Add(1)
		return err
	}
	c.sent.Add(1)

	// Track inflight for ACK-based tiers
	if tier != TierBesteff {
		c.inflightMu.Lock()
		c.inflight[seqFull] = &inflightPkt{
			data:     pkt,
			seq:      seq,      // wire seq for ACK matching in ackLoop
			streamID: streamID, // wire streamID for ACK matching in ackLoop
			tier:     tier,
			sentAt:   time.Now(),
		}
		c.inflightMu.Unlock()
	}

	return nil
}

// ── Background loops ─────────────────────────────────────────────────────────

func (c *Client) ackLoop(ctx context.Context) {
	buf := make([]byte, maxPayload)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		c.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, err := c.conn.Read(buf)
		if err != nil {
			continue
		}
		if n < headerSize || buf[0] != protoV2 {
			continue
		}

		pktType := buf[1]
		seq := binary.BigEndian.Uint16(buf[4:6])
		streamID := binary.BigEndian.Uint16(buf[6:8])

		switch pktType {
		case pktACK:
			c.processACK(seq, streamID)
		case pktNACK:
			c.processNACK(seq, streamID)
		case pktConfig:
			if n > headerSize {
				payload := make([]byte, n-headerSize)
				copy(payload, buf[headerSize:n])
				// ACK the config/pipeline-task packet so the server can
				// implement reliable server→client delivery: if no ACK
				// arrives within its RTO it retransmits. Servers that do
				// not implement retry simply ignore the unexpected ACK.
				ack := buildPacket(pktACK, 0, 0, seq, streamID, 0, nil)
				c.conn.Write(ack)
				c.onConfigMu.Lock()
				fn := c.onConfig
				c.onConfigMu.Unlock()
				if fn != nil {
					go fn(payload)
				}
			}
		case pktKeepalive:
			// Server responded to our keepalive
		}
	}
}

func (c *Client) keepaliveLoop(ctx context.Context) {
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ka := buildPacket(pktKeepalive, 0, 0, 0, 0, 0, nil)
			c.conn.Write(ka)
		}
	}
}

// retransmitLoop walks the inflight map every 500ms and retransmits any
// datagram whose per-packet RTO has elapsed. Two protections vs the
// pre-fix behavior (which retransmitted every unACKed packet at a fixed
// 2s interval, no cap on per-tick fan-out):
//
//  1. Exponential backoff via computeRTO. A packet that has already
//     been retried k times waits min(baseRTO * 2^k, maxRTO) before the
//     next retry. So a transient receiver stall amplifies into a
//     retransmit storm at most once: subsequent unsuccessful retries
//     quickly stretch out to the maxRTO ceiling and stop hammering the
//     socket.
//
//  2. Per-tick budget. We sort eligible packets oldest-first and
//     retransmit at most maxRetransmitBudget of them per tick;
//     remaining eligibles wait for the next tick (where they'll still
//     be eligible because their sentAt is unchanged). This converts an
//     N-packet burst into a paced trickle that the receiver's UDP
//     socket buffer can absorb.
//
// Together these address the n=1000 server-side log analysis pattern
// where 5M-span ingest produced 9,215 flushes (vs ~2,500 expected) and
// avg flush time grew from 264ms to 5.9s as the storm recursively
// deepened.
func (c *Client) retransmitLoop(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runRetransmitTick(time.Now())
		}
	}
}

// runRetransmitTick is the body of retransmitLoop's ticker case,
// extracted so unit tests can drive it deterministically without a
// real ticker. It is not called concurrently with itself in production
// (only the loop's single goroutine invokes it); tests that drive it
// directly must serialize their own calls.
func (c *Client) runRetransmitTick(now time.Time) {
	baseRTO := c.baseRTOMs.Load()
	maxRTO := c.maxRTOMs.Load()
	budget := c.maxRetransmitBudget.Load()
	if budget <= 0 {
		budget = defaultMaxRetransmitsPerTick
	}

	type eligibleEntry struct {
		key uint32
		pkt *inflightPkt
	}

	c.inflightMu.Lock()
	var (
		toRemove []uint32
		eligible []eligibleEntry
	)
	for key, pkt := range c.inflight {
		maxRetries := 3
		if pkt.tier == TierGuaranteed {
			maxRetries = 10
		}
		rto := computeRTO(baseRTO, maxRTO, pkt.retries)
		if now.Sub(pkt.sentAt) <= rto {
			continue
		}
		if pkt.retries >= maxRetries {
			toRemove = append(toRemove, key)
			c.dropped.Add(1)
			continue
		}
		eligible = append(eligible, eligibleEntry{key: key, pkt: pkt})
	}

	// Oldest-first ordering gives long-waiting packets priority when
	// the budget is exhausted; without this, Go's randomized map
	// iteration order would arbitrarily starve some packets across
	// successive ticks.
	sort.Slice(eligible, func(i, j int) bool {
		return eligible[i].pkt.sentAt.Before(eligible[j].pkt.sentAt)
	})

	sent := int64(0)
	for _, e := range eligible {
		if sent >= budget {
			break
		}
		// Write under the lock to match the pre-fix behavior. The lock
		// is intentionally not released during Write because the conn
		// is shared and Go's net.UDPConn.Write is atomic per call;
		// holding the lock keeps the inflight map consistent with the
		// observed retransmit counter.
		c.conn.Write(e.pkt.data)
		e.pkt.retries++
		e.pkt.sentAt = now
		c.retransmits.Add(1)
		sent++
	}

	for _, key := range toRemove {
		delete(c.inflight, key)
	}
	c.inflightMu.Unlock()

	if c.congestionPredictor != nil && len(toRemove) > 0 {
		c.congestionPredictor.RecordLoss(c.lossRate())
	}
}

// ── Packet construction ──────────────────────────────────────────────────────

func buildPacket(pktType, tier, flags uint8, seq, streamID uint16, contentID uint32, payload []byte) []byte {
	pkt := make([]byte, headerSize+len(payload))
	pkt[0] = protoV2
	pkt[1] = pktType
	pkt[2] = tier
	pkt[3] = flags
	binary.BigEndian.PutUint16(pkt[4:6], seq)
	binary.BigEndian.PutUint16(pkt[6:8], streamID)
	binary.BigEndian.PutUint32(pkt[8:12], contentID)
	binary.BigEndian.PutUint16(pkt[12:14], 0)
	if len(payload) > 0 {
		copy(pkt[headerSize:], payload)
	}
	cs := checksumV2(pkt)
	binary.BigEndian.PutUint16(pkt[12:14], cs)
	return pkt
}

func checksumV2(data []byte) uint16 {
	var sum uint32
	for i := 0; i+1 < len(data); i += 2 {
		sum += uint32(binary.BigEndian.Uint16(data[i : i+2]))
	}
	if len(data)%2 == 1 {
		sum += uint32(data[len(data)-1])
	}
	for sum>>16 != 0 {
		sum = (sum & 0xFFFF) + (sum >> 16)
	}
	return ^uint16(sum)
}

func fnv1a(data []byte) uint32 {
	hash := uint32(2166136261)
	for _, b := range data {
		hash ^= uint32(b)
		hash *= 16777619
	}
	return hash
}

// Stats returns transport counters.
type Stats struct {
	Sent        uint64                 `json:"sent"`
	Acked       uint64                 `json:"acked"`
	Retransmits uint64                 `json:"retransmits"`
	Dropped     uint64                 `json:"dropped"`
	Inflight    int                    `json:"inflight"`
	Established bool                   `json:"established"`
	LastRTTMs   int64                  `json:"last_rtt_ms,omitempty"`
	AIStats     map[string]interface{} `json:"ai_stats,omitempty"`
}

func (c *Client) Stats() Stats {
	c.inflightMu.Lock()
	inflight := len(c.inflight)
	c.inflightMu.Unlock()
	st := Stats{
		Sent:        c.sent.Load(),
		Acked:       c.acked.Load(),
		Retransmits: c.retransmits.Load(),
		Dropped:     c.dropped.Load(),
		Inflight:    inflight,
		Established: c.established.Load(),
		LastRTTMs:   c.lastRTTMs.Load(),
	}
	if c.aiClassifier != nil {
		st.AIStats = c.aiClassifier.Stats()
	}
	return st
}
