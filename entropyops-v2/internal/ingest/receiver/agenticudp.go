package receiver

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/entropyops/entropyops-v2/internal/fingerprint"
	"github.com/entropyops/entropyops-v2/internal/ingest/enrichment"
	"github.com/entropyops/entropyops-v2/internal/physics"
	"github.com/entropyops/entropyops-v2/internal/planner"
	"github.com/entropyops/entropyops-v2/internal/storage"
	sppv1 "github.com/entropyops/entropyops-v2/pkg/sppv1"
	"google.golang.org/protobuf/proto"
)

// AgenticUDP V2 wire protocol constants — must match C++ AgenticUDP_Shared.h.
const (
	protoV2       = 2
	headerV2Size  = 14
	maxUDPPayload = 65535

	pktData      = 1
	pktACK       = 2
	pktNACK      = 3
	pktKeepalive = 5
	pktHandshake = 6
	pktFIN       = 7
	pktMTUProbe  = 9
	pktMetrics   = 10
	pktConfig    = 11 // server → agent: OTEL collector config push

	flagRetransmit   = 0x01
	flagEncrypted    = 0x08
	flagFragment     = 0x10
	flagLastFragment = 0x20
	flagProtobuf     = 0x40 // payload is spp.v1.Envelope protobuf binary

	tierGuaranteed = 0
	tierReliable   = 1
	tierBesteff    = 2
)

// PacketHeaderV2 is the Go representation of the 14-byte AgenticUDP V2 header.
type PacketHeaderV2 struct {
	Version   uint8
	Type      uint8
	Tier      uint8
	Flags     uint8
	Seq       uint16
	StreamID  uint16
	ContentID uint32
	Checksum  uint16
}

// AgenticUDPReceiver listens for AgenticUDP V2 datagrams on a UDP socket
// and feeds decoded telemetry into the analytics pipeline (Path B / Path C).
//
// Path A: OTEL SDK → OTLP/gRPC → OTLPReceiver → Pipeline
// Path B: Framework → AgenticUDP (JSON) → AgenticUDPReceiver → Pipeline
// Path C: Framework → AgenticUDP/TLS (protobuf) → AgenticUDPReceiver → Pipeline
type AgenticUDPReceiver struct {
	port     int
	tenantID string
	conn     *net.UDPConn
	pipeline PipelinePublisher
	store    storage.Backend
	engine   *physics.Engine

	// DTLS support — when set, receiver accepts DTLS connections instead of raw UDP
	dtlsCfg      *DTLSConfig
	dtlsListener *dtlsListener

	// Post-ingest enrichment: DQI, exposure windows, topology, anomaly scoring, signal mgmt
	enricher *enrichment.Enricher

	// ZCO autoplan support
	sigCatalog      *fingerprint.SignatureCatalog
	plannerCfg      planner.ExecutorConfig
	zcoAutoplan     bool
	configResponses map[string][]byte // hostID → generated OTEL YAML (for command push)
	configMu        sync.Mutex

	// Storage coalescers: batch many small per-datagram writes into a
	// small number of WriteTraces / WriteMetrics / WriteLogs calls so the
	// receiver's read loop is never blocked on a SQL transaction commit.
	// See agenticudp_coalescer.go for the full motivation.
	traceBatcher  *traceCoalescer
	metricBatcher *metricCoalescer
	logBatcher    *logCoalescer

	// Session tracking: remote addr → session state
	sessions   map[string]*udpSession
	sessionsMu sync.RWMutex

	// Content dedup: content_id → last-seen timestamp
	dedup   map[uint32]time.Time
	dedupMu sync.Mutex

	// Fragment reassembly: content_id → ordered fragments
	fragments   map[uint32]*fragmentBuffer
	fragmentsMu sync.Mutex

	// Stats
	stats   AgenticUDPStats
	statsMu sync.Mutex

	cancel context.CancelFunc

	// Optional gate from api.Server — when non-nil and returns false, accepted
	// datagrams are counted as PacketsDroppedDisabled and not processed.
	ingestAllowed func() bool
}

// fragmentBuffer collects ordered fragments for a single content_id.
type fragmentBuffer struct {
	parts    map[uint16][]byte // seq → payload
	lastSeq  uint16            // seq of the last fragment (set when flagLastFragment arrives)
	complete bool
	created  time.Time
	streamID uint16
	from     *net.UDPAddr
	flags    uint8 // original flags (for flagProtobuf detection)
}

// replyFunc sends a raw packet back to a specific client. In raw UDP mode it
// uses conn.WriteToUDP; in DTLS mode it writes directly to the DTLS net.Conn.
type replyFunc func(data []byte) (int, error)

type udpSession struct {
	addr       *net.UDPAddr
	lastSeen   time.Time
	state      string // "handshake", "established", "closing"
	hostID     string // set from fingerprint data
	natType    string
	traversal  string            // direct | relay | auto
	nextExpSeq map[uint16]uint16 // stream_id → next expected seq
	reply      replyFunc         // per-session reply writer (set during handshake)
}

// AgenticUDPStats tracks receiver-side protocol counters.
type AgenticUDPStats struct {
	PacketsReceived      uint64 `json:"packets_received"`
	PacketsInvalid       uint64 `json:"packets_invalid"`
	DatagramsAccepted    uint64 `json:"datagrams_accepted"`
	DatagramsDuped       uint64 `json:"datagrams_duped"`
	ACKsSent             uint64 `json:"acks_sent"`
	NACKsSent            uint64 `json:"nacks_sent"`
	KeepalivesRecv       uint64 `json:"keepalives_recv"`
	HandshakesRecv       uint64 `json:"handshakes_recv"`
	MetricsRecv          uint64 `json:"metrics_recv"`
	SessionsActive       int    `json:"sessions_active"`
	ChecksumFails        uint64 `json:"checksum_fails"`
	FragmentsReassembled uint64 `json:"fragments_reassembled"`
	TraversalDirect      uint64 `json:"traversal_direct"`
	TraversalRelay       uint64 `json:"traversal_relay"`
	TraversalAuto        uint64 `json:"traversal_auto"`
	TraversalUnknown     uint64 `json:"traversal_unknown"`
	TraversalMode        string `json:"traversal_mode"`
	// PacketsDroppedDisabled counts datagrams dropped because ingest was toggled off.
	PacketsDroppedDisabled uint64 `json:"packets_dropped_disabled"`
}

// AgenticUDPDiagnostics provides operator-facing NAT/traversal visibility.
type AgenticUDPDiagnostics struct {
	Stats                 AgenticUDPStats     `json:"stats"`
	NATTypeDistribution   map[string]int      `json:"nat_type_distribution"`
	TraversalDistribution map[string]int      `json:"traversal_distribution"`
	RelayEndpoint         string              `json:"relay_endpoint"`
	SessionSamples        []map[string]string `json:"session_samples"`
}

// NewAgenticUDPReceiver creates a receiver bound to the given UDP port.
// telemetry is published to the pipeline under tenantID.
func NewAgenticUDPReceiver(port int, tenantID string) *AgenticUDPReceiver {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("AGENTICUDP_TRAVERSAL_MODE")))
	if mode == "" {
		mode = "auto"
	}
	return &AgenticUDPReceiver{
		port:      port,
		tenantID:  tenantID,
		sessions:  make(map[string]*udpSession),
		dedup:     make(map[uint32]time.Time),
		fragments: make(map[uint32]*fragmentBuffer),
		stats:     AgenticUDPStats{TraversalMode: mode},
	}
}

// SetPipeline attaches the analytics pipeline for Path B publishing.
func (r *AgenticUDPReceiver) SetPipeline(p PipelinePublisher) {
	r.pipeline = p
}

// SetStore attaches the storage backend and physics engine so Path B data
// is dual-written — both to the pipeline (ClickHouse) and to the primary
// store (SQLite/Postgres) that the UI reads from.
func (r *AgenticUDPReceiver) SetStore(store storage.Backend, engine *physics.Engine) {
	r.store = store
	r.engine = engine
}

// SetDTLS configures DTLS encryption for the AgenticUDP receiver.
// When set, the receiver listens for DTLS connections instead of raw UDP.
func (r *AgenticUDPReceiver) SetDTLS(cfg *DTLSConfig) {
	r.dtlsCfg = cfg
}

// SetZCO configures ZCO autoplan so fingerprints arriving over AgenticUDP
// trigger the same plan-generate-execute flow as HTTP reports.
func (r *AgenticUDPReceiver) SetZCO(catalog *fingerprint.SignatureCatalog, cfg planner.ExecutorConfig, autoplan bool) {
	r.sigCatalog = catalog
	r.plannerCfg = cfg
	r.zcoAutoplan = autoplan
	r.configResponses = make(map[string][]byte)
}

// SetEnricher attaches the shared post-ingest enrichment pipeline so
// AgenticUDP-ingested data receives the same DQI, topology, anomaly,
// exposure-window, and signal-management processing as OTLP data.
func (r *AgenticUDPReceiver) SetEnricher(e *enrichment.Enricher) {
	r.enricher = e
}

// SetIngestAllowed sets an optional runtime toggle; when fn returns false,
// datagrams are dropped after checksum validation (listeners stay up).
func (r *AgenticUDPReceiver) SetIngestAllowed(fn func() bool) {
	r.ingestAllowed = fn
}

// PendingConfig returns and clears any generated OTEL config for a host,
// ready to push back over AgenticUDP.
func (r *AgenticUDPReceiver) PendingConfig(hostID string) []byte {
	r.configMu.Lock()
	defer r.configMu.Unlock()
	cfg := r.configResponses[hostID]
	delete(r.configResponses, hostID)
	return cfg
}

// envIntDefault returns the parsed value of env var name, or fallback if
// the env var is unset or unparseable. Used for tunable runtime knobs that
// should not require a code change to adjust in production (e.g. UDP socket
// buffer sizing).
func envIntDefault(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

// Start begins listening on the UDP port. Blocks until ctx is cancelled.
// When DTLS is configured, accepts encrypted per-client connections (Path C).
// Otherwise, listens for raw UDP datagrams (Path B).
func (r *AgenticUDPReceiver) Start(ctx context.Context) error {
	ctx, r.cancel = context.WithCancel(ctx)

	go r.dedupGC(ctx)
	go r.sessionGC(ctx)
	r.startCoalescers(ctx)

	// DTLS mode: accept per-client encrypted connections
	if r.dtlsCfg != nil && r.dtlsCfg.Mode != TLSModeNone {
		return r.startDTLS(ctx)
	}

	// Raw UDP mode (Path B, backward compat)
	return r.startRawUDP(ctx)
}

func (r *AgenticUDPReceiver) startRawUDP(ctx context.Context) error {
	addr := &net.UDPAddr{Port: r.port}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("agenticudp: listen :%d: %w", r.port, err)
	}
	// Increase the UDP socket receive buffer so bursts (chunked bulk sends:
	// hundreds of datagrams of ~50 KB) don't overflow the kernel queue and
	// trigger a retransmit storm from the agent. Default kernel buffer
	// (typically 64–256 KB) holds only ~1–5 chunks.
	//
	// Tunable via ENTROPYOPS_AGENTIC_UDP_RCVBUF_BYTES; default 16 MB. On
	// Linux you may also need `sysctl -w net.core.rmem_max=16777216` for
	// the kernel to honor sizes above its default cap.
	rcvbuf := envIntDefault("ENTROPYOPS_AGENTIC_UDP_RCVBUF_BYTES", 16*1024*1024)
	if rcvbuf > 0 {
		if err := conn.SetReadBuffer(rcvbuf); err != nil {
			log.Printf("agenticudp: warning: SetReadBuffer(%d) failed: %v (continuing with kernel default)", rcvbuf, err)
		} else {
			log.Printf("agenticudp: socket receive buffer set to %d bytes", rcvbuf)
		}
	}
	r.conn = conn
	log.Printf("agenticudp: listening on :%d (Path B, cleartext)", r.port)

	buf := make([]byte, maxUDPPayload)
	for {
		select {
		case <-ctx.Done():
			conn.Close()
			return nil
		default:
		}

		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("agenticudp: read error: %v", err)
			continue
		}

		r.statsMu.Lock()
		r.stats.PacketsReceived++
		r.statsMu.Unlock()

		r.handlePacket(buf[:n], remoteAddr)
	}
}

func (r *AgenticUDPReceiver) startDTLS(ctx context.Context) error {
	listenAddr := fmt.Sprintf(":%d", r.port)
	dl, addr, err := newDTLSListener(listenAddr, *r.dtlsCfg)
	if err != nil {
		return err
	}
	r.dtlsListener = dl
	log.Printf("agenticudp: DTLS listener on %s (Path C, encrypted)", addr)

	go func() {
		<-ctx.Done()
		dl.Close()
	}()

	for {
		conn, err := dl.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("agenticudp: dtls accept error: %v", err)
			continue
		}
		go r.serveDTLSConn(ctx, conn)
	}
}

// serveDTLSConn reads AgenticUDP packets from a single DTLS connection.
func (r *AgenticUDPReceiver) serveDTLSConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	remoteAddr, _ := conn.RemoteAddr().(*net.UDPAddr)
	if remoteAddr == nil {
		udpStr := conn.RemoteAddr().String()
		remoteAddr, _ = net.ResolveUDPAddr("udp", udpStr)
	}

	buf := make([]byte, maxUDPPayload)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := conn.Read(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			return
		}

		r.statsMu.Lock()
		r.stats.PacketsReceived++
		r.statsMu.Unlock()

		dtlsReply := func(data []byte) (int, error) { return conn.Write(data) }
		r.handlePacketWithReply(buf[:n], remoteAddr, dtlsReply)
	}
}

// Stop shuts down the receiver.
func (r *AgenticUDPReceiver) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	if r.conn != nil {
		r.conn.Close()
	}
}

// Stats returns a snapshot of receiver counters.
func (r *AgenticUDPReceiver) Stats() AgenticUDPStats {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	r.sessionsMu.RLock()
	r.stats.SessionsActive = len(r.sessions)
	r.sessionsMu.RUnlock()
	return r.stats
}

// Diagnostics returns NAT/traversal distributions for enterprise runbooks.
func (r *AgenticUDPReceiver) Diagnostics() AgenticUDPDiagnostics {
	stats := r.Stats()
	out := AgenticUDPDiagnostics{
		Stats:                 stats,
		NATTypeDistribution:   map[string]int{},
		TraversalDistribution: map[string]int{},
		RelayEndpoint:         os.Getenv("AGENTICUDP_RELAY_ENDPOINT"),
		SessionSamples:        []map[string]string{},
	}
	r.sessionsMu.RLock()
	defer r.sessionsMu.RUnlock()
	for key, s := range r.sessions {
		nat := s.natType
		if nat == "" {
			nat = "unknown"
		}
		out.NATTypeDistribution[nat]++
		tr := s.traversal
		if tr == "" {
			tr = "unknown"
		}
		out.TraversalDistribution[tr]++
		if len(out.SessionSamples) < 20 {
			out.SessionSamples = append(out.SessionSamples, map[string]string{
				"remote":    key,
				"nat_type":  nat,
				"traversal": tr,
				"state":     s.state,
				"last_seen": s.lastSeen.UTC().Format(time.RFC3339),
			})
		}
	}
	return out
}

func (r *AgenticUDPReceiver) handlePacket(data []byte, from *net.UDPAddr) {
	r.handlePacketWithReply(data, from, nil)
}

func (r *AgenticUDPReceiver) handlePacketWithReply(data []byte, from *net.UDPAddr, rf replyFunc) {
	if len(data) < headerV2Size {
		r.statsMu.Lock()
		r.stats.PacketsInvalid++
		r.statsMu.Unlock()
		return
	}
	if data[0] != protoV2 {
		r.statsMu.Lock()
		r.stats.PacketsInvalid++
		r.statsMu.Unlock()
		return
	}

	hdr := parseHeaderV2(data)

	if !r.verifyChecksum(data) {
		r.statsMu.Lock()
		r.stats.ChecksumFails++
		r.statsMu.Unlock()
		return
	}

	if r.ingestAllowed != nil && !r.ingestAllowed() {
		r.statsMu.Lock()
		r.stats.PacketsDroppedDisabled++
		r.statsMu.Unlock()
		return
	}

	payload := data[headerV2Size:]

	switch hdr.Type {
	case pktHandshake:
		r.handleHandshake(hdr, payload, from, rf)
	case pktKeepalive:
		r.handleKeepalive(from)
	case pktData:
		r.handleData(hdr, payload, from)
	case pktMetrics:
		r.handleMetrics(hdr, payload, from)
	case pktFIN:
		r.handleFIN(from)
	case pktMTUProbe:
		r.sendACK(hdr.StreamID, hdr.Seq, hdr.ContentID, from)
	}
}

func (r *AgenticUDPReceiver) handleHandshake(hdr PacketHeaderV2, payload []byte, from *net.UDPAddr, rf replyFunc) {
	r.statsMu.Lock()
	r.stats.HandshakesRecv++
	r.statsMu.Unlock()

	declaredNAT := "unknown"
	if len(payload) > 0 {
		var hello map[string]interface{}
		if err := json.Unmarshal(payload, &hello); err == nil {
			if v, ok := hello["nat_type"].(string); ok && strings.TrimSpace(v) != "" {
				declaredNAT = strings.ToLower(strings.TrimSpace(v))
			}
		}
	}
	traversal := r.chooseTraversalMode(declaredNAT)

	key := from.String()
	reply := rf
	if reply == nil {
		reply = r.rawUDPReply(from)
	}
	r.sessionsMu.Lock()
	r.sessions[key] = &udpSession{
		addr:       from,
		lastSeen:   time.Now(),
		state:      "established",
		natType:    declaredNAT,
		traversal:  traversal,
		nextExpSeq: make(map[uint16]uint16),
		reply:      reply,
	}
	r.sessionsMu.Unlock()
	r.recordTraversalStat(traversal)

	ackPayload, _ := json.Marshal(map[string]interface{}{
		"traversal_mode": traversal,
		"relay_hint":     os.Getenv("AGENTICUDP_RELAY_ENDPOINT"),
	})
	ack := buildPacketV2(pktHandshake, 0, 0, 0, 0, 0, ackPayload)
	reply(ack)
	log.Printf("agenticudp: session established from %s nat=%s traversal=%s", key, declaredNAT, traversal)
}

func (r *AgenticUDPReceiver) chooseTraversalMode(natType string) string {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("AGENTICUDP_TRAVERSAL_MODE")))
	if mode == "" {
		mode = "auto"
	}
	switch mode {
	case "direct":
		return "direct"
	case "relay":
		return "relay"
	default:
		// Auto: asymmetric/symmetric NATs should prefer relay fallback.
		if strings.Contains(natType, "symmetric") || strings.Contains(natType, "restricted") {
			return "relay"
		}
		if natType == "unknown" || natType == "" {
			return "auto"
		}
		return "direct"
	}
}

func (r *AgenticUDPReceiver) recordTraversalStat(mode string) {
	r.statsMu.Lock()
	defer r.statsMu.Unlock()
	switch mode {
	case "direct":
		r.stats.TraversalDirect++
	case "relay":
		r.stats.TraversalRelay++
	case "auto":
		r.stats.TraversalAuto++
	default:
		r.stats.TraversalUnknown++
	}
}

// rawUDPReply returns a replyFunc that writes via the raw UDP socket.
func (r *AgenticUDPReceiver) rawUDPReply(addr *net.UDPAddr) replyFunc {
	return func(data []byte) (int, error) {
		if r.conn == nil {
			return 0, fmt.Errorf("raw UDP conn not available")
		}
		return r.conn.WriteToUDP(data, addr)
	}
}

func (r *AgenticUDPReceiver) handleKeepalive(from *net.UDPAddr) {
	r.statsMu.Lock()
	r.stats.KeepalivesRecv++
	r.statsMu.Unlock()

	r.touchSession(from)

	reply := r.sessionReply(from)
	ka := buildPacketV2(pktKeepalive, 0, 0, 0, 0, 0, nil)
	reply(ka)

	r.pushPendingConfig(from)
}

func (r *AgenticUDPReceiver) pushPendingConfig(to *net.UDPAddr) {
	key := to.String()
	r.sessionsMu.RLock()
	sess, ok := r.sessions[key]
	r.sessionsMu.RUnlock()
	if !ok || sess.hostID == "" {
		return
	}

	cfg := r.PendingConfig(sess.hostID)
	if cfg == nil {
		return
	}

	reply := r.sessionReply(to)
	pkt := buildPacketV2(pktConfig, tierGuaranteed, 0, 0, 0, 0, cfg)
	reply(pkt)
	log.Printf("agenticudp: pushed OTEL config to %s (%d bytes)", sess.hostID, len(cfg))
}

func (r *AgenticUDPReceiver) handleData(hdr PacketHeaderV2, payload []byte, from *net.UDPAddr) {
	r.touchSession(from)

	// ACK every non-best-effort fragment/packet immediately so the sender
	// can release its inflight buffer.
	if hdr.Tier != tierBesteff {
		r.sendACK(hdr.StreamID, hdr.Seq, hdr.ContentID, from)
	}

	// ── Fragment reassembly ──────────────────────────────────────────────
	if (hdr.Flags & flagFragment) != 0 {
		assembled := r.addFragment(hdr, payload, from)
		if assembled == nil {
			return // waiting for more fragments
		}
		payload = assembled
	}

	// Content dedup (on the full content_id, after reassembly)
	if r.isDuplicate(hdr.ContentID) {
		r.statsMu.Lock()
		r.stats.DatagramsDuped++
		r.statsMu.Unlock()
		return
	}

	r.statsMu.Lock()
	r.stats.DatagramsAccepted++
	r.statsMu.Unlock()

	if len(payload) > 0 {
		r.routeToQueue(hdr, payload, from)
	}
}

// addFragment stores a fragment and returns the reassembled payload when all
// parts have arrived. Returns nil while waiting for more fragments.
func (r *AgenticUDPReceiver) addFragment(hdr PacketHeaderV2, payload []byte, from *net.UDPAddr) []byte {
	r.fragmentsMu.Lock()
	defer r.fragmentsMu.Unlock()

	fb, ok := r.fragments[hdr.ContentID]
	if !ok {
		fb = &fragmentBuffer{
			parts:    make(map[uint16][]byte),
			created:  time.Now(),
			streamID: hdr.StreamID,
			from:     from,
			flags:    hdr.Flags,
		}
		r.fragments[hdr.ContentID] = fb
	}

	cp := make([]byte, len(payload))
	copy(cp, payload)
	fb.parts[hdr.Seq] = cp

	if (hdr.Flags & flagLastFragment) != 0 {
		fb.lastSeq = hdr.Seq
		fb.complete = true
	}

	if !fb.complete {
		return nil
	}

	// Check contiguous from seq 0 .. lastSeq
	for seq := uint16(0); seq <= fb.lastSeq; seq++ {
		if _, exists := fb.parts[seq]; !exists {
			return nil
		}
	}

	// All fragments present — reassemble in order
	var assembled []byte
	for seq := uint16(0); seq <= fb.lastSeq; seq++ {
		assembled = append(assembled, fb.parts[seq]...)
	}
	delete(r.fragments, hdr.ContentID)

	r.statsMu.Lock()
	r.stats.FragmentsReassembled++
	r.statsMu.Unlock()

	return assembled
}

// reapStaleFragments removes fragment buffers older than 30 seconds.
func (r *AgenticUDPReceiver) reapStaleFragments() {
	r.fragmentsMu.Lock()
	defer r.fragmentsMu.Unlock()
	cutoff := time.Now().Add(-30 * time.Second)
	for cid, fb := range r.fragments {
		if fb.created.Before(cutoff) {
			delete(r.fragments, cid)
		}
	}
}

func (r *AgenticUDPReceiver) handleMetrics(hdr PacketHeaderV2, payload []byte, from *net.UDPAddr) {
	r.statsMu.Lock()
	r.stats.MetricsRecv++
	r.statsMu.Unlock()
	r.touchSession(from)

	// Transport health metrics from the Framework's MetricsCollector.
	// Parse and publish as metrics into the pipeline.
	if r.pipeline != nil && len(payload) > 0 {
		var transportMetrics map[string]interface{}
		if err := json.Unmarshal(payload, &transportMetrics); err == nil {
			entityID, _ := transportMetrics["entity_id"].(string)
			if entityID == "" {
				entityID = from.IP.String()
			}
			metrics := []storage.Metric{
				{
					Timestamp:   time.Now().UTC(),
					ServiceName: entityID,
					TenantID:    r.tenantID,
					MetricName:  "agenticudp.transport_health",
					MetricType:  "gauge",
					Value:       safeFloat(transportMetrics["elasticity_score"]),
					Labels: map[string]string{
						"source":   "agenticudp",
						"protocol": "agentic-udp",
					},
				},
			}
			if r.pipeline != nil {
				r.pipeline.PublishMetrics(r.tenantID, metrics)
			}
			r.enqueueMetrics(r.tenantID, metrics)
		}
	}
}

func (r *AgenticUDPReceiver) handleFIN(from *net.UDPAddr) {
	key := from.String()
	r.sessionsMu.Lock()
	delete(r.sessions, key)
	r.sessionsMu.Unlock()
	log.Printf("agenticudp: session closed from %s", key)
}

// routeToQueue inspects the datagram payload and publishes to the
// appropriate pipeline topic (metrics, traces, logs, or fingerprint).
// When FLAG_PROTOBUF is set, decodes as spp.v1.Envelope (Path C).
// Otherwise, decodes as JSON envelope (Path B).
func (r *AgenticUDPReceiver) routeToQueue(hdr PacketHeaderV2, payload []byte, from *net.UDPAddr) {
	start := time.Now()
	defer func() { RecordAgenticUDPProcessing(time.Since(start)) }()

	// Path C: protobuf-encoded spp.v1.Envelope
	if hdr.Flags&flagProtobuf != 0 {
		r.routeProtoEnvelope(payload, from)
		return
	}

	// Path B: JSON envelope
	var envelope struct {
		SignalType string          `json:"signal_type"`
		TenantID   string          `json:"tenant_id"`
		Data       json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		// Not an envelope — treat as raw metric payload
		r.publishRawMetric(hdr, payload)
		return
	}

	tenantID := envelope.TenantID
	if tenantID == "" {
		tenantID = r.tenantID
	}

	switch envelope.SignalType {
	case "metrics":
		var metrics []storage.Metric
		if err := json.Unmarshal(envelope.Data, &metrics); err == nil {
			if r.pipeline != nil {
				r.pipeline.PublishMetrics(tenantID, metrics)
			}
			r.enqueueMetrics(tenantID, metrics)
		}
	case "traces":
		var traces []storage.Trace
		if err := json.Unmarshal(envelope.Data, &traces); err == nil {
			if r.pipeline != nil {
				r.pipeline.PublishTraces(tenantID, traces)
			}
			r.enqueueTraces(tenantID, traces)
		}
	case "logs":
		var logs []storage.LogRecord
		if err := json.Unmarshal(envelope.Data, &logs); err == nil {
			if r.pipeline != nil {
				r.pipeline.PublishLogs(tenantID, logs)
			}
			r.enqueueLogs(tenantID, logs)
		}
	case "fingerprint":
		r.handleFingerprintData(tenantID, envelope.Data, from)
	default:
		r.publishRawMetric(hdr, payload)
	}
}

// ── Path C: protobuf deserialization ──────────────────────────────────────────

// routeProtoEnvelope deserializes a protobuf spp.v1.Envelope and routes its
// payload to the same storage + pipeline paths as the JSON envelope.
func (r *AgenticUDPReceiver) routeProtoEnvelope(payload []byte, from *net.UDPAddr) {
	var env sppv1.Envelope
	if err := proto.Unmarshal(payload, &env); err != nil {
		log.Printf("agenticudp: proto unmarshal error: %v", err)
		return
	}

	tenantID := env.TenantId
	if tenantID == "" {
		tenantID = r.tenantID
	}

	switch env.SignalType {
	case sppv1.SignalType_SIGNAL_TYPE_METRICS:
		if mb := env.GetMetrics(); mb != nil {
			metrics := protoMetricsToStorage(mb, tenantID)
			if r.pipeline != nil {
				r.pipeline.PublishMetrics(tenantID, metrics)
			}
			r.enqueueMetrics(tenantID, metrics)
		}
	case sppv1.SignalType_SIGNAL_TYPE_TRACES:
		if tb := env.GetTraces(); tb != nil {
			traces := protoTracesToStorage(tb, tenantID)
			if r.pipeline != nil {
				r.pipeline.PublishTraces(tenantID, traces)
			}
			r.enqueueTraces(tenantID, traces)
		}
	case sppv1.SignalType_SIGNAL_TYPE_LOGS:
		if lb := env.GetLogs(); lb != nil {
			logs := protoLogsToStorage(lb, tenantID)
			if r.pipeline != nil {
				r.pipeline.PublishLogs(tenantID, logs)
			}
			r.enqueueLogs(tenantID, logs)
		}
	case sppv1.SignalType_SIGNAL_TYPE_FINGERPRINT:
		if fp := env.GetFingerprint(); fp != nil {
			fpJSON, _ := json.Marshal(protoFingerprintToHost(fp))
			r.handleFingerprintData(tenantID, fpJSON, from)
		}
	case sppv1.SignalType_SIGNAL_TYPE_CONFIG:
		if cfg := env.GetConfig(); cfg != nil {
			log.Printf("agenticudp: received config push for %s (proto path)", cfg.HostId)
		}
	default:
		log.Printf("agenticudp: unknown proto signal type %d", env.SignalType)
	}
}

func protoMetricsToStorage(mb *sppv1.MetricBatch, tenantID string) []storage.Metric {
	metrics := make([]storage.Metric, 0, len(mb.Metrics))
	for _, m := range mb.Metrics {
		sm := storage.Metric{
			ServiceName: m.ServiceName,
			MetricName:  m.MetricName,
			MetricType:  m.MetricType,
			Value:       m.Value,
			Labels:      m.Labels,
			TenantID:    tenantID,
		}
		if m.Timestamp != nil {
			sm.Timestamp = m.Timestamp.AsTime()
		}
		metrics = append(metrics, sm)
	}
	return metrics
}

func protoTracesToStorage(tb *sppv1.TraceBatch, tenantID string) []storage.Trace {
	traces := make([]storage.Trace, 0, len(tb.Spans))
	for _, s := range tb.Spans {
		st := storage.Trace{
			TraceID:       s.TraceId,
			SpanID:        s.SpanId,
			ParentSpanID:  s.ParentSpanId,
			ServiceName:   s.ServiceName,
			OperationName: s.OperationName,
			DurationUS:    s.DurationUs,
			StatusCode:    s.StatusCode,
			SpanKind:      s.SpanKind,
			Attributes:    s.Attributes,
			TenantID:      tenantID,
		}
		if s.StartTime != nil {
			st.StartTime = s.StartTime.AsTime()
		}
		if s.EndTime != nil {
			st.EndTime = s.EndTime.AsTime()
		}
		traces = append(traces, st)
	}
	return traces
}

func protoLogsToStorage(lb *sppv1.LogBatch, tenantID string) []storage.LogRecord {
	logs := make([]storage.LogRecord, 0, len(lb.Logs))
	for _, l := range lb.Logs {
		lr := storage.LogRecord{
			TenantID:       tenantID,
			TraceID:        l.TraceId,
			SpanID:         l.SpanId,
			ServiceName:    l.ServiceName,
			SeverityText:   l.SeverityText,
			SeverityNumber: int(l.SeverityNumber),
			Body:           l.Body,
			Attributes:     l.Attributes,
			ResourceAttrs:  l.ResourceAttrs,
		}
		if l.Timestamp != nil {
			lr.Timestamp = l.Timestamp.AsTime()
		}
		logs = append(logs, lr)
	}
	return logs
}

func protoFingerprintToHost(fp *sppv1.Fingerprint) *fingerprint.HostFingerprint {
	hfp := &fingerprint.HostFingerprint{
		OS:               fp.Os,
		Arch:             fp.Arch,
		Kernel:           fp.Kernel,
		CloudProvider:    fp.CloudProvider,
		Hostname:         fp.Hostname,
		ContainerRuntime: fp.ContainerRuntime,
		Platform:         fp.Platform,
		FingerprintID:    fp.FingerprintId,
		Confidence:       fp.Confidence,
	}
	if fp.CollectedAt != nil {
		hfp.CollectedAt = fp.CollectedAt.AsTime()
	}
	hfp.Duration = time.Duration(fp.DurationNs)

	for _, p := range fp.OpenPorts {
		hfp.OpenPorts = append(hfp.OpenPorts, int(p))
	}
	for _, pi := range fp.Processes {
		hfp.Processes = append(hfp.Processes, fingerprint.ProcessInfo{
			PID: pi.Pid, Name: pi.Name, Cmdline: pi.Cmdline,
		})
	}
	for _, ds := range fp.DetectedSoftware {
		sw := fingerprint.DetectedSoftware{
			Name: ds.Name, DisplayName: ds.DisplayName, Category: ds.Category,
			Confidence: ds.Confidence, Layer: ds.Layer, CollectStrategy: ds.CollectStrategy,
		}
		sw.MatchedProcess = ds.MatchedProcesses
		for _, p := range ds.MatchedPorts {
			sw.MatchedPorts = append(sw.MatchedPorts, int(p))
		}
		sw.MatchedConfigs = ds.MatchedConfigs
		hfp.DetectedSoftware = append(hfp.DetectedSoftware, sw)
	}
	for _, da := range fp.DetectedAgents {
		hfp.DetectedAgents = append(hfp.DetectedAgents, fingerprint.DetectedAgent{
			Name: da.Name, PID: da.Pid, Cmdline: da.Cmdline,
		})
	}
	return hfp
}

// handleFingerprintData processes a fingerprint report arriving over AgenticUDP.
// It stores the fingerprint and triggers ZCO autoplan when conditions are met,
// mirroring the HTTP handleFingerprintReport flow.
func (r *AgenticUDPReceiver) handleFingerprintData(tenantID string, data json.RawMessage, from *net.UDPAddr) {
	if r.store == nil {
		return
	}

	var fp fingerprint.HostFingerprint
	if err := json.Unmarshal(data, &fp); err != nil {
		log.Printf("agenticudp: fingerprint unmarshal error: %v", err)
		return
	}

	hostID := fp.Hostname
	if hostID == "" {
		hostID = "unknown"
	}
	if fp.CollectedAt.IsZero() {
		fp.CollectedAt = time.Now().UTC()
	}

	// Determine scope based on content — if DetectedSoftware is present, it's an OTel fingerprint.
	scope := fingerprint.ScopeModern
	if len(fp.DetectedSoftware) > 0 {
		scope = fingerprint.ScopeOTel
	}

	// Compute fingerprint ID using the scope-appropriate hash.
	var fpID string
	switch scope {
	case fingerprint.ScopeOTel:
		ofp := &fingerprint.OTelFingerprint{
			IDSchemeVersion:  1,
			Hostname:         fp.Hostname,
			DetectedSoftware: fp.DetectedSoftware,
			DetectedAgents:   fp.DetectedAgents,
		}
		fpID = fingerprint.ComputeOTelFingerprintID(ofp)
	default:
		mfp := &fingerprint.ModernFingerprint{
			OS: fp.OS, Arch: fp.Arch, Kernel: fp.Kernel,
			Hostname: fp.Hostname, Platform: fp.Platform,
			CloudProvider: fp.CloudProvider, ContainerRuntime: fp.ContainerRuntime,
			OpenPorts: fp.OpenPorts, Processes: fp.Processes,
			DetectedAgents: fp.DetectedAgents,
		}
		fpID = fingerprint.ComputeModernFingerprintID(mfp)
	}
	fp.FingerprintID = fpID

	ctx := context.Background()
	previousRec, _ := r.store.GetLatestFingerprint(ctx, tenantID, hostID, string(scope))
	changed := previousRec == nil || previousRec.FingerprintID != fpID

	fpData, err := json.Marshal(fp)
	if err != nil {
		log.Printf("agenticudp: fingerprint marshal error: %v", err)
		return
	}

	rec := storage.Fingerprint{
		ID:            udpRandomID(),
		TenantID:      tenantID,
		HostID:        hostID,
		Scope:         string(scope),
		FingerprintID: fpID,
		Data:          fpData,
		CollectedAt:   fp.CollectedAt,
	}
	if err := r.store.WriteFingerprint(ctx, rec); err != nil {
		log.Printf("agenticudp: WriteFingerprint error: %v", err)
		return
	}

	log.Printf("agenticudp: fingerprint stored host=%s scope=%s id=%s changed=%v",
		hostID, scope, fpID[:12], changed)

	// Tag the session with the host ID so we can push config on keepalive.
	if from != nil {
		key := from.String()
		r.sessionsMu.Lock()
		if s, ok := r.sessions[key]; ok {
			s.hostID = hostID
		}
		r.sessionsMu.Unlock()
	}

	// ZCO autoplan — trigger only on meaningful OTel transitions.
	if r.zcoAutoplan && scope == fingerprint.ScopeOTel && changed {
		r.runAutoplan(ctx, tenantID, hostID, &fp)
	}
}

// runAutoplan generates and executes an attachment plan from a fingerprint,
// then stores the generated OTEL config for push back to the agent.
func (r *AgenticUDPReceiver) runAutoplan(ctx context.Context, tenantID, hostID string, fp *fingerprint.HostFingerprint) {
	var sigs []fingerprint.SoftwareSignature
	if r.sigCatalog != nil {
		sigs = r.sigCatalog.All()
	}

	plan := planner.Generate(fp, sigs)
	plan.TenantID = tenantID

	planData, _ := json.Marshal(plan)
	planRec := storage.AttachmentPlanRecord{
		ID:            plan.ID,
		FingerprintID: plan.FingerprintID,
		TenantID:      tenantID,
		Data:          planData,
		Status:        string(plan.Status),
		CreatedAt:     plan.CreatedAt,
	}
	if err := r.store.WritePlan(ctx, planRec); err != nil {
		log.Printf("agenticudp: autoplan WritePlan error: %v", err)
		return
	}

	allHighConf := len(plan.Actions) > 0
	for _, a := range plan.Actions {
		if a.Confidence < 0.7 {
			allHighConf = false
			break
		}
	}

	if !allHighConf {
		log.Printf("agenticudp: autoplan %s generated but not auto-executed (low confidence)", plan.ID)
		return
	}

	if err := r.store.UpdatePlanStatus(ctx, tenantID, plan.ID, string(planner.PlanApproved)); err != nil {
		log.Printf("agenticudp: autoplan approve error: %v", err)
		return
	}
	plan.Status = planner.PlanApproved

	executor := planner.NewExecutor(r.store, r.plannerCfg)
	result, err := executor.Execute(ctx, plan)
	if err != nil {
		log.Printf("agenticudp: autoplan execute error: %v", err)
		return
	}

	_ = r.store.UpdatePlanStatus(ctx, tenantID, plan.ID, string(planner.PlanExecuted))

	log.Printf("agenticudp: autoplan %s executed for %s (%d actions, %d files)",
		plan.ID, hostID, len(plan.Actions), len(result.WrittenFiles))

	// If OTEL YAML was generated, store it for push back to the agent.
	if len(result.WrittenFiles) > 0 {
		for _, path := range result.WrittenFiles {
			yamlBytes, err := readFileBytes(path)
			if err == nil {
				r.configMu.Lock()
				r.configResponses[hostID] = yamlBytes
				r.configMu.Unlock()
				log.Printf("agenticudp: OTEL config ready for push to %s (%d bytes)", hostID, len(yamlBytes))
			}
		}
	}
}

func udpRandomID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func readFileBytes(path string) ([]byte, error) {
	return os.ReadFile(path)
}

// storeMetrics writes metrics to storage.Backend and runs them through the
// full enrichment pipeline (DQI, physics, exposure windows, topology,
// anomaly scoring, signal management). When the enricher is wired, all
// processing is delegated to it for OTLP-parity. Falls back to inline
// physics-only when enricher is nil.
func (r *AgenticUDPReceiver) storeMetrics(tenantID string, metrics []storage.Metric) {
	if r.store == nil || len(metrics) == 0 {
		return
	}

	// Mark the host as a live agent so the Collector & Agent Health view
	// shows it. AgenticUDP datagrams come from the EntropyOps agent itself,
	// so we treat the originating host entity as the agent ID. Without this
	// the UI would always read "0 Active agents" even with the agent running.
	r.recordAgentHeartbeatFromMetrics(tenantID, metrics)

	if r.enricher != nil {
		r.enricher.EnrichAndStoreMetrics(tenantID, metrics, "agenticudp_ingest")
		return
	}

	ctx := context.Background()
	if err := r.store.WriteMetrics(ctx, metrics); err != nil {
		log.Printf("agenticudp: store.WriteMetrics error: %v", err)
		return
	}

	if r.engine == nil {
		return
	}

	now := time.Now().UTC()
	byEntity := make(map[string]map[string]float64)
	for _, m := range metrics {
		entity := m.ServiceName
		if entity == "" {
			continue
		}
		if byEntity[entity] == nil {
			byEntity[entity] = make(map[string]float64)
		}
		byEntity[entity][m.MetricName] = m.Value
	}

	var states []storage.PhysicsState
	for entity, indicators := range byEntity {
		for _, layer := range physics.SupportedLayers {
			actx := physics.AnalysisContext{Source: "agenticudp_ingest"}
			result := r.engine.Analyze(layer, entity, indicators, actx)
			if result.EntropyRate == 0 && result.CoolingCapacity == 0 {
				continue
			}
			states = append(states, storage.PhysicsState{
				Timestamp:       now,
				EntityID:        entity,
				TenantID:        tenantID,
				Layer:           layer,
				EntropyRate:     result.EntropyRate,
				CoolingCapacity: result.CoolingCapacity,
				Regime:          result.Regime,
				Indicators:      indicators,
				Source:          "agenticudp_ingest",
			})
		}
	}
	if len(states) > 0 {
		if err := r.store.WritePhysicsState(ctx, states); err != nil {
			log.Printf("agenticudp: store.WritePhysicsState error: %v", err)
		}
	}
}

// recordAgentHeartbeatFromMetrics writes a lightweight AgentRegistration row
// keyed by the host entity each time AgenticUDP receives a metrics batch.
// Per-batch we register at most one heartbeat per distinct host service so we
// don't hammer the store. The host service is identified as the longest
// tenant-rooted service name (no '/' in it) which the agent emits as its
// host metrics (matches `cfg.HostScraperEntityID`). Falls back to the first
// service we see if no obvious host is present.
func (r *AgenticUDPReceiver) recordAgentHeartbeatFromMetrics(tenantID string, metrics []storage.Metric) {
	if r.store == nil || len(metrics) == 0 {
		return
	}
	host := ""
	seen := map[string]struct{}{}
	for _, m := range metrics {
		s := m.ServiceName
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		// Prefer top-level host entity (no '/' separator).
		if host == "" || (!strings.Contains(s, "/") && strings.Contains(host, "/")) {
			host = s
		}
	}
	if host == "" {
		return
	}
	now := time.Now().UTC()
	_ = r.store.UpsertAgentRegistration(context.Background(), storage.AgentRegistration{
		AgentID:    host,
		Version:    "agenticudp",
		TenantID:   tenantID,
		Scope:      []string{"metrics"},
		LastSeenAt: now,
	})
}

// enqueueTraces routes a per-datagram trace batch through the coalescer
// when one is attached, otherwise falls back to a direct synchronous
// write. This is the hot path for AgenticUDP bulk trace ingest; do not
// inline storeTraces here without first reading the comment block at the
// top of agenticudp_coalescer.go.
func (r *AgenticUDPReceiver) enqueueTraces(tenantID string, traces []storage.Trace) {
	if len(traces) == 0 {
		return
	}
	if r.traceBatcher != nil {
		r.traceBatcher.enqueue(tenantID, traces)
		return
	}
	r.storeTraces(tenantID, traces)
}

// enqueueMetrics is the metrics counterpart of enqueueTraces.
func (r *AgenticUDPReceiver) enqueueMetrics(tenantID string, metrics []storage.Metric) {
	if len(metrics) == 0 {
		return
	}
	if r.metricBatcher != nil {
		r.metricBatcher.enqueue(tenantID, metrics)
		return
	}
	r.storeMetrics(tenantID, metrics)
}

// enqueueLogs is the logs counterpart of enqueueTraces.
func (r *AgenticUDPReceiver) enqueueLogs(tenantID string, logs []storage.LogRecord) {
	if len(logs) == 0 {
		return
	}
	if r.logBatcher != nil {
		r.logBatcher.enqueue(tenantID, logs)
		return
	}
	r.storeLogs(tenantID, logs)
}

// storeTraces writes traces to storage.Backend.
func (r *AgenticUDPReceiver) storeTraces(tenantID string, traces []storage.Trace) {
	if r.store == nil || len(traces) == 0 {
		return
	}
	for i := range traces {
		if traces[i].TenantID == "" {
			traces[i].TenantID = tenantID
		}
	}
	if err := r.store.WriteTraces(context.Background(), traces); err != nil {
		log.Printf("agenticudp: store.WriteTraces error: %v", err)
	}
}

func (r *AgenticUDPReceiver) storeLogs(tenantID string, logs []storage.LogRecord) {
	if r.store == nil || len(logs) == 0 {
		return
	}
	for i := range logs {
		if logs[i].TenantID == "" {
			logs[i].TenantID = tenantID
		}
	}
	if err := r.store.WriteLogs(context.Background(), logs); err != nil {
		log.Printf("agenticudp: store.WriteLogs error: %v", err)
	}

	structured := make([]StructuredLogRecord, 0, len(logs))
	for _, l := range logs {
		structured = append(structured, StructuredLogRecord{
			Timestamp:    l.Timestamp,
			ObservedAt:   time.Now().UTC(),
			ServiceName:  l.ServiceName,
			EntityID:     l.ServiceName,
			SeverityText: l.SeverityText,
			Body:         l.Body,
			Attributes:   l.Attributes,
			Resource:     l.ResourceAttrs,
			TenantID:     l.TenantID,
			Format:       "agenticudp",
		})
	}
	appendStructuredLogs(structured)
}

func (r *AgenticUDPReceiver) publishRawMetric(hdr PacketHeaderV2, payload []byte) {
	if r.pipeline == nil {
		return
	}
	metrics := []storage.Metric{
		{
			Timestamp:   time.Now().UTC(),
			ServiceName: fmt.Sprintf("stream-%d", hdr.StreamID),
			TenantID:    r.tenantID,
			MetricName:  "agenticudp.raw_datagram",
			MetricType:  "gauge",
			Value:       float64(len(payload)),
			Labels: map[string]string{
				"tier":       fmt.Sprintf("%d", hdr.Tier),
				"content_id": fmt.Sprintf("%08x", hdr.ContentID),
			},
		},
	}
	r.pipeline.PublishMetrics(r.tenantID, metrics)
}

// ── Packet building & verification ───────────────────────────────────────────

func parseHeaderV2(data []byte) PacketHeaderV2 {
	return PacketHeaderV2{
		Version:   data[0],
		Type:      data[1],
		Tier:      data[2],
		Flags:     data[3],
		Seq:       binary.BigEndian.Uint16(data[4:6]),
		StreamID:  binary.BigEndian.Uint16(data[6:8]),
		ContentID: binary.BigEndian.Uint32(data[8:12]),
		Checksum:  binary.BigEndian.Uint16(data[12:14]),
	}
}

func buildPacketV2(pktType, tier, flags uint8, seq, streamID uint16, contentID uint32, payload []byte) []byte {
	pkt := make([]byte, headerV2Size+len(payload))
	pkt[0] = protoV2
	pkt[1] = pktType
	pkt[2] = tier
	pkt[3] = flags
	binary.BigEndian.PutUint16(pkt[4:6], seq)
	binary.BigEndian.PutUint16(pkt[6:8], streamID)
	binary.BigEndian.PutUint32(pkt[8:12], contentID)
	binary.BigEndian.PutUint16(pkt[12:14], 0) // checksum placeholder
	if len(payload) > 0 {
		copy(pkt[headerV2Size:], payload)
	}
	cs := calculateChecksum(pkt)
	binary.BigEndian.PutUint16(pkt[12:14], cs)
	return pkt
}

// calculateChecksum matches the C++ internet checksum implementation.
func calculateChecksum(data []byte) uint16 {
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

func (r *AgenticUDPReceiver) verifyChecksum(data []byte) bool {
	stored := binary.BigEndian.Uint16(data[12:14])
	tmp := make([]byte, len(data))
	copy(tmp, data)
	binary.BigEndian.PutUint16(tmp[12:14], 0)
	computed := calculateChecksum(tmp)
	return stored == computed
}

func (r *AgenticUDPReceiver) sendACK(streamID, seq uint16, contentID uint32, to *net.UDPAddr) {
	reply := r.sessionReply(to)
	ack := buildPacketV2(pktACK, 0, 0, seq, streamID, contentID, nil)
	reply(ack)
	r.statsMu.Lock()
	r.stats.ACKsSent++
	r.statsMu.Unlock()
}

// sessionReply returns the reply function for the session identified by the
// remote address. Falls back to raw UDP if no session or no reply is stored.
func (r *AgenticUDPReceiver) sessionReply(addr *net.UDPAddr) replyFunc {
	key := addr.String()
	r.sessionsMu.RLock()
	sess, ok := r.sessions[key]
	r.sessionsMu.RUnlock()
	if ok && sess.reply != nil {
		return sess.reply
	}
	return r.rawUDPReply(addr)
}

// ── Session & dedup management ───────────────────────────────────────────────

func (r *AgenticUDPReceiver) touchSession(from *net.UDPAddr) {
	key := from.String()
	r.sessionsMu.Lock()
	if s, ok := r.sessions[key]; ok {
		s.lastSeen = time.Now()
	} else {
		r.sessions[key] = &udpSession{
			addr:       from,
			lastSeen:   time.Now(),
			state:      "established",
			nextExpSeq: make(map[uint16]uint16),
		}
	}
	r.sessionsMu.Unlock()
}

func (r *AgenticUDPReceiver) isDuplicate(contentID uint32) bool {
	r.dedupMu.Lock()
	defer r.dedupMu.Unlock()
	if _, exists := r.dedup[contentID]; exists {
		return true
	}
	r.dedup[contentID] = time.Now()
	return false
}

func (r *AgenticUDPReceiver) dedupGC(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-60 * time.Second)
			r.dedupMu.Lock()
			for k, v := range r.dedup {
				if v.Before(cutoff) {
					delete(r.dedup, k)
				}
			}
			r.dedupMu.Unlock()
			r.reapStaleFragments()
		}
	}
}

func (r *AgenticUDPReceiver) sessionGC(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-5 * time.Minute)
			r.sessionsMu.Lock()
			for k, s := range r.sessions {
				if s.lastSeen.Before(cutoff) {
					log.Printf("agenticudp: session expired %s", k)
					delete(r.sessions, k)
				}
			}
			r.sessionsMu.Unlock()
		}
	}
}

func safeFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	default:
		return 0.0
	}
}
