// Package transport — AgenticUDP V3 (Agentic AI UDP) protocol client.
//
// # Protocol
//
// V3 extends V2 with an 8-byte message-framing prefix in the data-plane header,
// raising it from 14 to 22 bytes:
//
//	Offset  Size  Field
//	     0     1  version  = 0x03
//	     1     1  pkt_type
//	     2     1  tier
//	     3     1  flags    (FRAGMENTED=0x01, FRAG_START=0x02, FRAG_END=0x04)
//	     4     2  seq      (per-datagram, wire-wraps at 65535)
//	     6     2  stream_id
//	     8     4  msg_id   (logical message; same across all fragments)
//	    12     2  frag_offset  (fragment index, 0-based)
//	    14     2  frag_total   (total fragments; 1 for single-datagram messages)
//	    16     4  content_id   (FNV-1a of fragment payload)
//	    20     2  checksum     (same 1s-complement algo as V2)
//
// # New packet type
//
//	pktSACK (4) — server ACKs multiple datagram seqs in a single packet.
//	Payload: packed uint16 list of seqs sharing the header's stream_id.
//
// # Key improvements over V2
//
//  1. Transparent fragmentation — payloads larger than maxPayloadV3 (~64 978 B)
//     are split by the transport layer into individually-ACKed datagrams sharing
//     a msg_id, so the application layer never needs to chunk at the JSON level.
//
//  2. Selective ACK (SACK) — the server can acknowledge N fragments in one
//     packet, reducing per-datagram ACK overhead under bulk load.
//
//  3. Dynamic inflight window — the embedded CongestionPredictor drives
//     maxInflight via congestionTunerLoop; the window shrinks under predicted
//     congestion and recovers when conditions clear.
//
//  4. Pipeline task correlation — SendOnStream / SendTaskResult carry a
//     uint16 streamID that the server uses to correlate task results with
//     the pktConfig-pushed task, enabling fully bidirectional agentic pipelines.
//
// # Backward compatibility
//
// The session handshake reuses the V2 format (protoV2=2, pktHandshake).
// V3 DATA packets are identified by buf[0]==protoV3 (=3). A V3 client
// connecting to a V2 server will complete the handshake successfully; the
// server will reject V3 DATA datagrams (unknown version byte). Deploy the
// V3 server module to unlock all V3 features.
package transport

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ── V3 wire constants ─────────────────────────────────────────────────────────

const (
	protoV3      = 3
	headerSizeV3 = 22 // 8 bytes longer than V2

	// maxPayloadV3 is the maximum application-layer bytes per V3 datagram.
	// maxPayload is the max total datagram size (header+payload); V3 uses
	// a 22-byte header so the data capacity is 65000 − 22 = 64978 bytes.
	// V2 data capacity is 65000 − 14 = 64986 bytes; the 8-byte difference
	// is the price of the extra framing fields and is negligible in practice.
	maxPayloadV3 = maxPayload - headerSizeV3

	// pktSACK is a V3-only packet type. The server sends a SACK to
	// acknowledge multiple datagram seqs at once. Payload is a packed
	// []uint16 (big-endian) of seq values sharing the header stream_id.
	pktSACK = 4

	// Fragment flags (stored in the flags byte, OR-ed with any user flags).
	flagV3Fragmented = 0x01 // datagram belongs to a multi-fragment message
	flagV3FragStart  = 0x02 // first fragment of the message
	flagV3FragEnd    = 0x04 // last fragment of the message
)

// ── V3 packet builder ─────────────────────────────────────────────────────────

// buildPacketV3 assembles a V3 datagram with the 22-byte header. The
// checksum field (bytes 20:22) is computed over the entire packet including
// the payload, then written back in place — same 1s-complement algorithm as
// V2 so server-side validation code can remain version-agnostic.
func buildPacketV3(pktType, tier, flags uint8, seq, streamID uint16,
	msgID uint32, fragOffset, fragTotal uint16,
	contentID uint32, payload []byte) []byte {

	pkt := make([]byte, headerSizeV3+len(payload))
	pkt[0] = protoV3
	pkt[1] = pktType
	pkt[2] = tier
	pkt[3] = flags
	binary.BigEndian.PutUint16(pkt[4:6], seq)
	binary.BigEndian.PutUint16(pkt[6:8], streamID)
	binary.BigEndian.PutUint32(pkt[8:12], msgID)
	binary.BigEndian.PutUint16(pkt[12:14], fragOffset)
	binary.BigEndian.PutUint16(pkt[14:16], fragTotal)
	binary.BigEndian.PutUint32(pkt[16:20], contentID)
	binary.BigEndian.PutUint16(pkt[20:22], 0) // placeholder until checksum is computed
	if len(payload) > 0 {
		copy(pkt[headerSizeV3:], payload)
	}
	cs := checksumV2(pkt) // reuses V2 checksum — same algorithm
	binary.BigEndian.PutUint16(pkt[20:22], cs)
	return pkt
}

// ── ClientV3 ─────────────────────────────────────────────────────────────────

// ClientV3 is the AgenticUDP V3 transport client.
//
// It embeds *Client for session management, AI modules, inflight tracking,
// keepalive, and retransmit loops. ClientV3 overrides Connect to start
// ackLoopV3 instead of the V2 ackLoop, and replaces the send path with V3
// packet builders that support fragmentation and SACK.
//
// Create with NewClientV3 or NewClientV3DTLS; call Connect before Send*.
type ClientV3 struct {
	*Client

	// msgCounter is a session-scoped monotonic message ID. All fragments
	// of one logical message share the same msgID, enabling the server to
	// reassemble them even if datagrams arrive out of order.
	msgCounter atomic.Uint32

	// inflightMsgs tracks fragmented messages awaiting full ACK. A message
	// is complete once ackedFrags == totalFrags. Protected by inflightMsgsMu.
	inflightMsgsMu sync.Mutex
	inflightMsgs   map[uint32]*inflightMsgV3

	// fragPayloadSize overrides maxPayloadV3 as the per-fragment data
	// capacity. Zero means use maxPayloadV3. Set to a small value in tests
	// to exercise fragmentation without bumping into OS UDP send-buffer
	// limits (macOS SO_SNDBUF defaults to 9216 B on loopback).
	fragPayloadSize int
}

// inflightMsgV3 tracks the per-fragment ACK state for a V3 multi-datagram
// message. The message is considered delivered once ackedFrags == totalFrags.
type inflightMsgV3 struct {
	msgID      uint32
	totalFrags uint16
	ackedFrags uint32 // incremented atomically by ackLoopV3 when a frag is ACKed
	sentAt     time.Time
}

// ── Constructors ──────────────────────────────────────────────────────────────

// NewClientV3 creates a V3 transport client (cleartext UDP).
// host is "host:port", e.g. "core.entropyops.io:4320".
func NewClientV3(host, tenantID string) (*ClientV3, error) {
	c, err := NewClient(host, tenantID)
	if err != nil {
		return nil, err
	}
	return &ClientV3{
		Client:       c,
		inflightMsgs: make(map[uint32]*inflightMsgV3),
	}, nil
}

// NewClientV3DTLS creates a V3 transport client with DTLS encryption.
func NewClientV3DTLS(host, tenantID string, dtlsCfg DTLSClientConfig) (*ClientV3, error) {
	c, err := NewClientDTLS(host, tenantID, dtlsCfg)
	if err != nil {
		return nil, err
	}
	return &ClientV3{
		Client:       c,
		inflightMsgs: make(map[uint32]*inflightMsgV3),
	}, nil
}

// ── Session setup ─────────────────────────────────────────────────────────────

// Connect establishes the UDP/DTLS session and starts V3 background loops.
// Unlike Client.Connect, this starts ackLoopV3 (which understands pktSACK)
// instead of the V2 ackLoop.
func (cv3 *ClientV3) Connect(ctx context.Context) error {
	ctx, cv3.cancel = context.WithCancel(ctx)
	if err := cv3.doHandshake(ctx); err != nil {
		return err
	}
	go cv3.ackLoopV3(ctx)
	go cv3.keepaliveLoop(ctx)
	go cv3.retransmitLoop(ctx)
	if cv3.congestionPredictor != nil {
		go cv3.congestionTunerLoop(ctx)
	}
	return nil
}

// ── Send path ─────────────────────────────────────────────────────────────────

// transmitSingleV3 sends a payload that fits in one V3 datagram.
func (cv3 *ClientV3) transmitSingleV3(payload []byte, tier Tier, streamID uint16, userFlags uint8) error {
	seqFull := cv3.seqCounter.Add(1)
	seq := uint16(seqFull)
	msgID := cv3.nextMsgID()
	contentID := fnv1a(payload)
	pkt := buildPacketV3(pktData, uint8(tier), userFlags, seq, streamID, msgID, 0, 1, contentID, payload)

	if _, err := cv3.conn.Write(pkt); err != nil {
		cv3.dropped.Add(1)
		return err
	}
	cv3.sent.Add(1)

	if tier != TierBesteff {
		cv3.inflightMu.Lock()
		cv3.inflight[seqFull] = &inflightPkt{
			data: pkt, seq: seq, streamID: streamID, tier: tier, sentAt: time.Now(),
		}
		cv3.inflightMu.Unlock()
	}
	return nil
}

// fragDataCapacity returns the per-fragment data capacity. In production
// this is always maxPayloadV3; tests may set fragPayloadSize to a smaller
// value to stay within OS UDP send-buffer limits.
func (cv3 *ClientV3) fragDataCapacity() int {
	if cv3.fragPayloadSize > 0 {
		return cv3.fragPayloadSize
	}
	return maxPayloadV3
}

// transmitFragmentedV3 splits payload across multiple V3 datagrams sharing
// a single msgID. Fragments are sent in order; the inflight window is honoured
// between fragments to prevent receiver buffer overflow.
func (cv3 *ClientV3) transmitFragmentedV3(payload []byte, tier Tier, streamID uint16) error {
	msgID := cv3.nextMsgID()
	chunkSize := cv3.fragDataCapacity()
	total := (len(payload) + chunkSize - 1) / chunkSize
	if total > 65535 {
		return fmt.Errorf("agenticudp v3: payload %d bytes requires %d fragments (limit 65535)", len(payload), total)
	}
	fragTotal := uint16(total)

	// Register the message so ackLoopV3 can track completion.
	if tier != TierBesteff {
		cv3.inflightMsgsMu.Lock()
		cv3.inflightMsgs[msgID] = &inflightMsgV3{
			msgID:      msgID,
			totalFrags: fragTotal,
			sentAt:     time.Now(),
		}
		cv3.inflightMsgsMu.Unlock()
	}

	bgCtx := context.Background()
	stallDeadline := time.Now().Add(60 * time.Second)

	for i := 0; i < total; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		chunk := payload[start:end]

		flags := uint8(flagV3Fragmented)
		if i == 0 {
			flags |= flagV3FragStart
		}
		if i == total-1 {
			flags |= flagV3FragEnd
		}

		if err := cv3.waitForInflightWindow(bgCtx, stallDeadline); err != nil {
			return err
		}

		seqFull := cv3.seqCounter.Add(1)
		seq := uint16(seqFull)
		contentID := fnv1a(chunk)
		pkt := buildPacketV3(pktData, uint8(tier), flags, seq, streamID, msgID, uint16(i), fragTotal, contentID, chunk)

		if _, err := cv3.conn.Write(pkt); err != nil {
			cv3.dropped.Add(1)
			return err
		}
		cv3.sent.Add(1)

		if tier != TierBesteff {
			cv3.inflightMu.Lock()
			cv3.inflight[seqFull] = &inflightPkt{
				data: pkt, seq: seq, streamID: streamID, tier: tier, sentAt: time.Now(),
			}
			cv3.inflightMu.Unlock()
		}
	}
	return nil
}

// transmitV3 routes to the single-datagram or fragmented path depending on
// payload size. This is the core V3 transmit primitive; the application layer
// never needs to chunk payloads — the transport does it transparently.
func (cv3 *ClientV3) transmitV3(payload []byte, tier Tier, streamID uint16, userFlags uint8) error {
	if len(payload) <= cv3.fragDataCapacity() {
		return cv3.transmitSingleV3(payload, tier, streamID, userFlags)
	}
	return cv3.transmitFragmentedV3(payload, tier, streamID)
}

// sendSignalV3 is the unified V3 signal send path. It applies AI tier
// classification (from cache) and congestion shaping, then marshals the full
// payload and hands it to transmitV3. Unlike the V2 sendChunkedSignal, there
// is no application-level chunking — the transport fragments automatically.
func (cv3 *ClientV3) sendSignalV3(signalType string, tier Tier, streamID uint16, data interface{}) error {
	if !cv3.established.Load() {
		return fmt.Errorf("agenticudp v3: not connected")
	}

	// AI tier classification: hot path costs <1 μs on a cache hit.
	if cv3.aiClassifier != nil && cv3.featureExtractor != nil {
		rttMs := float64(cv3.lastRTTMs.Load())
		sigCtx := cv3.featureExtractor.Extract(signalType, 0, rttMs, cv3.lossRate())
		classification := cv3.aiClassifier.Classify(sigCtx)
		tier = classification.Tier
	}

	// Congestion-aware shaping.
	if cv3.congestionPredictor != nil {
		shaping := cv3.congestionPredictor.RecommendShaping()
		if shaping.DeferBestEffort && tier == TierBesteff {
			cv3.dropped.Add(1)
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
		TenantID:   cv3.tenantID,
		Data:       data,
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("agenticudp v3: marshal %s: %w", signalType, err)
	}
	return cv3.transmitV3(payload, tier, streamID, 0)
}

// ── Public high-level send API ────────────────────────────────────────────────

// SendTraces sends a trace payload at TierGuaranteed (AI may upgrade/downgrade).
func (cv3 *ClientV3) SendTraces(traces interface{}) error {
	return cv3.sendSignalV3("traces", TierGuaranteed, 0, traces)
}

// SendMetrics sends a metrics payload at TierBesteff (AI may promote under anomaly).
func (cv3 *ClientV3) SendMetrics(metrics interface{}) error {
	return cv3.sendSignalV3("metrics", TierBesteff, 0, metrics)
}

// SendLogs sends a log payload at TierReliable.
func (cv3 *ClientV3) SendLogs(logs interface{}) error {
	return cv3.sendSignalV3("logs", TierReliable, 0, logs)
}

// SendEvents sends an event payload at TierReliable.
func (cv3 *ClientV3) SendEvents(events interface{}) error {
	return cv3.sendSignalV3("events", TierReliable, 0, events)
}

// SendCycle sends metrics, traces, and logs in one call (nil values skipped).
// This is the drop-in replacement for Client.SendCycle — no application-level
// chunking is required; the transport fragments large payloads automatically.
func (cv3 *ClientV3) SendCycle(metrics, traces, logs interface{}) error {
	var firstErr error
	if metrics != nil {
		if err := cv3.sendSignalV3("metrics", TierBesteff, 0, metrics); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if logs != nil {
		if err := cv3.sendSignalV3("logs", TierReliable, 0, logs); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if traces != nil {
		if err := cv3.sendSignalV3("traces", TierGuaranteed, 0, traces); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// SendOnStream sends data on a specific streamID at the given tier. Use this
// to reply to a pipeline task on the same streamID the task arrived on, so
// the server can correlate result → task without scanning every datagram.
func (cv3 *ClientV3) SendOnStream(streamID uint16, signalType string, tier Tier, data interface{}) error {
	return cv3.sendSignalV3(signalType, tier, streamID, data)
}

// SendTaskResult sends a task result back to the server on task.StreamID at
// TierGuaranteed. The result is wrapped in a "task_result" signal envelope.
func (cv3 *ClientV3) SendTaskResult(task PipelineTask, result interface{}) error {
	return cv3.SendOnStream(task.StreamID, "task_result", TierGuaranteed, map[string]interface{}{
		"task_id":   task.TaskID,
		"task_type": task.TaskType,
		"result":    result,
	})
}

// SetPipelineTaskHandler registers a typed callback for pipeline tasks pushed
// by the server via pktConfig. Internally uses Client.SetConfigHandler.
func (cv3 *ClientV3) SetPipelineTaskHandler(fn func(task PipelineTask)) {
	cv3.SetConfigHandler(func(raw []byte) {
		var task PipelineTask
		if err := json.Unmarshal(raw, &task); err != nil || task.TaskID == "" {
			return
		}
		if task.DeadlineMs > 0 && time.Now().After(time.UnixMilli(task.DeadlineMs)) {
			return
		}
		fn(task)
	})
}

// ── V3 ACK loop ───────────────────────────────────────────────────────────────

// ackLoopV3 is the V3 replacement for Client.ackLoop. It handles:
//   - pktACK (type 2): single datagram ACK — delegates to processACK.
//   - pktSACK (type 4): packed list of seq numbers to ACK — processes each via processACK.
//   - pktNACK (type 3): immediate NACK retransmit — delegates to processNACK.
//   - pktConfig (type 11): server-pushed task/config — ACKs back then fires onConfig.
//   - pktKeepalive (type 12): server keepalive response — no-op.
//
// V2 packets (buf[0]==protoV2) are handled via the shared processACK/processNACK
// helpers for backward compatibility during rolling server upgrades.
func (cv3 *ClientV3) ackLoopV3(ctx context.Context) {
	buf := make([]byte, maxPayload)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		cv3.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, err := cv3.conn.Read(buf)
		if err != nil {
			continue
		}

		// Route by protocol version byte.
		if n >= headerSize && buf[0] == protoV2 {
			// V2 packet on a V3 session (server not yet upgraded, or keepalive).
			pktType := buf[1]
			seq := binary.BigEndian.Uint16(buf[4:6])
			streamID := binary.BigEndian.Uint16(buf[6:8])
			switch pktType {
			case pktACK:
				cv3.processACK(seq, streamID)
			case pktNACK:
				cv3.processNACK(seq, streamID)
			case pktConfig:
				if n > headerSize {
					cv3.handleConfigPayload(buf[headerSize:n], seq, streamID, protoV2)
				}
			}
			continue
		}

		if n < headerSizeV3 || buf[0] != protoV3 {
			continue // malformed or unknown version
		}

		pktType := buf[1]
		seq := binary.BigEndian.Uint16(buf[4:6])
		streamID := binary.BigEndian.Uint16(buf[6:8])

		switch pktType {
		case pktACK:
			cv3.processACK(seq, streamID)

		case pktSACK:
			// Payload is a packed list of uint16 seq values to ACK.
			// All share the stream_id from the V3 header.
			numSeqs := (n - headerSizeV3) / 2
			for i := 0; i < numSeqs; i++ {
				sackedSeq := binary.BigEndian.Uint16(buf[headerSizeV3+i*2:])
				cv3.processACK(sackedSeq, streamID)
			}

		case pktNACK:
			cv3.processNACK(seq, streamID)

		case pktConfig:
			if n > headerSizeV3 {
				cv3.handleConfigPayload(buf[headerSizeV3:n], seq, streamID, protoV3)
			}

		case pktKeepalive:
			// Server responded to our keepalive — no action needed.
		}
	}
}

// handleConfigPayload ACKs the pktConfig back to the server and fires
// the onConfig callback. The ACK format matches the originating protocol
// version so the server does not need to parse cross-version ACKs.
func (cv3 *ClientV3) handleConfigPayload(data []byte, seq, streamID uint16, proto uint8) {
	payload := make([]byte, len(data))
	copy(payload, data)

	var ack []byte
	if proto == protoV3 {
		ack = buildPacketV3(pktACK, 0, 0, seq, streamID, 0, 0, 1, 0, nil)
	} else {
		ack = buildPacket(pktACK, 0, 0, seq, streamID, 0, nil)
	}
	cv3.conn.Write(ack)

	cv3.onConfigMu.Lock()
	fn := cv3.onConfig
	cv3.onConfigMu.Unlock()
	if fn != nil {
		go fn(payload)
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// nextMsgID returns a session-scoped monotonic message ID. Thread-safe.
func (cv3 *ClientV3) nextMsgID() uint32 {
	return cv3.msgCounter.Add(1) // sync/atomic.Uint32.Add is available since Go 1.19
}

// ── Diagnostic helpers (shared with test) ─────────────────────────────────────

// NewClientV3FromConn creates a ClientV3 backed by an existing net.Conn.
// Intended for testing (e.g. backed by a net.Pipe or in-memory UDP pair).
// Connect must still be called before Send* methods.
func NewClientV3FromConn(conn net.Conn, tenantID string) *ClientV3 {
	c := &Client{
		conn:     conn,
		tenantID: tenantID,
		inflight: make(map[uint32]*inflightPkt),
	}
	cap := int64(64)
	c.maxInflight.Store(cap)
	c.baseMaxInflight.Store(cap)
	c.baseRTOMs.Store(defaultBaseRTOMs)
	c.maxRTOMs.Store(defaultMaxRTOMs)
	c.maxRetransmitBudget.Store(defaultMaxRetransmitsPerTick)
	return &ClientV3{
		Client:       c,
		inflightMsgs: make(map[uint32]*inflightMsgV3),
	}
}

// ── Logging helpers ───────────────────────────────────────────────────────────

func init() {
	// Silence the "agenticudp v3" prefix in tests unless ENTROPYOPS_DEBUG is set.
	_ = log.Prefix
}
