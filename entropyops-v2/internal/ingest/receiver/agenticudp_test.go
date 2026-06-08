package receiver

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/entropyops/entropyops-v2/internal/storage"
)

// ── Packet building / parsing ────────────────────────────────────────────────

func TestParseHeaderV2(t *testing.T) {
	pkt := buildPacketV2(pktData, tierReliable, flagProtobuf, 99, 3, 0xCAFEBABE, nil)
	hdr := parseHeaderV2(pkt)

	if hdr.Version != protoV2 {
		t.Errorf("version: got %d, want %d", hdr.Version, protoV2)
	}
	if hdr.Type != pktData {
		t.Errorf("type: got %d, want %d", hdr.Type, pktData)
	}
	if hdr.Tier != tierReliable {
		t.Errorf("tier: got %d, want %d", hdr.Tier, tierReliable)
	}
	if hdr.Flags != flagProtobuf {
		t.Errorf("flags: got %x, want %x", hdr.Flags, flagProtobuf)
	}
	if hdr.Seq != 99 {
		t.Errorf("seq: got %d, want 99", hdr.Seq)
	}
	if hdr.StreamID != 3 {
		t.Errorf("streamID: got %d, want 3", hdr.StreamID)
	}
	if hdr.ContentID != 0xCAFEBABE {
		t.Errorf("contentID: got %x, want CAFEBABE", hdr.ContentID)
	}
}

func TestBuildPacketV2_Checksum(t *testing.T) {
	pkt := buildPacketV2(pktData, 0, 0, 1, 0, 12345, []byte("test data"))
	storedCS := binary.BigEndian.Uint16(pkt[12:14])
	if storedCS == 0 {
		t.Fatal("checksum should be non-zero")
	}

	// Verify round-trip
	r := NewAgenticUDPReceiver(0, "test")
	if !r.verifyChecksum(pkt) {
		t.Fatal("checksum verification failed on freshly built packet")
	}

	// Corrupt a byte and verify checksum fails
	pkt[headerV2Size] ^= 0xFF
	if r.verifyChecksum(pkt) {
		t.Fatal("corrupted packet should fail checksum")
	}
}

func TestCalculateChecksum(t *testing.T) {
	data := []byte{0x00, 0x01, 0x00, 0x02, 0x00, 0x03}
	cs := calculateChecksum(data)
	if cs == 0 {
		t.Error("checksum should be non-zero for non-trivial data")
	}

	// Internet checksum property: checksum of (data + checksum) should be 0xFFFF
	full := make([]byte, len(data)+2)
	copy(full, data)
	binary.BigEndian.PutUint16(full[len(data):], cs)
	verify := calculateChecksum(full)
	if verify != 0 {
		t.Errorf("verification checksum: got %x, want 0", verify)
	}
}

// ── Dedup ────────────────────────────────────────────────────────────────────

func TestIsDuplicate(t *testing.T) {
	r := NewAgenticUDPReceiver(0, "test")

	if r.isDuplicate(1234) {
		t.Error("first occurrence should not be duplicate")
	}
	if !r.isDuplicate(1234) {
		t.Error("second occurrence should be duplicate")
	}
	if r.isDuplicate(5678) {
		t.Error("different contentID should not be duplicate")
	}
}

// ── Session management ──────────────────────────────────────────────────────

func TestTouchSession(t *testing.T) {
	r := NewAgenticUDPReceiver(0, "test")
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5000}

	r.touchSession(addr)

	r.sessionsMu.RLock()
	_, exists := r.sessions[addr.String()]
	r.sessionsMu.RUnlock()

	if !exists {
		t.Error("session should exist after touchSession")
	}
}

func TestHandleFIN(t *testing.T) {
	r := NewAgenticUDPReceiver(0, "test")
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5000}

	r.touchSession(addr)
	r.handleFIN(addr)

	r.sessionsMu.RLock()
	_, exists := r.sessions[addr.String()]
	r.sessionsMu.RUnlock()

	if exists {
		t.Error("session should be removed after FIN")
	}
}

// ── Stats ────────────────────────────────────────────────────────────────────

func TestStats(t *testing.T) {
	r := NewAgenticUDPReceiver(0, "test")
	s := r.Stats()
	if s.PacketsReceived != 0 || s.HandshakesRecv != 0 {
		t.Errorf("initial stats should be zero: %+v", s)
	}
}

// ── Full round-trip loopback test ────────────────────────────────────────────

type mockPipeline struct {
	mu      sync.Mutex
	metrics []storage.Metric
	traces  []storage.Trace
	logs    []storage.LogRecord
}

func (m *mockPipeline) PublishMetrics(tenantID string, metrics []storage.Metric) {
	m.mu.Lock()
	m.metrics = append(m.metrics, metrics...)
	m.mu.Unlock()
}
func (m *mockPipeline) PublishTraces(tenantID string, traces []storage.Trace) {
	m.mu.Lock()
	m.traces = append(m.traces, traces...)
	m.mu.Unlock()
}
func (m *mockPipeline) PublishLogs(tenantID string, logs []storage.LogRecord) {
	m.mu.Lock()
	m.logs = append(m.logs, logs...)
	m.mu.Unlock()
}

func TestReceiver_HandshakeAndData_Loopback(t *testing.T) {
	// Start receiver on random port
	r := NewAgenticUDPReceiver(0, "test-tenant")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Bind manually to get actual port
	addr := &net.UDPAddr{Port: 0}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	actualPort := conn.LocalAddr().(*net.UDPAddr).Port
	r.conn = conn
	r.port = actualPort

	// Start receive loop in background
	go func() {
		buf := make([]byte, maxUDPPayload)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			n, from, err := conn.ReadFromUDP(buf)
			if err != nil {
				continue
			}
			r.handlePacket(buf[:n], from)
		}
	}()

	// Client: send handshake
	clientAddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	clientConn, err := net.DialUDP("udp", clientAddr, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: actualPort})
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	handshake := buildPacketV2(pktHandshake, 0, 0, 0, 0, 0, nil)
	clientConn.Write(handshake)
	time.Sleep(50 * time.Millisecond)

	stats := r.Stats()
	if stats.HandshakesRecv < 1 {
		t.Errorf("handshakes: got %d, want >= 1", stats.HandshakesRecv)
	}

	// Send a JSON data packet (metrics)
	envelope := map[string]interface{}{
		"signal_type": "metrics",
		"tenant_id":   "test-tenant",
		"data":        []map[string]interface{}{{"metric_name": "cpu", "value": 0.75}},
	}
	payload, _ := json.Marshal(envelope)
	dataPkt := buildPacketV2(pktData, tierBesteff, 0, 1, 0, 42, payload)
	clientConn.Write(dataPkt)
	time.Sleep(100 * time.Millisecond)

	stats = r.Stats()
	if stats.DatagramsAccepted < 1 {
		t.Errorf("datagrams_accepted: got %d, want >= 1", stats.DatagramsAccepted)
	}

	// Send keepalive
	kaPkt := buildPacketV2(pktKeepalive, 0, 0, 0, 0, 0, nil)
	clientConn.Write(kaPkt)
	time.Sleep(50 * time.Millisecond)

	stats = r.Stats()
	if stats.KeepalivesRecv < 1 {
		t.Errorf("keepalives: got %d, want >= 1", stats.KeepalivesRecv)
	}

	// Send FIN
	finPkt := buildPacketV2(pktFIN, 0, 0, 0, 0, 0, nil)
	clientConn.Write(finPkt)
	time.Sleep(50 * time.Millisecond)
}

func TestReceiver_InvalidPacket_Dropped(t *testing.T) {
	r := NewAgenticUDPReceiver(0, "test")
	from := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9999}

	// Too short
	r.handlePacket([]byte{0x01, 0x02}, from)
	stats := r.Stats()
	if stats.PacketsInvalid < 1 {
		t.Errorf("short packet should be invalid: got %d", stats.PacketsInvalid)
	}

	// Wrong version
	badVersion := buildPacketV2(pktData, 0, 0, 1, 0, 0, []byte("x"))
	badVersion[0] = 99
	r.handlePacket(badVersion, from)
	stats = r.Stats()
	if stats.PacketsInvalid < 2 {
		t.Errorf("wrong version should be invalid: got %d", stats.PacketsInvalid)
	}

	// Bad checksum
	badCS := buildPacketV2(pktData, 0, 0, 1, 0, 0, []byte("y"))
	badCS[headerV2Size] ^= 0xFF // corrupt payload without updating checksum
	r.handlePacket(badCS, from)
	stats = r.Stats()
	if stats.ChecksumFails < 1 {
		t.Errorf("corrupted packet should fail checksum: got %d", stats.ChecksumFails)
	}
}

func TestReceiver_Dedup_DropsDuplicate(t *testing.T) {
	r := NewAgenticUDPReceiver(0, "test")
	from := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9999}

	payload, _ := json.Marshal(map[string]interface{}{
		"signal_type": "logs",
		"tenant_id":   "test",
		"data":        "log line 1",
	})
	contentID := uint32(12345)
	pkt := buildPacketV2(pktData, tierBesteff, 0, 1, 0, contentID, payload)

	// First packet: should be processed
	r.handlePacket(pkt, from)
	s1 := r.Stats()

	// Same packet again: should be deduped
	r.handlePacket(pkt, from)
	s2 := r.Stats()

	if s2.DatagramsDuped < 1 {
		t.Errorf("duplicate packet should be dropped: dedup_drops=%d", s2.DatagramsDuped)
	}
	_ = s1
}
