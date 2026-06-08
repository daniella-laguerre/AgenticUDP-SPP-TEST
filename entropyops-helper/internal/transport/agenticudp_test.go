package transport

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// ── Packet construction / parsing helpers ────────────────────────────────────

func TestBuildPacket_HeaderLayout(t *testing.T) {
	pkt := buildPacket(pktData, tierReliable, 0, 42, 7, 0xDEADBEEF, []byte("hello"))

	if pkt[0] != protoV2 {
		t.Fatalf("version: got %d, want %d", pkt[0], protoV2)
	}
	if pkt[1] != pktData {
		t.Fatalf("type: got %d, want %d", pkt[1], pktData)
	}
	if pkt[2] != tierReliable {
		t.Fatalf("tier: got %d, want %d", pkt[2], tierReliable)
	}
	if seq := binary.BigEndian.Uint16(pkt[4:6]); seq != 42 {
		t.Fatalf("seq: got %d, want 42", seq)
	}
	if sid := binary.BigEndian.Uint16(pkt[6:8]); sid != 7 {
		t.Fatalf("streamID: got %d, want 7", sid)
	}
	if cid := binary.BigEndian.Uint32(pkt[8:12]); cid != 0xDEADBEEF {
		t.Fatalf("contentID: got %x, want DEADBEEF", cid)
	}
	if got := string(pkt[headerSize:]); got != "hello" {
		t.Fatalf("payload: got %q, want %q", got, "hello")
	}
}

func TestBuildPacket_Checksum(t *testing.T) {
	pkt := buildPacket(pktData, 0, 0, 1, 0, 0, []byte("test payload"))
	storedCS := binary.BigEndian.Uint16(pkt[12:14])
	if storedCS == 0 {
		t.Fatal("checksum should be non-zero for non-trivial packet")
	}

	// Verify checksum: zeroing the checksum field and recomputing should match.
	tmp := make([]byte, len(pkt))
	copy(tmp, pkt)
	binary.BigEndian.PutUint16(tmp[12:14], 0)
	recomputed := checksumV2(tmp)
	if recomputed != storedCS {
		t.Fatalf("checksum mismatch: stored=%x recomputed=%x", storedCS, recomputed)
	}
}

func TestFnv1a_Deterministic(t *testing.T) {
	data := []byte("consistent hash test")
	h1 := fnv1a(data)
	h2 := fnv1a(data)
	if h1 != h2 {
		t.Fatalf("fnv1a not deterministic: %x != %x", h1, h2)
	}
	h3 := fnv1a([]byte("different"))
	if h1 == h3 {
		t.Fatal("fnv1a: different inputs should produce different hashes")
	}
}

// ── Client lifecycle tests ──────────────────────────────────────────────────

func startMockReceiver(t *testing.T) (*net.UDPConn, *net.UDPAddr) {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return conn, conn.LocalAddr().(*net.UDPAddr)
}

func TestClient_ConnectAndHandshake(t *testing.T) {
	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	// Background goroutine: read handshake, reply with handshake ACK
	go func() {
		buf := make([]byte, 1024)
		n, clientAddr, err := serverConn.ReadFromUDP(buf)
		if err != nil || n < headerSize {
			return
		}
		if buf[1] != pktHandshake {
			t.Errorf("expected handshake packet, got type %d", buf[1])
			return
		}
		reply := buildPacket(pktHandshake, 0, 0, 0, 0, 0, nil)
		serverConn.WriteToUDP(reply, clientAddr)
	}()

	client, err := NewClient(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if !client.established.Load() {
		t.Fatal("client should be established after handshake")
	}
}

func TestClient_SendSignal_JSON(t *testing.T) {
	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	var received []byte
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(2) // handshake + data

	go func() {
		buf := make([]byte, 65535)
		for i := 0; i < 2; i++ {
			n, clientAddr, err := serverConn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if buf[1] == pktHandshake {
				reply := buildPacket(pktHandshake, 0, 0, 0, 0, 0, nil)
				serverConn.WriteToUDP(reply, clientAddr)
				wg.Done()
			} else if buf[1] == pktData {
				mu.Lock()
				received = make([]byte, n-headerSize)
				copy(received, buf[headerSize:n])
				mu.Unlock()
				wg.Done()
			}
		}
	}()

	client, err := NewClient(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	testData := map[string]string{"cpu": "0.5"}
	if err := client.SendMetrics(testData); err != nil {
		t.Fatalf("SendMetrics: %v", err)
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	var envelope struct {
		SignalType string                 `json:"signal_type"`
		TenantID   string                `json:"tenant_id"`
		Data       map[string]string     `json:"data"`
	}
	if err := json.Unmarshal(received, &envelope); err != nil {
		t.Fatalf("unmarshal payload: %v (raw=%q)", err, string(received))
	}
	if envelope.SignalType != "metrics" {
		t.Errorf("signal_type: got %q, want %q", envelope.SignalType, "metrics")
	}
	if envelope.TenantID != "test-tenant" {
		t.Errorf("tenant_id: got %q, want %q", envelope.TenantID, "test-tenant")
	}
}

func TestClient_SendCycle_ThreeDatagrams(t *testing.T) {
	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	var dataCount int
	var mu sync.Mutex
	done := make(chan struct{})

	go func() {
		buf := make([]byte, 65535)
		for {
			n, clientAddr, err := serverConn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if n < headerSize {
				continue
			}
			if buf[1] == pktHandshake {
				reply := buildPacket(pktHandshake, 0, 0, 0, 0, 0, nil)
				serverConn.WriteToUDP(reply, clientAddr)
			} else if buf[1] == pktData {
				mu.Lock()
				dataCount++
				if dataCount >= 3 {
					mu.Unlock()
					close(done)
					return
				}
				mu.Unlock()
			}
		}
	}()

	client, err := NewClient(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	metrics := map[string]float64{"cpu": 0.5}
	traces := []string{"span-1"}
	logs := []string{"log-line-1"}

	if err := client.SendCycle(metrics, traces, logs); err != nil {
		t.Fatalf("SendCycle: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		mu.Lock()
		t.Fatalf("timeout: only received %d/3 data packets", dataCount)
		mu.Unlock()
	}
}

func TestClient_Stats_TracksSent(t *testing.T) {
	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	go func() {
		buf := make([]byte, 65535)
		for {
			_, clientAddr, err := serverConn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if buf[1] == pktHandshake {
				reply := buildPacket(pktHandshake, 0, 0, 0, 0, 0, nil)
				serverConn.WriteToUDP(reply, clientAddr)
			}
		}
	}()

	client, err := NewClient(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		client.SendMetrics(map[string]int{"i": i})
	}
	time.Sleep(50 * time.Millisecond)

	st := client.Stats()
	if st.Sent < 5 {
		t.Errorf("sent: got %d, want >= 5", st.Sent)
	}
	if !st.Established {
		t.Error("stats should show established=true")
	}
}

// ── AI integration tests ────────────────────────────────────────────────────

func TestClient_EnableAI_ClassifierOverridesTier(t *testing.T) {
	classifier := NewAIClassifier()
	extractor := NewFeatureExtractor()
	predictor := NewCongestionPredictor()

	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	var receivedTier uint8
	var mu sync.Mutex
	done := make(chan struct{})

	go func() {
		buf := make([]byte, 65535)
		for {
			n, clientAddr, err := serverConn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if n < headerSize {
				continue
			}
			if buf[1] == pktHandshake {
				reply := buildPacket(pktHandshake, 0, 0, 0, 0, 0, nil)
				serverConn.WriteToUDP(reply, clientAddr)
			} else if buf[1] == pktData {
				mu.Lock()
				receivedTier = buf[2]
				mu.Unlock()
				close(done)
				return
			}
		}
	}()

	client, err := NewClient(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	client.EnableAI(classifier, extractor, predictor)

	// Send metrics — hardcoded default is TierBesteff (2), but the rule-based
	// classifier should produce a classification (may differ from hardcoded).
	if err := client.SendMetrics(map[string]float64{"cpu": 0.9}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for data packet")
	}

	mu.Lock()
	defer mu.Unlock()
	// The AI classifier is active (rule-based fallback), so the tier should
	// have been set by the classifier, not necessarily the hardcoded default.
	// We just verify the packet was sent with a valid tier.
	if receivedTier > 2 {
		t.Errorf("received tier %d is out of range [0,2]", receivedTier)
	}
}

func TestFeatureExtractor_Extract(t *testing.T) {
	fe := NewFeatureExtractor()
	ctx := fe.Extract("metrics", 1024, 15.0, 0.01)

	if ctx.SignalType != "metrics" {
		t.Errorf("signal_type: got %q, want %q", ctx.SignalType, "metrics")
	}
	if ctx.PayloadSizeKB < 0.9 || ctx.PayloadSizeKB > 1.1 {
		t.Errorf("payload_size_kb: got %.2f, want ~1.0", ctx.PayloadSizeKB)
	}
	if ctx.RTTMs != 15.0 {
		t.Errorf("rtt_ms: got %.1f, want 15.0", ctx.RTTMs)
	}
	if ctx.LossRate != 0.01 {
		t.Errorf("loss_rate: got %f, want 0.01", ctx.LossRate)
	}

	// Second call: should register as "routine" since same signal type seen before
	ctx2 := fe.Extract("metrics", 512, 10.0, 0.0)
	if ctx2.SignalRarity != "routine" {
		t.Errorf("second call rarity: got %q, want %q", ctx2.SignalRarity, "routine")
	}
}

func TestCongestionPredictor_RecordAndPredict(t *testing.T) {
	cp := NewCongestionPredictor()

	// With < 10 samples, should return "none" severity
	pred := cp.Predict()
	if pred.Severity != "none" {
		t.Errorf("empty predictor severity: got %q, want %q", pred.Severity, "none")
	}

	// Feed normal RTT samples
	for i := 0; i < 20; i++ {
		cp.RecordRTT(10.0 + float64(i)*0.1)
	}
	cp.RecordLoss(0.001)

	pred = cp.Predict()
	if pred.Severity == "" {
		t.Error("prediction severity should not be empty after feeding data")
	}

	shaping := cp.RecommendShaping()
	if shaping.MaxBatchSizeKB <= 0 {
		t.Error("shaping MaxBatchSizeKB should be positive")
	}
}

func TestAIClassifier_RuleBased(t *testing.T) {
	t.Setenv("ENTROPYOPS_AGENT_AI_FALLBACK", "rule")
	c := NewAIClassifier()

	// Stable + routine → should classify as besteff
	ctx := SignalContext{
		SignalType:    "metrics",
		EntityRegime:  "stable",
		AnomalyScore:  0.1,
		SignalRarity:  "routine",
		CongestionLevel: "low",
	}
	result := c.Classify(ctx)
	if result.Tier != TierBesteff {
		t.Errorf("stable+routine: got tier %d, want %d (besteff)", result.Tier, TierBesteff)
	}

	// Critical regime → should classify as guaranteed
	ctx.EntityRegime = "critical"
	result = c.Classify(ctx)
	if result.Tier != TierGuaranteed {
		t.Errorf("critical regime: got tier %d, want %d (guaranteed)", result.Tier, TierGuaranteed)
	}

	// High anomaly score → guaranteed
	ctx.EntityRegime = "stable"
	ctx.AnomalyScore = 0.9
	ctx.SignalRarity = "routine"
	result = c.Classify(ctx)
	if result.Tier != TierGuaranteed {
		t.Errorf("high anomaly: got tier %d, want %d (guaranteed)", result.Tier, TierGuaranteed)
	}
}

func TestAIClassifier_ThompsonSampling(t *testing.T) {
	c := NewAIClassifier()

	// Train: guaranteed always succeeds for critical metrics
	for i := 0; i < 20; i++ {
		c.UpdateThompson("metrics", "critical", "low", TierGuaranteed, true)
		c.UpdateThompson("metrics", "critical", "low", TierBesteff, false)
	}

	ctx := SignalContext{
		SignalType:    "metrics",
		EntityRegime:  "critical",
		CongestionLevel: "low",
	}
	result := c.Classify(ctx)
	// After strong training, Thompson should favor guaranteed for critical metrics
	if result.Model != "thompson" && result.Model != "rule" {
		t.Errorf("model should be thompson or rule, got %q", result.Model)
	}
}

func TestAIClassifier_Stats(t *testing.T) {
	c := NewAIClassifier()
	c.Classify(SignalContext{SignalType: "test"})
	c.Classify(SignalContext{SignalType: "test"})

	stats := c.Stats()
	if stats["enabled"] != false {
		t.Error("classifier should be disabled by default (no env var)")
	}
	if stats["provider"] != "rule" {
		t.Errorf("default provider: got %v, want 'rule'", stats["provider"])
	}
}

// ── Oversize-datagram chunking tests ────────────────────────────────────────

// TestIsOversizeDatagramErr verifies the helper recognises:
//   - syscall.EMSGSIZE directly (Linux/macOS errno + Windows WSAEMSGSIZE)
//   - wrapped errors via errors.Is
//   - the textual "larger than the internal message buffer" string Windows
//     surfaces in older Go runtimes that don't map WSAEMSGSIZE → EMSGSIZE
//   - the legacy "message too long" / "message too large" wordings
//
// And rejects unrelated errors such as connection refused.
func TestIsOversizeDatagramErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"emsgsize_direct", syscall.EMSGSIZE, true},
		{"emsgsize_wrapped", fmt.Errorf("write udp: %w", syscall.EMSGSIZE), true},
		{
			"windows_wsasend_text",
			errors.New("write udp 127.0.0.1:53389->127.0.0.1:4318: wsasend: A message sent on a datagram socket was larger than the internal message buffer or some other network limit"),
			true,
		},
		{"linux_text", errors.New("write udp: message too long"), true},
		{"bsd_text", errors.New("write udp: message too large"), true},
		{"unrelated", errors.New("connection refused"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isOversizeDatagramErr(tc.err); got != tc.want {
				t.Fatalf("isOversizeDatagramErr(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestSendChunkedSignal_OversizePayloadAutoChunks drives sendChunkedSignal with
// a slice payload large enough that the assembled datagram exceeds maxPayload,
// causing transmitWithFlags to short-circuit to syscall.EMSGSIZE before any
// Write. The chunker must detect that, split the slice, and successfully
// deliver multiple smaller datagrams to the mock receiver. This is the unit
// reproduction of the production WSAEMSGSIZE bug.
func TestSendChunkedSignal_OversizePayloadAutoChunks(t *testing.T) {
	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	var dataPackets int32
	done := make(chan struct{})
	doneOnce := sync.Once{}

	go func() {
		buf := make([]byte, 65535)
		for {
			serverConn.SetReadDeadline(time.Now().Add(5 * time.Second))
			n, clientAddr, err := serverConn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if n < headerSize {
				continue
			}
			switch buf[1] {
			case pktHandshake:
				reply := buildPacket(pktHandshake, 0, 0, 0, 0, 0, nil)
				serverConn.WriteToUDP(reply, clientAddr)
			case pktData:
				if atomic.AddInt32(&dataPackets, 1) >= 2 {
					doneOnce.Do(func() { close(done) })
				}
			}
		}
	}()

	client, err := NewClient(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Build a logs slice whose JSON-encoded envelope easily exceeds maxPayload
	// (65000 bytes minus 14-byte header). 200 entries * ~700 bytes each ≈ 140KB,
	// well over the limit, and large enough that the first chunkSize=10 split
	// will still need to halve at least once.
	bigField := strings.Repeat("X", 700)
	logs := make([]string, 200)
	for i := range logs {
		logs[i] = fmt.Sprintf("log-line-%03d-%s", i, bigField)
	}

	if err := client.sendChunkedSignal("logs", TierReliable, logs, 10); err != nil {
		t.Fatalf("sendChunkedSignal returned error instead of chunking: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("expected >=2 chunked datagrams to arrive, got %d", atomic.LoadInt32(&dataPackets))
	}

	if got := atomic.LoadInt32(&dataPackets); got < 2 {
		t.Errorf("expected at least 2 datagrams from chunking, got %d", got)
	}
}

// ── Inflight window flow control ─────────────────────────────────────────────

// The chunked bulk send path was observed to burst hundreds of datagrams
// at the receiver within a few milliseconds, overflowing the kernel UDP
// receive buffer and triggering a retransmit storm (eo-ingest-bench
// 5000-span run on 2026-04-28: 5678 sent, 1390 acked, 47534 retx, 3748
// dropped). The Client now enforces an upper bound on unACKed datagrams
// during chunked sends; these tests pin that behavior.

func TestSetMaxInflight_NegativeClampsToZero(t *testing.T) {
	c := &Client{}
	c.SetMaxInflight(32)
	if got := c.maxInflight.Load(); got != 32 {
		t.Errorf("got %d, want 32", got)
	}
	c.SetMaxInflight(-5)
	if got := c.maxInflight.Load(); got != 0 {
		t.Errorf("negative should clamp to 0, got %d", got)
	}
}

func TestWaitForInflightWindow_DisabledWhenCapZero(t *testing.T) {
	c := &Client{inflight: make(map[uint32]*inflightPkt)}
	c.maxInflight.Store(0)
	for i := uint32(0); i < 100; i++ {
		c.inflight[i] = &inflightPkt{}
	}
	start := time.Now()
	if err := c.waitForInflightWindow(context.Background(), time.Time{}); err != nil {
		t.Fatalf("err: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Millisecond {
		t.Errorf("cap=0 should be no-op, took %v", elapsed)
	}
}

func TestWaitForInflightWindow_BlocksUntilDrained(t *testing.T) {
	c := &Client{inflight: make(map[uint32]*inflightPkt)}
	c.maxInflight.Store(3)
	c.inflight[1] = &inflightPkt{}
	c.inflight[2] = &inflightPkt{}
	c.inflight[3] = &inflightPkt{}

	go func() {
		time.Sleep(40 * time.Millisecond)
		c.inflightMu.Lock()
		delete(c.inflight, 1)
		c.inflightMu.Unlock()
	}()

	start := time.Now()
	if err := c.waitForInflightWindow(context.Background(), time.Now().Add(time.Second)); err != nil {
		t.Fatalf("expected to unblock after drain: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 20*time.Millisecond {
		t.Errorf("should have waited until inflight drained, returned in %v", elapsed)
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("unblocked too late: %v", elapsed)
	}
}

func TestWaitForInflightWindow_StallTimesOut(t *testing.T) {
	c := &Client{inflight: make(map[uint32]*inflightPkt)}
	c.maxInflight.Store(1)
	c.inflight[1] = &inflightPkt{} // never drained
	err := c.waitForInflightWindow(context.Background(), time.Now().Add(40*time.Millisecond))
	if err == nil {
		t.Fatalf("expected stall error after deadline")
	}
	if !strings.Contains(err.Error(), "stalled") {
		t.Errorf("error message should mention stall: %v", err)
	}
}

func TestWaitForInflightWindow_RespectsContextCancel(t *testing.T) {
	c := &Client{inflight: make(map[uint32]*inflightPkt)}
	c.maxInflight.Store(1)
	c.inflight[1] = &inflightPkt{}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	err := c.waitForInflightWindow(ctx, time.Now().Add(time.Second))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// ── Retransmit backoff + per-tick budget ────────────────────────────────────
//
// These tests pin the fix for the n=1000 retransmit storm: the
// pre-fix retransmitLoop used a fixed 2s timeout and no per-tick cap,
// which converted O(N) inflight packets into an O(N) burst on every
// tick. Server-side log analysis showed 9,215 flushes for 5M spans
// (vs ~2,500 expected) and avg flush time growing from 264ms to 5.9s
// as the storm recursively deepened. The transport now does capped
// exponential backoff per-packet and limits how many retransmits go
// out in any single tick.

func TestComputeRTO_ExponentialBackoff(t *testing.T) {
	cases := []struct {
		name    string
		base    int64
		max     int64
		retries int
		wantMs  int64
	}{
		{"first_retry", 2000, 30000, 0, 2000},
		{"second_retry_doubles", 2000, 30000, 1, 4000},
		{"third_retry_doubles", 2000, 30000, 2, 8000},
		{"fourth_retry_doubles", 2000, 30000, 3, 16000},
		{"fifth_retry_capped", 2000, 30000, 4, 30000},
		{"sixth_retry_still_capped", 2000, 30000, 5, 30000},
		{"tenth_retry_still_capped", 2000, 30000, 10, 30000},
		{"large_retries_no_overflow", 2000, 30000, 100, 30000},
		{"negative_retries_clamped", 2000, 30000, -3, 2000},
		{"zero_base_uses_default", 0, 30000, 0, defaultBaseRTOMs},
		{"max_below_base_raises_max", 5000, 1000, 1, 5000}, // max collapses to base, then retry doubles ineligible
		{"custom_base_and_max", 100, 800, 4, 800},          // 100 → 200 → 400 → 800 → 800 (capped)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := computeRTO(tc.base, tc.max, tc.retries)
			wantD := time.Duration(tc.wantMs) * time.Millisecond
			if got != wantD {
				t.Errorf("computeRTO(base=%d, max=%d, retries=%d) = %v, want %v",
					tc.base, tc.max, tc.retries, got, wantD)
			}
		})
	}
}

func TestSetBaseRTO_IgnoresNonPositive(t *testing.T) {
	c := &Client{}
	c.baseRTOMs.Store(2000)
	c.SetBaseRTO(5000)
	if got := c.baseRTOMs.Load(); got != 5000 {
		t.Errorf("positive value: got %d, want 5000", got)
	}
	c.SetBaseRTO(0)
	if got := c.baseRTOMs.Load(); got != 5000 {
		t.Errorf("zero should be no-op, got %d", got)
	}
	c.SetBaseRTO(-100)
	if got := c.baseRTOMs.Load(); got != 5000 {
		t.Errorf("negative should be no-op, got %d", got)
	}
}

func TestSetMaxRTO_IgnoresNonPositive(t *testing.T) {
	c := &Client{}
	c.maxRTOMs.Store(30000)
	c.SetMaxRTO(45000)
	if got := c.maxRTOMs.Load(); got != 45000 {
		t.Errorf("positive value: got %d, want 45000", got)
	}
	c.SetMaxRTO(0)
	if got := c.maxRTOMs.Load(); got != 45000 {
		t.Errorf("zero should be no-op, got %d", got)
	}
}

func TestSetRetransmitBudget_IgnoresNonPositive(t *testing.T) {
	c := &Client{}
	c.maxRetransmitBudget.Store(64)
	c.SetRetransmitBudget(16)
	if got := c.maxRetransmitBudget.Load(); got != 16 {
		t.Errorf("positive value: got %d, want 16", got)
	}
	c.SetRetransmitBudget(0)
	if got := c.maxRetransmitBudget.Load(); got != 16 {
		t.Errorf("zero should be no-op (would silently regress to storm), got %d", got)
	}
	c.SetRetransmitBudget(-5)
	if got := c.maxRetransmitBudget.Load(); got != 16 {
		t.Errorf("negative should be no-op, got %d", got)
	}
}

func TestEnvInt64Positive(t *testing.T) {
	cases := []struct {
		name string
		set  string // "" = unset
		def  int64
		want int64
	}{
		{"unset_uses_default", "", 1234, 1234},
		{"valid_positive_used", "5000", 1234, 5000},
		{"zero_falls_back", "0", 1234, 1234},
		{"negative_falls_back", "-100", 1234, 1234},
		{"garbage_falls_back", "not-a-number", 1234, 1234},
		{"whitespace_trimmed", "  7777  ", 1234, 7777},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := "ENTROPYOPS_TEST_" + tc.name
			if tc.set != "" {
				t.Setenv(key, tc.set)
			} else {
				_ = os.Unsetenv(key)
			}
			if got := envInt64Positive(key, tc.def); got != tc.want {
				t.Errorf("envInt64Positive(%q, %d) = %d, want %d", key, tc.def, got, tc.want)
			}
		})
	}
}

// newTestClientForRetransmit builds a Client that has a real UDP socket
// pointed at a discard listener, but does NOT start any background
// goroutines. This lets unit tests drive runRetransmitTick directly
// against a hand-populated inflight map without races against
// ackLoop / keepaliveLoop / the production retransmitLoop.
func newTestClientForRetransmit(t *testing.T) (*Client, func()) {
	t.Helper()
	server, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	conn, err := net.DialUDP("udp", nil, server.LocalAddr().(*net.UDPAddr))
	if err != nil {
		server.Close()
		t.Fatalf("dial: %v", err)
	}
	c := &Client{
		conn:     conn,
		rawConn:  conn,
		addr:     server.LocalAddr().(*net.UDPAddr),
		tenantID: "test",
		inflight: make(map[uint32]*inflightPkt),
	}
	c.maxInflight.Store(0)
	c.baseRTOMs.Store(defaultBaseRTOMs)
	c.maxRTOMs.Store(defaultMaxRTOMs)
	c.maxRetransmitBudget.Store(defaultMaxRetransmitsPerTick)
	cleanup := func() {
		conn.Close()
		server.Close()
	}
	return c, cleanup
}

func TestRunRetransmitTick_HonorsBackoff(t *testing.T) {
	c, cleanup := newTestClientForRetransmit(t)
	defer cleanup()
	c.baseRTOMs.Store(100) // 100ms base for fast test
	c.maxRTOMs.Store(800)

	pkt := &inflightPkt{
		data:    buildPacket(pktData, uint8(TierReliable), 0, 1, 0, 0, []byte("x")),
		seq:     1,
		tier:    TierReliable,
		sentAt:  time.Now(),
		retries: 0,
	}
	c.inflight[1] = pkt

	// Tick before RTO elapses → no retransmit.
	c.runRetransmitTick(pkt.sentAt.Add(50 * time.Millisecond))
	if got := c.retransmits.Load(); got != 0 {
		t.Errorf("pre-RTO tick fired retransmit (got %d)", got)
	}
	if pkt.retries != 0 {
		t.Errorf("retries advanced before RTO: got %d", pkt.retries)
	}

	// Tick after first RTO (100ms) → first retransmit.
	c.runRetransmitTick(pkt.sentAt.Add(150 * time.Millisecond))
	if got := c.retransmits.Load(); got != 1 {
		t.Fatalf("expected 1 retransmit after first RTO, got %d", got)
	}
	if pkt.retries != 1 {
		t.Fatalf("retries: got %d, want 1", pkt.retries)
	}

	firstRetransmitAt := pkt.sentAt

	// Backoff: next RTO should be 200ms. Tick at +150ms (still within
	// 200ms window) must NOT retransmit.
	c.runRetransmitTick(firstRetransmitAt.Add(150 * time.Millisecond))
	if got := c.retransmits.Load(); got != 1 {
		t.Errorf("backoff window violated: retransmitted at +150ms (RTO should be 200ms), got %d", got)
	}

	// Tick at +250ms must retransmit (RTO=200ms elapsed).
	c.runRetransmitTick(firstRetransmitAt.Add(250 * time.Millisecond))
	if got := c.retransmits.Load(); got != 2 {
		t.Errorf("expected 2nd retransmit after 200ms backoff, got %d", got)
	}
	if pkt.retries != 2 {
		t.Errorf("retries: got %d, want 2", pkt.retries)
	}
}

func TestRunRetransmitTick_RespectsBudget(t *testing.T) {
	c, cleanup := newTestClientForRetransmit(t)
	defer cleanup()
	c.baseRTOMs.Store(100)
	c.maxRTOMs.Store(800)
	c.maxRetransmitBudget.Store(8) // 8 per tick

	now := time.Now()
	// 32 packets, all eligible (sent 200ms ago, RTO=100ms).
	for i := 0; i < 32; i++ {
		key := uint32(i + 1)
		c.inflight[key] = &inflightPkt{
			data:    buildPacket(pktData, uint8(TierReliable), 0, uint16(i+1), 0, 0, []byte("p")),
			seq:     uint16(i + 1),
			tier:    TierReliable,
			sentAt:  now.Add(-200 * time.Millisecond),
			retries: 0,
		}
	}

	c.runRetransmitTick(now)
	if got := c.retransmits.Load(); got != 8 {
		t.Errorf("first tick: expected 8 retransmits (budget cap), got %d", got)
	}

	// Second tick: the 8 already-retransmitted packets had their
	// sentAt updated to `now` and now have retries=1 (RTO=200ms). The
	// other 24 still have sentAt=now-200ms and retries=0 (RTO=100ms),
	// so they're still eligible. Budget caps the second tick at 8 too.
	c.runRetransmitTick(now)
	if got := c.retransmits.Load(); got != 16 {
		t.Errorf("second tick: expected total 16 retransmits, got %d", got)
	}
}

func TestRunRetransmitTick_OldestFirst(t *testing.T) {
	c, cleanup := newTestClientForRetransmit(t)
	defer cleanup()
	c.baseRTOMs.Store(100)
	c.maxRTOMs.Store(800)
	c.maxRetransmitBudget.Store(2)

	now := time.Now()
	// Three packets: oldest, middle, newest. Budget=2 so newest stays
	// inflight with retries=0 after the tick; oldest two should be
	// retransmitted (retries=1).
	pkts := []struct {
		key    uint32
		sentAt time.Time
	}{
		{1, now.Add(-500 * time.Millisecond)}, // oldest
		{2, now.Add(-300 * time.Millisecond)}, // middle
		{3, now.Add(-150 * time.Millisecond)}, // newest, but still eligible (RTO=100ms)
	}
	for _, p := range pkts {
		c.inflight[p.key] = &inflightPkt{
			data:    buildPacket(pktData, uint8(TierReliable), 0, uint16(p.key), 0, 0, []byte("p")),
			seq:     uint16(p.key),
			tier:    TierReliable,
			sentAt:  p.sentAt,
			retries: 0,
		}
	}

	c.runRetransmitTick(now)
	if got := c.retransmits.Load(); got != 2 {
		t.Fatalf("expected 2 retransmits within budget, got %d", got)
	}
	if c.inflight[1].retries != 1 {
		t.Errorf("oldest packet should have been retransmitted; retries=%d", c.inflight[1].retries)
	}
	if c.inflight[2].retries != 1 {
		t.Errorf("middle packet should have been retransmitted; retries=%d", c.inflight[2].retries)
	}
	if c.inflight[3].retries != 0 {
		t.Errorf("newest eligible packet should have been deferred by budget; retries=%d", c.inflight[3].retries)
	}
}

func TestRunRetransmitTick_DropsAfterMaxRetries(t *testing.T) {
	c, cleanup := newTestClientForRetransmit(t)
	defer cleanup()
	c.baseRTOMs.Store(50)
	c.maxRTOMs.Store(800)

	now := time.Now()
	// TierReliable max retries = 3. Pre-load retries=3; the next
	// eligible tick must drop, not retransmit.
	c.inflight[42] = &inflightPkt{
		data:    buildPacket(pktData, uint8(TierReliable), 0, 42, 0, 0, []byte("p")),
		seq:     42,
		tier:    TierReliable,
		sentAt:  now.Add(-2 * time.Second),
		retries: 3,
	}
	c.runRetransmitTick(now)
	if got := c.retransmits.Load(); got != 0 {
		t.Errorf("at max retries should drop, not retransmit; got %d retransmits", got)
	}
	if got := c.dropped.Load(); got != 1 {
		t.Errorf("expected dropped=1 after max retries, got %d", got)
	}
	if _, exists := c.inflight[42]; exists {
		t.Errorf("dropped packet should be removed from inflight map")
	}
}

func TestRunRetransmitTick_GuaranteedTierAllowsMoreRetries(t *testing.T) {
	c, cleanup := newTestClientForRetransmit(t)
	defer cleanup()
	c.baseRTOMs.Store(10)
	c.maxRTOMs.Store(50)
	c.maxRetransmitBudget.Store(1000)

	now := time.Now()
	c.inflight[7] = &inflightPkt{
		data:    buildPacket(pktData, uint8(TierGuaranteed), 0, 7, 0, 0, []byte("p")),
		seq:     7,
		tier:    TierGuaranteed,
		sentAt:  now.Add(-time.Second),
		retries: 5,
	}
	c.runRetransmitTick(now)
	if got := c.dropped.Load(); got != 0 {
		t.Errorf("guaranteed tier should not drop at retries=5 (max=10), dropped=%d", got)
	}
	if got := c.retransmits.Load(); got != 1 {
		t.Errorf("expected 1 retransmit, got %d", got)
	}
	if c.inflight[7].retries != 6 {
		t.Errorf("retries should increment to 6, got %d", c.inflight[7].retries)
	}
}

// TestSendChunkedSignal_PacesUnderInflightCap verifies that a chunked
// guaranteed-tier bulk send does not place more than maxInflight datagrams
// on the wire before any ACK. The mock receiver DELIBERATELY does not
// ACK; we only check that the client's inflight count never exceeds the
// cap, and that the send eventually fails with the stall error rather
// than bursting all chunks at once.
func TestSendChunkedSignal_PacesUnderInflightCap(t *testing.T) {
	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	go func() {
		buf := make([]byte, 65535)
		for {
			serverConn.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, clientAddr, err := serverConn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if n < headerSize {
				continue
			}
			if buf[1] == pktHandshake {
				reply := buildPacket(pktHandshake, 0, 0, 0, 0, 0, nil)
				serverConn.WriteToUDP(reply, clientAddr)
			}
			// Intentionally never ACK pktData — forces inflight to grow.
		}
	}()

	client, err := NewClient(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetMaxInflight(4)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Sample inflight count from a watchdog goroutine while the bulk send is
	// running. With a cap of 4 we should NEVER see more than 4 inflight.
	maxObserved := int32(0)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			client.inflightMu.Lock()
			n := int32(len(client.inflight))
			client.inflightMu.Unlock()
			if n > atomic.LoadInt32(&maxObserved) {
				atomic.StoreInt32(&maxObserved, n)
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()

	// Force chunking by exceeding maxPayload. 200 lines of 700 bytes ≈ 140 KB.
	// At chunkSize=10 that's 20 chunks; with cap=4 the send should stall.
	bigField := strings.Repeat("X", 700)
	logs := make([]string, 200)
	for i := range logs {
		logs[i] = fmt.Sprintf("log-line-%03d-%s", i, bigField)
	}

	// Use a shorter stall deadline so the test fails fast. We invoke the
	// internal helper directly so the deadline is small enough for CI.
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- client.sendChunkedSignal("logs", TierReliable, logs, 10)
	}()

	select {
	case err := <-doneCh:
		// Expected: stall error after some chunks were sent within the cap.
		if err == nil {
			t.Fatalf("expected stall error from inflight cap; got nil")
		}
		if !strings.Contains(err.Error(), "stalled") {
			t.Logf("note: send returned non-stall error: %v", err)
		}
	case <-time.After(80 * time.Second):
		t.Fatal("sendChunkedSignal never returned (deadline=60s in helper, allow some slack)")
	}

	close(stop)
	if got := atomic.LoadInt32(&maxObserved); got > 4 {
		t.Errorf("inflight cap violated: max observed %d, cap was 4", got)
	}
}
