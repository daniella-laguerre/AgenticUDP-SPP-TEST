package transport

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"
)

// ── V3 packet construction ────────────────────────────────────────────────────

func TestBuildPacketV3_HeaderLayout(t *testing.T) {
	payload := []byte("v3 payload")
	pkt := buildPacketV3(pktData, tierGuaranteed, flagV3FragStart, 99, 7, 42, 1, 3, 0xCAFEBABE, payload)

	if len(pkt) != headerSizeV3+len(payload) {
		t.Fatalf("packet length: got %d, want %d", len(pkt), headerSizeV3+len(payload))
	}
	if pkt[0] != protoV3 {
		t.Fatalf("version: got %d, want %d", pkt[0], protoV3)
	}
	if pkt[1] != pktData {
		t.Fatalf("pkt_type: got %d, want pktData(%d)", pkt[1], pktData)
	}
	if pkt[2] != tierGuaranteed {
		t.Fatalf("tier: got %d, want tierGuaranteed(%d)", pkt[2], tierGuaranteed)
	}
	if pkt[3] != flagV3FragStart {
		t.Fatalf("flags: got %d, want %d", pkt[3], flagV3FragStart)
	}
	if seq := binary.BigEndian.Uint16(pkt[4:6]); seq != 99 {
		t.Fatalf("seq: got %d, want 99", seq)
	}
	if sid := binary.BigEndian.Uint16(pkt[6:8]); sid != 7 {
		t.Fatalf("stream_id: got %d, want 7", sid)
	}
	if mid := binary.BigEndian.Uint32(pkt[8:12]); mid != 42 {
		t.Fatalf("msg_id: got %d, want 42", mid)
	}
	if fo := binary.BigEndian.Uint16(pkt[12:14]); fo != 1 {
		t.Fatalf("frag_offset: got %d, want 1", fo)
	}
	if ft := binary.BigEndian.Uint16(pkt[14:16]); ft != 3 {
		t.Fatalf("frag_total: got %d, want 3", ft)
	}
	if cid := binary.BigEndian.Uint32(pkt[16:20]); cid != 0xCAFEBABE {
		t.Fatalf("content_id: got %x, want CAFEBABE", cid)
	}
	if got := string(pkt[headerSizeV3:]); got != string(payload) {
		t.Fatalf("payload: got %q, want %q", got, payload)
	}
}

func TestBuildPacketV3_Checksum(t *testing.T) {
	pkt := buildPacketV3(pktData, 0, 0, 1, 0, 100, 0, 1, 0, []byte("checksum test"))
	storedCS := binary.BigEndian.Uint16(pkt[20:22])
	if storedCS == 0 {
		t.Fatal("checksum should be non-zero for non-trivial packet")
	}
	// Zero the checksum field and recompute — must match.
	tmp := make([]byte, len(pkt))
	copy(tmp, pkt)
	binary.BigEndian.PutUint16(tmp[20:22], 0)
	if recomputed := checksumV2(tmp); recomputed != storedCS {
		t.Fatalf("checksum mismatch: stored=%x recomputed=%x", storedCS, recomputed)
	}
}

func TestBuildPacketV3_SingleDatagram(t *testing.T) {
	// A single-datagram message has frag_offset=0, frag_total=1, no fragment flags.
	pkt := buildPacketV3(pktData, tierGuaranteed, 0, 1, 0, 1, 0, 1, 0, []byte("single"))
	if fo := binary.BigEndian.Uint16(pkt[12:14]); fo != 0 {
		t.Fatalf("frag_offset: want 0, got %d", fo)
	}
	if ft := binary.BigEndian.Uint16(pkt[14:16]); ft != 1 {
		t.Fatalf("frag_total: want 1, got %d", ft)
	}
	if pkt[3] != 0 {
		t.Fatalf("flags: want 0 (no fragment flags), got %d", pkt[3])
	}
}

// ── V3 fragmentation ──────────────────────────────────────────────────────────

func TestClientV3_Fragmentation_CorrectFragCount(t *testing.T) {
	// Use a small per-fragment size so we stay within macOS's default UDP
	// send-buffer limit (SO_SNDBUF ≈ 9216 B on loopback).  Production code
	// always uses maxPayloadV3 (64978 B); fragPayloadSize overrides it only
	// in tests.
	const testFragSize = 400
	payloadSize := testFragSize*2 + testFragSize/2 // ~2.5 × testFragSize → 3 frags
	data := make([]byte, payloadSize)
	for i := range data {
		data[i] = byte(i)
	}

	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	type received struct {
		n     int
		buf   []byte
		flags uint8
		seq   uint16
		msgID uint32
		fo    uint16
		ft    uint16
	}
	var pkts []received
	var mu sync.Mutex
	done := make(chan struct{})

	go func() {
		buf := make([]byte, maxPayload)
		for {
			serverConn.SetReadDeadline(time.Now().Add(3 * time.Second))
			n, _, err := serverConn.ReadFromUDP(buf)
			if err != nil {
				close(done)
				return
			}
			if n < headerSizeV3 || buf[0] != protoV3 {
				continue // skip handshake and other non-V3 packets
			}
			cp := make([]byte, n)
			copy(cp, buf[:n])
			mu.Lock()
			pkts = append(pkts, received{
				n:     n,
				buf:   cp,
				flags: cp[3],
				seq:   binary.BigEndian.Uint16(cp[4:6]),
				msgID: binary.BigEndian.Uint32(cp[8:12]),
				fo:    binary.BigEndian.Uint16(cp[12:14]),
				ft:    binary.BigEndian.Uint16(cp[14:16]),
			})
			mu.Unlock()
		}
	}()

	client, err := NewClientV3(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Override fragment size for test (avoids OS UDP send-buffer limits).
	client.fragPayloadSize = testFragSize

	// Directly mark established so we can transmit without a live server handshake.
	client.established.Store(true)

	if err := client.transmitFragmentedV3(data, TierBesteff, 0); err != nil {
		t.Fatalf("transmitFragmentedV3: %v", err)
	}

	expectedFrags := (payloadSize + testFragSize - 1) / testFragSize

	// Wait for all expected fragments to arrive.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(pkts)
		mu.Unlock()
		if n >= expectedFrags {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(pkts) < expectedFrags {
		t.Fatalf("expected %d fragments, got %d", expectedFrags, len(pkts))
	}

	// All fragments must share the same msgID.
	msgID := pkts[0].msgID
	for i, p := range pkts {
		if p.msgID != msgID {
			t.Errorf("fragment %d: msg_id=%d, want %d", i, p.msgID, msgID)
		}
	}

	// frag_total must equal the expected fragment count.
	for i, p := range pkts {
		if int(p.ft) != expectedFrags {
			t.Errorf("fragment %d: frag_total=%d, want %d", i, p.ft, expectedFrags)
		}
	}

	// frag_offsets must be 0, 1, 2, … in order.
	for i, p := range pkts {
		if int(p.fo) != i {
			t.Errorf("fragment %d: frag_offset=%d, want %d", i, p.fo, i)
		}
	}

	// First fragment must have FRAG_START flag; last must have FRAG_END.
	if pkts[0].flags&flagV3FragStart == 0 {
		t.Error("first fragment: FRAG_START flag not set")
	}
	if pkts[len(pkts)-1].flags&flagV3FragEnd == 0 {
		t.Error("last fragment: FRAG_END flag not set")
	}

	// All fragments must have FRAGMENTED flag.
	for i, p := range pkts {
		if p.flags&flagV3Fragmented == 0 {
			t.Errorf("fragment %d: FRAGMENTED flag not set", i)
		}
	}

	// Reassemble and verify content.
	var reassembled []byte
	for _, p := range pkts {
		reassembled = append(reassembled, p.buf[headerSizeV3:]...)
	}
	if len(reassembled) != len(data) {
		t.Fatalf("reassembled len %d, want %d", len(reassembled), len(data))
	}
	for i := range data {
		if reassembled[i] != data[i] {
			t.Fatalf("reassembled byte %d: got %d, want %d", i, reassembled[i], data[i])
		}
	}
}

func TestClientV3_TransmitSingle_V3Header(t *testing.T) {
	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	received := make(chan []byte, 1)
	go func() {
		buf := make([]byte, maxPayload)
		serverConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _, err := serverConn.ReadFromUDP(buf)
		if err != nil || n < headerSizeV3 {
			return
		}
		cp := make([]byte, n)
		copy(cp, buf[:n])
		received <- cp
	}()

	client, err := NewClientV3(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.established.Store(true)

	payload := []byte(`{"signal_type":"traces"}`)
	if err := client.transmitSingleV3(payload, TierGuaranteed, 5, 0); err != nil {
		t.Fatalf("transmitSingleV3: %v", err)
	}

	select {
	case pkt := <-received:
		if pkt[0] != protoV3 {
			t.Fatalf("version: got %d, want protoV3=%d", pkt[0], protoV3)
		}
		if sid := binary.BigEndian.Uint16(pkt[6:8]); sid != 5 {
			t.Fatalf("stream_id: got %d, want 5", sid)
		}
		if ft := binary.BigEndian.Uint16(pkt[14:16]); ft != 1 {
			t.Fatalf("frag_total: got %d, want 1 (single-datagram)", ft)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for V3 datagram")
	}
}

// ── V3 SACK handling ──────────────────────────────────────────────────────────

func TestClientV3_SACK_AcksMultipleSeqs(t *testing.T) {
	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	// clientAddr holds the address of the first datagram we receive.
	clientAddrCh := make(chan *net.UDPAddr, 1)

	go func() {
		buf := make([]byte, maxPayload)
		serverConn.SetReadDeadline(time.Now().Add(3 * time.Second))
		n, addr, err := serverConn.ReadFromUDP(buf)
		if err != nil || n < headerSize {
			return
		}
		// Capture client address for the SACK reply.
		clientAddrCh <- addr
	}()

	client, err := NewClientV3(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.established.Store(true)

	// Send three TierGuaranteed datagrams; they will be tracked in inflight.
	var sentSeqs []uint16
	for i := 0; i < 3; i++ {
		seqBefore := uint16(client.seqCounter.Load())
		if err := client.transmitSingleV3([]byte("data"), TierGuaranteed, 0, 0); err != nil {
			t.Fatalf("transmit %d: %v", i, err)
		}
		sentSeqs = append(sentSeqs, seqBefore+1)
	}

	// Start ackLoopV3 so SACK packets are processed.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go client.ackLoopV3(ctx)

	// Wait for client address from the server's perspective.
	var clientAddr *net.UDPAddr
	select {
	case clientAddr = <-clientAddrCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for client to send")
	}

	// Build and send a SACK ACKing all 3 seqs in one packet.
	sackPayload := make([]byte, len(sentSeqs)*2)
	for i, seq := range sentSeqs {
		binary.BigEndian.PutUint16(sackPayload[i*2:], seq)
	}
	sack := buildPacketV3(pktSACK, 0, 0, 0, 0, 0, 0, 1, 0, sackPayload)
	if _, err := serverConn.WriteToUDP(sack, clientAddr); err != nil {
		t.Fatalf("send SACK: %v", err)
	}

	// Wait for ackLoopV3 to process all 3 ACKs.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if client.Stats().Acked == 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := client.Stats().Acked; got != 3 {
		t.Fatalf("Acked: got %d, want 3", got)
	}

	// All inflight entries should be cleared.
	client.inflightMu.Lock()
	remaining := len(client.inflight)
	client.inflightMu.Unlock()
	if remaining != 0 {
		t.Fatalf("inflight after SACK: got %d, want 0", remaining)
	}
}

func TestClientV3_SACK_NoDuplicateAck(t *testing.T) {
	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	clientAddrCh := make(chan *net.UDPAddr, 1)
	go func() {
		buf := make([]byte, maxPayload)
		serverConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, addr, err := serverConn.ReadFromUDP(buf)
		if err == nil {
			clientAddrCh <- addr
		}
	}()

	client, err := NewClientV3(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.established.Store(true)

	seqBefore := uint16(client.seqCounter.Load())
	if err := client.transmitSingleV3([]byte("x"), TierGuaranteed, 0, 0); err != nil {
		t.Fatal(err)
	}
	sentSeq := seqBefore + 1

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go client.ackLoopV3(ctx)

	var clientAddr *net.UDPAddr
	select {
	case clientAddr = <-clientAddrCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	// Send the same SACK twice — acked should only increment once.
	sackPayload := make([]byte, 2)
	binary.BigEndian.PutUint16(sackPayload, sentSeq)
	sack := buildPacketV3(pktSACK, 0, 0, 0, 0, 0, 0, 1, 0, sackPayload)
	serverConn.WriteToUDP(sack, clientAddr)
	time.Sleep(50 * time.Millisecond)
	serverConn.WriteToUDP(sack, clientAddr) // duplicate
	time.Sleep(50 * time.Millisecond)

	if got := client.Stats().Acked; got != 1 {
		t.Fatalf("Acked after duplicate SACK: got %d, want 1", got)
	}
}

// ── V3 pipeline task dispatch ─────────────────────────────────────────────────

func TestClientV3_SetPipelineTaskHandler_Dispatch(t *testing.T) {
	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	clientAddrCh := make(chan *net.UDPAddr, 1)
	go func() {
		buf := make([]byte, maxPayload)
		serverConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, addr, err := serverConn.ReadFromUDP(buf)
		if err == nil {
			clientAddrCh <- addr
		}
	}()

	client, err := NewClientV3(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.established.Store(true)

	// Register handler before starting ackLoopV3.
	taskCh := make(chan PipelineTask, 1)
	client.SetPipelineTaskHandler(func(task PipelineTask) {
		taskCh <- task
	})

	// Send a dummy datagram so the server learns the client address.
	client.transmitSingleV3([]byte("ping"), TierBesteff, 0, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go client.ackLoopV3(ctx)

	var clientAddr *net.UDPAddr
	select {
	case clientAddr = <-clientAddrCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout getting client addr")
	}

	// Server pushes a pipeline task via pktConfig.
	task := PipelineTask{
		TaskID:   "task-001",
		TaskType: "inference",
		StreamID: 42,
		Params:   json.RawMessage(`{"model":"llama3"}`),
	}
	payload, _ := json.Marshal(task)
	cfg := buildPacketV3(pktConfig, 0, 0, 1, 0, 0, 0, 1, 0, payload)
	if _, err := serverConn.WriteToUDP(cfg, clientAddr); err != nil {
		t.Fatalf("WriteToUDP config: %v", err)
	}

	select {
	case received := <-taskCh:
		if received.TaskID != "task-001" {
			t.Errorf("TaskID: got %q, want task-001", received.TaskID)
		}
		if received.TaskType != "inference" {
			t.Errorf("TaskType: got %q, want inference", received.TaskType)
		}
		if received.StreamID != 42 {
			t.Errorf("StreamID: got %d, want 42", received.StreamID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for pipeline task dispatch")
	}
}

func TestClientV3_SetPipelineTaskHandler_ExpiredDeadline(t *testing.T) {
	// A task whose DeadlineMs is in the past must not be dispatched.
	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	clientAddrCh := make(chan *net.UDPAddr, 1)
	go func() {
		buf := make([]byte, maxPayload)
		serverConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, addr, err := serverConn.ReadFromUDP(buf)
		if err == nil {
			clientAddrCh <- addr
		}
	}()

	client, err := NewClientV3(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.established.Store(true)

	dispatched := make(chan struct{}, 1)
	client.SetPipelineTaskHandler(func(task PipelineTask) {
		dispatched <- struct{}{}
	})

	client.transmitSingleV3([]byte("ping"), TierBesteff, 0, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go client.ackLoopV3(ctx)

	var clientAddr *net.UDPAddr
	select {
	case clientAddr = <-clientAddrCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	// Task with deadline 1 ms in the past.
	task := PipelineTask{
		TaskID:     "expired",
		TaskType:   "noop",
		DeadlineMs: time.Now().Add(-1 * time.Millisecond).UnixMilli(),
	}
	payload, _ := json.Marshal(task)
	cfg := buildPacketV3(pktConfig, 0, 0, 1, 0, 0, 0, 1, 0, payload)
	serverConn.WriteToUDP(cfg, clientAddr)

	select {
	case <-dispatched:
		t.Fatal("expired task should not have been dispatched")
	case <-time.After(200 * time.Millisecond):
		// Correct: handler was not called.
	}
}

// ── V3 connect + handshake ────────────────────────────────────────────────────

func TestClientV3_ConnectHandshake(t *testing.T) {
	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	// Server responds to V2-format handshake (backward compat).
	go func() {
		buf := make([]byte, 1024)
		serverConn.SetReadDeadline(time.Now().Add(3 * time.Second))
		n, clientAddr, err := serverConn.ReadFromUDP(buf)
		if err != nil || n < headerSize {
			return
		}
		if buf[1] == pktHandshake {
			reply := buildPacket(pktHandshake, 0, 0, 0, 0, 0, nil)
			serverConn.WriteToUDP(reply, clientAddr)
		}
	}()

	client, err := NewClientV3(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("ClientV3.Connect: %v", err)
	}
	if !client.established.Load() {
		t.Fatal("ClientV3 should be established after handshake")
	}
}

// ── V3 backward compat: V2 ACK processed by ackLoopV3 ────────────────────────

func TestClientV3_ackLoopV3_AcceptsV2ACK(t *testing.T) {
	// If the server sends a V2-format ACK (protoV2=2) in a V3 session (e.g.,
	// rolling upgrade), ackLoopV3 must process it correctly.
	serverConn, serverAddr := startMockReceiver(t)
	defer serverConn.Close()

	clientAddrCh := make(chan *net.UDPAddr, 1)
	go func() {
		buf := make([]byte, maxPayload)
		serverConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, addr, err := serverConn.ReadFromUDP(buf)
		if err == nil {
			clientAddrCh <- addr
		}
	}()

	client, err := NewClientV3(serverAddr.String(), "test-tenant")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.established.Store(true)

	seqBefore := uint16(client.seqCounter.Load())
	if err := client.transmitSingleV3([]byte("probe"), TierGuaranteed, 0, 0); err != nil {
		t.Fatal(err)
	}
	sentSeq := seqBefore + 1

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go client.ackLoopV3(ctx)

	var clientAddr *net.UDPAddr
	select {
	case clientAddr = <-clientAddrCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	// Send a V2-format ACK (protoV2 header, 14 bytes).
	v2ack := buildPacket(pktACK, 0, 0, sentSeq, 0, 0, nil)
	serverConn.WriteToUDP(v2ack, clientAddr)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if client.Stats().Acked == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := client.Stats().Acked; got != 1 {
		t.Fatalf("V2 ACK via ackLoopV3: Acked=%d, want 1", got)
	}
}
