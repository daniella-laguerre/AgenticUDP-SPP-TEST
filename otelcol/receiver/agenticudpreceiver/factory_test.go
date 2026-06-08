package agenticudpreceiver

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfig_Validate_AppliesDefaults(t *testing.T) {
	c := &Config{}
	if err := c.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if c.Endpoint != "0.0.0.0:4320" {
		t.Errorf("default endpoint = %q, want 0.0.0.0:4320", c.Endpoint)
	}
	if c.MaxPacketBytes != 65535 {
		t.Errorf("default max packet = %d, want 65535", c.MaxPacketBytes)
	}
	if c.FragmentTTLSeconds != 30 {
		t.Errorf("default fragment ttl = %d, want 30", c.FragmentTTLSeconds)
	}
}

func TestParseHeader_RejectsBadVersion(t *testing.T) {
	buf := make([]byte, HeaderV2Size)
	buf[0] = 1 // wrong version
	_, ok := ParseHeader(buf)
	if ok {
		t.Error("ParseHeader accepted version 1; expected reject")
	}
}

func TestParseHeader_RejectsShortPacket(t *testing.T) {
	_, ok := ParseHeader([]byte{2, 1, 0, 0})
	if ok {
		t.Error("ParseHeader accepted short packet")
	}
}

func TestParseHeader_AcceptsV2(t *testing.T) {
	buf := encodeHeader(HeaderV2{
		Version:   ProtoV2,
		Type:      PktData,
		Tier:      0,
		Flags:     0,
		Seq:       7,
		StreamID:  42,
		ContentID: 0xCAFEF00D,
		Checksum:  0x1234,
	})
	h, ok := ParseHeader(buf)
	if !ok {
		t.Fatal("ParseHeader rejected valid V2 header")
	}
	if h.Type != PktData || h.Seq != 7 || h.ContentID != 0xCAFEF00D {
		t.Errorf("decoded header mismatch: %+v", h)
	}
}

func TestDecodeJSONEnvelope(t *testing.T) {
	payload := []byte(`{"tenant_id":"acme","time":"2026-04-27T00:00:00Z","metrics":[{"name":"x","value":1.5,"time":"2026-04-27T00:00:00Z"}]}`)
	env, err := DecodeJSONEnvelope(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.TenantID != "acme" {
		t.Errorf("tenant = %q, want acme", env.TenantID)
	}
	if len(env.Metrics) != 1 || env.Metrics[0].Name != "x" {
		t.Errorf("metrics decoded incorrectly: %+v", env.Metrics)
	}
}

func TestDecodeJSONEnvelope_RejectsNonJSON(t *testing.T) {
	if _, err := DecodeJSONEnvelope([]byte{0x01, 0x02, 0x03}); err == nil {
		t.Error("DecodeJSONEnvelope accepted binary garbage")
	}
}

func TestFactory_RejectsAllNilConsumers(t *testing.T) {
	f := Factory{}
	cfg := f.CreateDefaultConfig()
	if _, err := f.CreateReceiver(cfg, nil, nil, nil); err == nil {
		t.Error("CreateReceiver allowed all-nil consumers")
	}
}

func TestReceiver_RoundTripJSON(t *testing.T) {
	mc := &countingMetrics{}
	cfg := Config{Endpoint: "127.0.0.1:0"}
	r, err := NewReceiver(cfg, mc, nil, nil)
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	r.conn = conn
	r.cfg.Endpoint = conn.LocalAddr().String()
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.wg.Add(1)
	go r.loop(ctx)

	send, err := net.DialUDP("udp", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer send.Close()

	hdr := encodeHeader(HeaderV2{Version: ProtoV2, Type: PktData})
	body := []byte(`{"tenant_id":"acme","time":"2026-04-27T00:00:00Z","metrics":[{"name":"k","value":2,"time":"2026-04-27T00:00:00Z"}]}`)
	if _, err := send.Write(append(hdr, body...)); err != nil {
		t.Fatalf("send: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&mc.calls) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = r.Stop()
	if atomic.LoadInt64(&mc.calls) == 0 {
		t.Fatalf("metrics consumer never invoked; stats=%v", r.Stats())
	}
}

type countingMetrics struct {
	mu    sync.Mutex
	calls int64
}

func (c *countingMetrics) ConsumeMetrics(_ context.Context, _ *JSONEnvelope) error {
	atomic.AddInt64(&c.calls, 1)
	return nil
}

func encodeHeader(h HeaderV2) []byte {
	buf := make([]byte, HeaderV2Size)
	buf[0] = h.Version
	buf[1] = h.Type
	buf[2] = h.Tier
	buf[3] = h.Flags
	binary.BigEndian.PutUint16(buf[4:6], h.Seq)
	binary.BigEndian.PutUint16(buf[6:8], h.StreamID)
	binary.BigEndian.PutUint32(buf[8:12], h.ContentID)
	binary.BigEndian.PutUint16(buf[12:14], h.Checksum)
	return buf
}

func TestFragmentBuffer_AssemblesInOrder(t *testing.T) {
	fb := NewFragmentBuffer(0)
	from := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
	h := HeaderV2{Version: ProtoV2, Type: PktData, Flags: FlagFragment, ContentID: 1}
	if got := fb.AddAndMaybeAssemble(h, []byte("ab"), from); got != nil {
		t.Errorf("intermediate fragment returned %q, want nil", got)
	}
	h2 := h
	h2.Seq = 1
	h2.Flags = FlagFragment | FlagLastFragment
	got := fb.AddAndMaybeAssemble(h2, []byte("cd"), from)
	if string(got) != "abcd" {
		t.Errorf("assembled = %q, want abcd", got)
	}
}

func TestFragmentBuffer_NonFragmentPassThrough(t *testing.T) {
	fb := NewFragmentBuffer(0)
	h := HeaderV2{Version: ProtoV2, Type: PktData}
	got := fb.AddAndMaybeAssemble(h, []byte("hi"), nil)
	if string(got) != "hi" {
		t.Errorf("non-fragment passthrough = %q, want hi", got)
	}
}
