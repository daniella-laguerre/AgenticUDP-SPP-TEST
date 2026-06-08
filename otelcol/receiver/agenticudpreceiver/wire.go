// Package agenticudpreceiver decodes AgenticUDP V2 datagrams into a
// transport-agnostic intermediate representation suitable for handing
// to either the OpenTelemetry Collector pdata model or any other
// downstream encoder.
//
// This file is the wire-format decoder. It is intentionally a clean
// re-implementation of the V2 header logic from
// entropyops-v2/internal/ingest/receiver/agenticudp.go so the
// upstream contrib module has no compile-time dependency on the
// EntropyOps server.
package agenticudpreceiver

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// V2 wire constants — must match agenticudp.go.
const (
	ProtoV2      = 2
	HeaderV2Size = 14

	PktData      = 1
	PktACK       = 2
	PktNACK      = 3
	PktKeepalive = 5
	PktHandshake = 6
	PktFIN       = 7
	PktMTUProbe  = 9
	PktMetrics   = 10

	FlagRetransmit   = 0x01
	FlagEncrypted    = 0x08
	FlagFragment     = 0x10
	FlagLastFragment = 0x20
	FlagProtobuf     = 0x40
)

// HeaderV2 is the 14-byte AgenticUDP V2 header.
type HeaderV2 struct {
	Version   uint8
	Type      uint8
	Tier      uint8
	Flags     uint8
	Seq       uint16
	StreamID  uint16
	ContentID uint32
	Checksum  uint16
}

// ParseHeader decodes a 14-byte V2 header, returning ok=false on any
// length / version mismatch. Mirrors the receiver's strict parsing.
func ParseHeader(data []byte) (HeaderV2, bool) {
	if len(data) < HeaderV2Size {
		return HeaderV2{}, false
	}
	h := HeaderV2{
		Version:   data[0],
		Type:      data[1],
		Tier:      data[2],
		Flags:     data[3],
		Seq:       binary.BigEndian.Uint16(data[4:6]),
		StreamID:  binary.BigEndian.Uint16(data[6:8]),
		ContentID: binary.BigEndian.Uint32(data[8:12]),
		Checksum:  binary.BigEndian.Uint16(data[12:14]),
	}
	if h.Version != ProtoV2 {
		return h, false
	}
	return h, true
}

// Frame is a decoded AgenticUDP V2 frame after fragment reassembly.
type Frame struct {
	Header  HeaderV2
	Payload []byte
	From    *net.UDPAddr
}

// JSONEnvelope is the AgenticUDP V2 JSON-tier payload shape used for
// metrics and traces. Mirrors what the EntropyOps agent writes.
type JSONEnvelope struct {
	TenantID string                   `json:"tenant_id,omitempty"`
	Source   string                   `json:"source,omitempty"`
	Schema   string                   `json:"schema,omitempty"`
	Time     time.Time                `json:"time"`
	Metrics  []JSONMetric             `json:"metrics,omitempty"`
	Traces   []JSONSpan               `json:"traces,omitempty"`
	Logs     []JSONLog                `json:"logs,omitempty"`
	Resource map[string]string        `json:"resource,omitempty"`
	Extra    map[string]interface{}   `json:"extra,omitempty"`
}

// JSONMetric is the metric subdocument.
type JSONMetric struct {
	Name        string            `json:"name"`
	Value       float64           `json:"value"`
	Unit        string            `json:"unit,omitempty"`
	ServiceName string            `json:"service_name,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Time        time.Time         `json:"time"`
}

// JSONSpan is the trace subdocument.
type JSONSpan struct {
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id,omitempty"`
	ServiceName  string            `json:"service_name,omitempty"`
	Name         string            `json:"name"`
	Kind         string            `json:"kind,omitempty"`
	StartTime    time.Time         `json:"start_time"`
	EndTime      time.Time         `json:"end_time"`
	StatusCode   string            `json:"status_code,omitempty"`
	Attributes   map[string]string `json:"attributes,omitempty"`
}

// JSONLog is the log subdocument.
type JSONLog struct {
	TraceID      string            `json:"trace_id,omitempty"`
	SpanID       string            `json:"span_id,omitempty"`
	ServiceName  string            `json:"service_name,omitempty"`
	SeverityText string            `json:"severity_text,omitempty"`
	Body         string            `json:"body"`
	Time         time.Time         `json:"time"`
	Attributes   map[string]string `json:"attributes,omitempty"`
}

// FragmentBuffer reassembles fragmented payloads keyed by ContentID.
// Safe for concurrent use; not exported as a thread-safe map for
// callers because each receiver owns its own buffer.
type FragmentBuffer struct {
	mu        sync.Mutex
	parts     map[uint32]*fragmentEntry
	maxAge    time.Duration
	createdAt time.Time
}

type fragmentEntry struct {
	parts    map[uint16][]byte
	lastSeq  uint16
	complete bool
	flags    uint8
	created  time.Time
	from     *net.UDPAddr
}

// NewFragmentBuffer returns a fragment reassembly buffer that drops
// entries older than maxAge.
func NewFragmentBuffer(maxAge time.Duration) *FragmentBuffer {
	if maxAge <= 0 {
		maxAge = 30 * time.Second
	}
	return &FragmentBuffer{
		parts:  make(map[uint32]*fragmentEntry),
		maxAge: maxAge,
	}
}

// AddAndMaybeAssemble takes a fragment-flagged datagram and returns
// the assembled payload when the last fragment arrives. Returns nil
// while still gathering parts.
func (b *FragmentBuffer) AddAndMaybeAssemble(h HeaderV2, payload []byte, from *net.UDPAddr) []byte {
	if h.Flags&FlagFragment == 0 {
		return payload
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	entry, ok := b.parts[h.ContentID]
	if !ok {
		entry = &fragmentEntry{
			parts:   make(map[uint16][]byte, 8),
			created: time.Now(),
			from:    from,
			flags:   h.Flags,
		}
		b.parts[h.ContentID] = entry
	}
	entry.parts[h.Seq] = append([]byte(nil), payload...)
	if h.Flags&FlagLastFragment != 0 {
		entry.lastSeq = h.Seq
		entry.complete = true
	}
	if !entry.complete {
		return nil
	}
	if uint16(len(entry.parts)) <= entry.lastSeq {
		return nil
	}
	out := make([]byte, 0, 1024)
	for i := uint16(0); i <= entry.lastSeq; i++ {
		part, present := entry.parts[i]
		if !present {
			return nil
		}
		out = append(out, part...)
	}
	delete(b.parts, h.ContentID)
	return out
}

// Reap drops fragment entries older than the configured maxAge.
func (b *FragmentBuffer) Reap() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	cutoff := time.Now().Add(-b.maxAge)
	dropped := 0
	for id, entry := range b.parts {
		if entry.created.Before(cutoff) {
			delete(b.parts, id)
			dropped++
		}
	}
	return dropped
}

// DecodeJSONEnvelope parses an AgenticUDP V2 JSON payload. Returns an
// error when the payload is empty or non-JSON.
func DecodeJSONEnvelope(payload []byte) (*JSONEnvelope, error) {
	if len(payload) == 0 {
		return nil, errors.New("empty payload")
	}
	if payload[0] != '{' && payload[0] != '[' {
		return nil, fmt.Errorf("not JSON (first byte=0x%02x)", payload[0])
	}
	var env JSONEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return nil, fmt.Errorf("json decode: %w", err)
	}
	return &env, nil
}
