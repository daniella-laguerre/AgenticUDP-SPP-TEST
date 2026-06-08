// Factory + receiver scaffolding for the OpenTelemetry Collector
// `agenticudpreceiver`.
//
// The actual integration with `go.opentelemetry.io/collector/component`,
// `consumer.{Logs,Metrics,Traces}`, and `pdata` is intentionally
// expressed as INTERFACES rather than imports so this module compiles
// standalone in CI without pulling the (very large) collector SDK
// graph into our main `entropyops-v2` module.
//
// The OCB build of the collector wires these interfaces to the real
// SDK types — see README.md for the integration recipe.
package agenticudpreceiver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

// Config is the receiver's user-facing YAML config.
type Config struct {
	// Endpoint is the bind address: "host:port". Empty defaults to
	// 0.0.0.0:4320 — same as the EntropyOps server's AgenticUDP listener
	// (moved from 4318 in v2.11 to free the IANA-reserved OTLP HTTP port).
	Endpoint string `mapstructure:"endpoint"`
	// DefaultTenant is applied when an inbound envelope omits
	// `tenant_id`. Mirrors the EntropyOps server's default.
	DefaultTenant string `mapstructure:"default_tenant"`
	// FragmentTTLSeconds bounds how long a partial fragment buffer
	// is held before being reaped. Zero defaults to 30s.
	FragmentTTLSeconds int `mapstructure:"fragment_ttl_seconds"`
	// MaxPacketBytes caps the read buffer (default 65535 — UDP MTU).
	MaxPacketBytes int `mapstructure:"max_packet_bytes"`
}

// Validate returns an error when the config is unusable.
func (c *Config) Validate() error {
	if c.Endpoint == "" {
		c.Endpoint = "0.0.0.0:4320"
	}
	if c.MaxPacketBytes <= 0 {
		c.MaxPacketBytes = 65535
	}
	if c.FragmentTTLSeconds <= 0 {
		c.FragmentTTLSeconds = 30
	}
	return nil
}

// MetricsConsumer is the subset of consumer.Metrics this receiver
// drives. The OCB-built collector binds it to the real
// `go.opentelemetry.io/collector/consumer.Metrics`.
type MetricsConsumer interface {
	ConsumeMetrics(ctx context.Context, env *JSONEnvelope) error
}

// TracesConsumer mirrors MetricsConsumer for traces.
type TracesConsumer interface {
	ConsumeTraces(ctx context.Context, env *JSONEnvelope) error
}

// LogsConsumer mirrors MetricsConsumer for logs.
type LogsConsumer interface {
	ConsumeLogs(ctx context.Context, env *JSONEnvelope) error
}

// Receiver is the long-lived UDP listener. One receiver instance can
// serve all three signal kinds concurrently because the JSON envelope
// declares which lanes are populated.
type Receiver struct {
	cfg       Config
	metricsTo MetricsConsumer
	tracesTo  TracesConsumer
	logsTo    LogsConsumer

	conn       *net.UDPConn
	frags      *FragmentBuffer
	cancel     context.CancelFunc
	wg         sync.WaitGroup

	envelopes int64
	dropped   int64
}

// NewReceiver constructs a receiver. Any consumer may be nil — the
// receiver will silently drop the corresponding signal lane. This
// matches how the upstream OpenTelemetry Collector binds receivers
// per-pipeline.
func NewReceiver(cfg Config, m MetricsConsumer, t TracesConsumer, l LogsConsumer) (*Receiver, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Receiver{
		cfg:       cfg,
		metricsTo: m,
		tracesTo:  t,
		logsTo:    l,
		frags:     NewFragmentBuffer(0),
	}, nil
}

// Start binds the UDP socket and begins draining datagrams. Returns
// immediately; the read loop runs until ctx is cancelled or Stop is
// called.
func (r *Receiver) Start(ctx context.Context) error {
	addr, err := net.ResolveUDPAddr("udp", r.cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", r.cfg.Endpoint, err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", r.cfg.Endpoint, err)
	}
	r.conn = conn
	ctx, r.cancel = context.WithCancel(ctx)
	r.wg.Add(1)
	go r.loop(ctx)
	log.Printf("agenticudpreceiver: listening on %s default_tenant=%q", r.cfg.Endpoint, r.cfg.DefaultTenant)
	return nil
}

// Stop closes the listener and waits for the read goroutine to exit.
func (r *Receiver) Stop() error {
	if r.cancel != nil {
		r.cancel()
	}
	if r.conn != nil {
		_ = r.conn.Close()
	}
	r.wg.Wait()
	return nil
}

// Stats returns thread-safe counters for the OTel collector's self-metrics.
func (r *Receiver) Stats() map[string]int64 {
	return map[string]int64{
		"envelopes": atomic.LoadInt64(&r.envelopes),
		"dropped":   atomic.LoadInt64(&r.dropped),
	}
}

func (r *Receiver) loop(ctx context.Context) {
	defer r.wg.Done()
	buf := make([]byte, r.cfg.MaxPacketBytes)
	for {
		if ctx.Err() != nil {
			return
		}
		n, from, err := r.conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			atomic.AddInt64(&r.dropped, 1)
			continue
		}
		if n < HeaderV2Size {
			atomic.AddInt64(&r.dropped, 1)
			continue
		}
		r.handle(ctx, append([]byte(nil), buf[:n]...), from)
	}
}

func (r *Receiver) handle(ctx context.Context, data []byte, from *net.UDPAddr) {
	hdr, ok := ParseHeader(data)
	if !ok {
		atomic.AddInt64(&r.dropped, 1)
		return
	}
	switch hdr.Type {
	case PktData, PktMetrics:
	default:
		// Control plane packets — handshake, ack/nack, keepalive, FIN.
		// The upstream contrib receiver doesn't need to participate in
		// the reliability handshake; we silently ignore.
		return
	}
	payload := data[HeaderV2Size:]
	full := r.frags.AddAndMaybeAssemble(hdr, payload, from)
	if full == nil {
		return
	}
	if hdr.Flags&FlagProtobuf != 0 {
		// SPP-protobuf payload. Phase 11 will add the decoder; for
		// now, drop with a counter so the collector's debug log makes
		// the gap visible to operators.
		atomic.AddInt64(&r.dropped, 1)
		return
	}
	env, err := DecodeJSONEnvelope(full)
	if err != nil {
		atomic.AddInt64(&r.dropped, 1)
		return
	}
	if env.TenantID == "" {
		env.TenantID = r.cfg.DefaultTenant
	}
	atomic.AddInt64(&r.envelopes, 1)
	r.fanout(ctx, env)
}

func (r *Receiver) fanout(ctx context.Context, env *JSONEnvelope) {
	var firstErr error
	if r.metricsTo != nil && len(env.Metrics) > 0 {
		if err := r.metricsTo.ConsumeMetrics(ctx, env); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if r.tracesTo != nil && len(env.Traces) > 0 {
		if err := r.tracesTo.ConsumeTraces(ctx, env); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if r.logsTo != nil && len(env.Logs) > 0 {
		if err := r.logsTo.ConsumeLogs(ctx, env); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		atomic.AddInt64(&r.dropped, 1)
	}
}

// Factory is the constructor the OCB-built collector calls. The
// upstream contrib repo will rename this to `NewFactory` and bind it
// to `receiver.NewFactory` from the SDK; we keep the interface
// minimal here.
type Factory struct{}

// CreateDefaultConfig returns a Config with collector-friendly
// defaults populated.
func (Factory) CreateDefaultConfig() *Config {
	c := &Config{}
	_ = c.Validate()
	return c
}

// CreateReceiver wires the receiver to its three signal-typed
// consumers. Any consumer may be nil. When all three are nil, the
// factory returns an error to surface configuration mistakes early.
func (Factory) CreateReceiver(cfg *Config, m MetricsConsumer, t TracesConsumer, l LogsConsumer) (*Receiver, error) {
	if m == nil && t == nil && l == nil {
		return nil, errors.New("agenticudpreceiver: at least one of metrics/traces/logs consumer must be configured")
	}
	return NewReceiver(*cfg, m, t, l)
}
