package otlpshim

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// Type aliases keep the recv* helper signatures readable without
// dragging the long collector* import paths into shim.go's API surface.
type (
	tracesReq  = *collectortrace.ExportTraceServiceRequest
	metricsReq = *collectormetrics.ExportMetricsServiceRequest
	logsReq    = *collectorlogs.ExportLogsServiceRequest
)

// countSpans walks the OTLP request tree and returns the number of leaf
// spans. Used for counters; cheap relative to the protobuf decode that
// already happened in the listener.
func countSpans(req tracesReq) int {
	n := 0
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			n += len(ss.GetSpans())
		}
	}
	return n
}

func countDataPoints(req metricsReq) int {
	n := 0
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			n += len(sm.GetMetrics())
		}
	}
	return n
}

func countLogs(req logsReq) int {
	n := 0
	for _, rl := range req.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			n += len(sl.GetLogRecords())
		}
	}
	return n
}

// Config controls the shim's listener bindings. Only loopback exposure is
// recommended; see the package docstring for collision considerations
// against an on-host Core.
//
// The transport choice (Path A / B / C) is NOT in this struct because
// it's encoded in the Forwarder implementation passed to New(). The
// agent main builds the appropriate forwarder from operator flags and
// passes it in. That keeps the shim's surface narrow and the wire
// implementations independently testable.
type Config struct {
	// Bind is the IP the listeners bind to. Defaults to "127.0.0.1" so a
	// stray app on the LAN can't push raw OTLP into the agent's loopback
	// trust boundary. Set to "0.0.0.0" only when the agent is intentionally
	// acting as the OTLP collector for a host's containers.
	Bind string

	// HTTPPort is the OTLP/HTTP listener port. 0 disables HTTP.
	// Default: 4318 (OTel convention).
	HTTPPort int

	// GRPCPort is the OTLP/gRPC listener port. 0 disables gRPC.
	// Default: 4317 (OTel convention). Note: collides with Core's own OTLP
	// gRPC listener if both run on the same host — operator must pick a
	// different port (e.g. 14317) for single-box installs.
	GRPCPort int

	// TenantID is stamped onto every translated record before forwarding.
	// Should match the tenant the agent itself authenticates as.
	//
	// Kept on Config (not just on the Forwarder) for two reasons:
	//   1. Stats labels can include the tenant without reaching into the
	//      forwarder's internal config.
	//   2. The shim can refuse to start with an empty tenant — this
	//      surfaces operator misconfiguration before any OTLP listener
	//      goes up.
	TenantID string
}

// Shim wires the OTLP HTTP and gRPC listeners to a Forwarder. Start
// returns once listeners are bound; Stop blocks until they fully drain.
type Shim struct {
	cfg Config
	fwd Forwarder

	httpSrv *httpServer
	grpcSrv *grpcServer

	tracesIn  atomic.Uint64
	metricsIn atomic.Uint64
	logsIn    atomic.Uint64
	fwdErrs   atomic.Uint64

	startOnce sync.Once
	stopOnce  sync.Once
}

// New constructs a Shim. nil Forwarder is rejected because a shim that
// can't forward is just an OTLP black hole. The Forwarder's Kind() is
// logged at startup so operators can confirm which path their app
// instrumentation actually rides.
func New(cfg Config, fwd Forwarder) (*Shim, error) {
	if fwd == nil {
		return nil, fmt.Errorf("otlpshim: forwarder is required")
	}
	if cfg.Bind == "" {
		cfg.Bind = "127.0.0.1"
	}
	if cfg.TenantID == "" {
		cfg.TenantID = "default"
	}
	return &Shim{cfg: cfg, fwd: fwd}, nil
}

// Start binds the configured listeners. Listener errors after this call
// are logged but don't terminate the agent — the agent's host-scraper +
// AgenticUDP loop must keep running even if the OTLP shim flaps.
//
// Logs a high-visibility banner on the active forward path. We log the
// path twice on purpose: once in the banner so it's easy to spot during
// install/upgrade, and once per-listener so an operator who greps for
// "otlpshim:" sees both the banner and the bind details. Customers and
// support engineers consistently asked "which path is my shim using?"
// — the banner makes that answerable from the journal/event log without
// any additional tooling.
func (s *Shim) Start(ctx context.Context) error {
	var startErr error
	s.startOnce.Do(func() {
		log.Printf("otlpshim: ACTIVE TRANSPORT = %s (tenant=%s) — verify any time with: curl http://%s:%d/eo/transport",
			s.fwd.Kind(), s.cfg.TenantID, s.cfg.Bind, s.cfg.HTTPPort)
		if s.cfg.HTTPPort > 0 {
			httpSrv, err := startHTTP(ctx, s.cfg.Bind, s.cfg.HTTPPort, s)
			if err != nil {
				startErr = fmt.Errorf("otlpshim: http listener: %w", err)
				return
			}
			s.httpSrv = httpSrv
			log.Printf("otlpshim: OTLP/HTTP listening on %s:%d → %s",
				s.cfg.Bind, s.cfg.HTTPPort, s.fwd.Kind())
		}
		if s.cfg.GRPCPort > 0 {
			grpcSrv, err := startGRPC(ctx, s.cfg.Bind, s.cfg.GRPCPort, s)
			if err != nil {
				startErr = fmt.Errorf("otlpshim: grpc listener: %w", err)
				return
			}
			s.grpcSrv = grpcSrv
			log.Printf("otlpshim: OTLP/gRPC listening on %s:%d → %s",
				s.cfg.Bind, s.cfg.GRPCPort, s.fwd.Kind())
		}
	})
	return startErr
}

// Stop signals the listeners to drain and returns once they are done.
// Also closes the Forwarder so any path-specific resources (Path A's
// gRPC client conn, primarily) are released.
//
// Safe to call multiple times.
func (s *Shim) Stop() {
	s.stopOnce.Do(func() {
		if s.httpSrv != nil {
			s.httpSrv.stop()
		}
		if s.grpcSrv != nil {
			s.grpcSrv.stop()
		}
		if s.fwd != nil {
			if err := s.fwd.Close(); err != nil {
				log.Printf("otlpshim: forwarder close (%s): %v", s.fwd.Kind(), err)
			}
		}
	})
}

// recvTraces / recvMetrics / recvLogs are the single dispatch point
// shared by the HTTP and gRPC listeners. They count the inbound payload
// (so Stats reflects what apps sent, not what reached Core) and then
// hand off to the active Forwarder. Forwarder errors bump fwdErrs and
// are logged once per call — apps already retry on their side, so we
// don't need to surface them upstream.
func (s *Shim) recvTraces(ctx context.Context, req tracesReq) {
	if req == nil {
		return
	}
	s.tracesIn.Add(uint64(countSpans(req)))
	if err := s.fwd.ForwardTraces(ctx, req); err != nil {
		s.fwdErrs.Add(1)
		log.Printf("otlpshim: forward traces (%s): %v", s.fwd.Kind(), err)
	}
}

func (s *Shim) recvMetrics(ctx context.Context, req metricsReq) {
	if req == nil {
		return
	}
	s.metricsIn.Add(uint64(countDataPoints(req)))
	if err := s.fwd.ForwardMetrics(ctx, req); err != nil {
		s.fwdErrs.Add(1)
		log.Printf("otlpshim: forward metrics (%s): %v", s.fwd.Kind(), err)
	}
}

func (s *Shim) recvLogs(ctx context.Context, req logsReq) {
	if req == nil {
		return
	}
	s.logsIn.Add(uint64(countLogs(req)))
	if err := s.fwd.ForwardLogs(ctx, req); err != nil {
		s.fwdErrs.Add(1)
		log.Printf("otlpshim: forward logs (%s): %v", s.fwd.Kind(), err)
	}
}

// Stats returns current counters for the shim. The "transport" key
// reports which path the shim is forwarding via — handy for operators
// confirming they actually flipped the toggle they intended to flip.
func (s *Shim) Stats() map[string]interface{} {
	return map[string]interface{}{
		"transport":      string(s.fwd.Kind()),
		"traces_in":      s.tracesIn.Load(),
		"metrics_in":     s.metricsIn.Load(),
		"logs_in":        s.logsIn.Load(),
		"forward_errors": s.fwdErrs.Load(),
	}
}

// transportInfo is the small, stable shape served by the HTTP
// /eo/transport endpoint. It deliberately exposes ONLY the operator-
// facing config (which path, where the shim is bound, what tenant
// gets stamped) and not internal counters — Stats() is the place for
// counters. Kept narrow so on-call tooling, agent-watcher sidecars,
// and the deploy/instrumentation/verify-shim.* scripts can all parse
// the same shape across releases.
//
// Field names are snake_case to match the Stats() shape and the
// Core's own JSON style — keeps anyone scripting against both
// endpoints from having to remember two casing conventions.
func (s *Shim) transportInfo() map[string]interface{} {
	return map[string]interface{}{
		"transport":      string(s.fwd.Kind()),
		"tenant":         s.cfg.TenantID,
		"bind":           s.cfg.Bind,
		"http_port":      s.cfg.HTTPPort,
		"grpc_port":      s.cfg.GRPCPort,
		"otlp_http_url":  fmt.Sprintf("http://%s:%d", s.cfg.Bind, s.cfg.HTTPPort),
		"otlp_grpc_addr": fmt.Sprintf("%s:%d", s.cfg.Bind, s.cfg.GRPCPort),
	}
}
