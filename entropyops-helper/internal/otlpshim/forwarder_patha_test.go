package otlpshim

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// captureCoreServer records every Export call's request + gRPC metadata
// so tests can assert that pathAForwarder injected x-tenant / x-api-key /
// x-agent-token correctly. Three small *Shim structs mount each OTLP
// service onto the same shared store — embedding all three Unimplemented
// servers on one struct produces ambiguous Export methods.
type captureCoreServer struct {
	mu       sync.Mutex
	gotMD    metadata.MD
	traces   []*collectortrace.ExportTraceServiceRequest
	metrics  []*collectormetrics.ExportMetricsServiceRequest
	logs     []*collectorlogs.ExportLogsServiceRequest
	gotCalls int
}

type traceExportShim struct {
	collectortrace.UnimplementedTraceServiceServer
	parent *captureCoreServer
}

func (s *traceExportShim) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	s.parent.mu.Lock()
	defer s.parent.mu.Unlock()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		s.parent.gotMD = md
	}
	s.parent.traces = append(s.parent.traces, req)
	s.parent.gotCalls++
	return &collectortrace.ExportTraceServiceResponse{}, nil
}

type metricsExportShim struct {
	collectormetrics.UnimplementedMetricsServiceServer
	parent *captureCoreServer
}

func (m *metricsExportShim) Export(ctx context.Context, req *collectormetrics.ExportMetricsServiceRequest) (*collectormetrics.ExportMetricsServiceResponse, error) {
	m.parent.mu.Lock()
	defer m.parent.mu.Unlock()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		m.parent.gotMD = md
	}
	m.parent.metrics = append(m.parent.metrics, req)
	m.parent.gotCalls++
	return &collectormetrics.ExportMetricsServiceResponse{}, nil
}

type logsExportShim struct {
	collectorlogs.UnimplementedLogsServiceServer
	parent *captureCoreServer
}

func (l *logsExportShim) Export(ctx context.Context, req *collectorlogs.ExportLogsServiceRequest) (*collectorlogs.ExportLogsServiceResponse, error) {
	l.parent.mu.Lock()
	defer l.parent.mu.Unlock()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		l.parent.gotMD = md
	}
	l.parent.logs = append(l.parent.logs, req)
	l.parent.gotCalls++
	return &collectorlogs.ExportLogsServiceResponse{}, nil
}

// startFakeCore brings up a real gRPC server on an ephemeral loopback
// port and registers the three OTLP services. Real TCP — not bufconn —
// because the production constructor uses grpc.DialContext with no
// custom dialer hook; this keeps the test exercising the real dial path
// (resolution, TCP connect, HTTP/2 handshake).
func startFakeCore(t *testing.T) (string, *captureCoreServer, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	core := &captureCoreServer{}
	srv := grpc.NewServer()
	collectortrace.RegisterTraceServiceServer(srv, &traceExportShim{parent: core})
	collectormetrics.RegisterMetricsServiceServer(srv, &metricsExportShim{parent: core})
	collectorlogs.RegisterLogsServiceServer(srv, &logsExportShim{parent: core})
	go func() { _ = srv.Serve(lis) }()
	return lis.Addr().String(), core, func() {
		srv.GracefulStop()
		_ = lis.Close()
	}
}

func sampleLogReq(service string) *collectorlogs.ExportLogsServiceRequest {
	return &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: service}}},
			}},
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					SeverityText: "INFO",
					Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "msg"}},
				}},
			}},
		}},
	}
}

func TestNewPathAForwarder_RejectsEmptyTarget(t *testing.T) {
	if _, err := NewPathAForwarder(PathAConfig{}); err == nil {
		t.Fatal("Target is required (Core's gRPC endpoint); empty must be rejected")
	}
}

func TestPathAForwarder_TracesPassthroughWithMetadata(t *testing.T) {
	target, core, stop := startFakeCore(t)
	defer stop()

	fwd, err := NewPathAForwarder(PathAConfig{
		Target:      target,
		TenantID:    "tenant-pathA",
		APIKey:      "secret-key",
		AgentToken:  "agent-token-xyz",
		TLS:         false,
		DialTimeout: 2 * time.Second,
		CallTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewPathAForwarder: %v", err)
	}
	defer fwd.Close()
	if got := fwd.Kind(); got != ForwarderPathA {
		t.Errorf("Kind: want %q, got %q", ForwarderPathA, got)
	}

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "checkout"}}},
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					Name:    "POST /pay",
					TraceId: make([]byte, 16),
					SpanId:  make([]byte, 8),
				}},
			}},
		}},
	}
	if err := fwd.ForwardTraces(context.Background(), req); err != nil {
		t.Fatalf("ForwardTraces: %v", err)
	}

	core.mu.Lock()
	defer core.mu.Unlock()
	if core.gotCalls != 1 {
		t.Fatalf("Core Export calls: want 1, got %d", core.gotCalls)
	}
	if len(core.traces) != 1 || len(core.traces[0].GetResourceSpans()) != 1 {
		t.Fatalf("Core saw wrong shape: %+v", core.traces)
	}

	// Critical assertion: Path A is a passthrough — translation lives
	// in B and C only — so the inbound span name must round-trip exactly.
	gotSpan := core.traces[0].GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()[0]
	if gotSpan.GetName() != "POST /pay" {
		t.Errorf("span name: got %q", gotSpan.GetName())
	}

	if got := core.gotMD.Get("x-tenant"); len(got) != 1 || got[0] != "tenant-pathA" {
		t.Errorf("x-tenant header: %v", got)
	}
	if got := core.gotMD.Get("x-api-key"); len(got) != 1 || got[0] != "secret-key" {
		t.Errorf("x-api-key header: %v", got)
	}
	if got := core.gotMD.Get("x-agent-token"); len(got) != 1 || got[0] != "agent-token-xyz" {
		t.Errorf("x-agent-token header: %v", got)
	}
}

func TestPathAForwarder_OmitsAuthHeadersWhenEmpty(t *testing.T) {
	target, core, stop := startFakeCore(t)
	defer stop()

	fwd, err := NewPathAForwarder(PathAConfig{
		Target:   target,
		TenantID: "tenant-only",
	})
	if err != nil {
		t.Fatalf("NewPathAForwarder: %v", err)
	}
	defer fwd.Close()

	if err := fwd.ForwardLogs(context.Background(), sampleLogReq("svc")); err != nil {
		t.Fatalf("ForwardLogs: %v", err)
	}

	core.mu.Lock()
	defer core.mu.Unlock()
	if got := core.gotMD.Get("x-tenant"); len(got) != 1 || got[0] != "tenant-only" {
		t.Errorf("x-tenant should still be sent: %v", got)
	}
	// Empty auth fields MUST be omitted (not sent as ""). Sending an
	// empty x-api-key would change Core's authorizeContext behaviour
	// from "no key supplied" to "key supplied but empty" — different
	// failure mode, harder to debug.
	if got := core.gotMD.Get("x-api-key"); len(got) != 0 {
		t.Errorf("x-api-key MUST be omitted when empty (got %v)", got)
	}
	if got := core.gotMD.Get("x-agent-token"); len(got) != 0 {
		t.Errorf("x-agent-token MUST be omitted when empty (got %v)", got)
	}
}

func TestPathAForwarder_EmptyRequestsAreSkipped(t *testing.T) {
	target, core, stop := startFakeCore(t)
	defer stop()

	fwd, err := NewPathAForwarder(PathAConfig{Target: target, TenantID: "t"})
	if err != nil {
		t.Fatalf("NewPathAForwarder: %v", err)
	}
	defer fwd.Close()

	if err := fwd.ForwardTraces(context.Background(), &collectortrace.ExportTraceServiceRequest{}); err != nil {
		t.Fatalf("empty traces: %v", err)
	}
	if err := fwd.ForwardMetrics(context.Background(), &collectormetrics.ExportMetricsServiceRequest{}); err != nil {
		t.Fatalf("empty metrics: %v", err)
	}
	if err := fwd.ForwardLogs(context.Background(), &collectorlogs.ExportLogsServiceRequest{}); err != nil {
		t.Fatalf("empty logs: %v", err)
	}

	core.mu.Lock()
	defer core.mu.Unlock()
	// Skipping the RPC entirely on empty requests saves a network
	// roundtrip per cycle when the listener happens to decode a
	// just-flushed empty batch.
	if core.gotCalls != 0 {
		t.Errorf("empty requests should skip RPC entirely (Core got %d)", core.gotCalls)
	}
}

func TestPathAForwarder_CloseIdempotent(t *testing.T) {
	target, _, stop := startFakeCore(t)
	defer stop()

	fwd, err := NewPathAForwarder(PathAConfig{Target: target, TenantID: "t"})
	if err != nil {
		t.Fatalf("NewPathAForwarder: %v", err)
	}
	if err := fwd.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	// Second close on an already-closed grpc.ClientConn returns
	// ErrClientConnClosing; we don't assert it because behaviour can
	// shift across grpc-go versions and the agent main only calls
	// Close once during shutdown.
	_ = fwd.Close()
}
