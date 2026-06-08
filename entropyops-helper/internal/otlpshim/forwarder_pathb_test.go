package otlpshim

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/entropyops/entropyops-helper/internal/transport"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// recordingPathBClient captures everything pathBForwarder hands it. The
// Send* signatures in PathBClient take interface{} so the test can later
// type-assert back to []transport.{Trace,Metric,Log}Record (the shape
// translate.go is specified to produce).
type recordingPathBClient struct {
	mu      sync.Mutex
	traces  []interface{}
	metrics []interface{}
	logs    []interface{}
	failOn  string // "traces" | "metrics" | "logs" — return errFail
}

var errFail = errors.New("recordingPathBClient: forced failure")

func (r *recordingPathBClient) SendMetrics(data interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failOn == "metrics" {
		return errFail
	}
	r.metrics = append(r.metrics, data)
	return nil
}

func (r *recordingPathBClient) SendTraces(data interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failOn == "traces" {
		return errFail
	}
	r.traces = append(r.traces, data)
	return nil
}

func (r *recordingPathBClient) SendLogs(data interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failOn == "logs" {
		return errFail
	}
	r.logs = append(r.logs, data)
	return nil
}

func TestNewPathBForwarder_NilClientRejected(t *testing.T) {
	if _, err := NewPathBForwarder(nil, "t"); err == nil {
		t.Fatal("nil client should be rejected — pathBForwarder cannot ship without a transport.Client")
	}
}

func TestNewPathBForwarder_DefaultTenant(t *testing.T) {
	cli := &recordingPathBClient{}
	fwd, err := NewPathBForwarder(cli, "")
	if err != nil {
		t.Fatalf("NewPathBForwarder: %v", err)
	}
	if got := fwd.Kind(); got != ForwarderPathB {
		t.Errorf("Kind: want %q, got %q", ForwarderPathB, got)
	}
	// Empty tenant should normalise to "default" so downstream Core's
	// authorizeContext doesn't reject the cycle for missing x-tenant.
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "svc"}}},
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{Name: "op", TraceId: make([]byte, 16), SpanId: make([]byte, 8)}},
			}},
		}},
	}
	if err := fwd.ForwardTraces(context.Background(), req); err != nil {
		t.Fatalf("ForwardTraces: %v", err)
	}
	cli.mu.Lock()
	defer cli.mu.Unlock()
	if len(cli.traces) != 1 {
		t.Fatalf("traces sent: want 1, got %d", len(cli.traces))
	}
	recs, ok := cli.traces[0].([]transport.TraceRecord)
	if !ok {
		t.Fatalf("traces payload type: want []transport.TraceRecord, got %T", cli.traces[0])
	}
	if recs[0].TenantID != "default" {
		t.Errorf("tenant default: want 'default', got %q", recs[0].TenantID)
	}
}

func TestPathBForwarder_TranslatesAndForwards(t *testing.T) {
	cli := &recordingPathBClient{}
	fwd, err := NewPathBForwarder(cli, "tenant-z")
	if err != nil {
		t.Fatalf("NewPathBForwarder: %v", err)
	}

	traceReq := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "checkout"}}},
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					TraceId: []byte{0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef},
					SpanId:  []byte{0xab, 0xcd, 0xab, 0xcd, 0xab, 0xcd, 0xab, 0xcd},
					Name:    "POST /pay",
					Kind:    tracepb.Span_SPAN_KIND_SERVER,
				}},
			}},
		}},
	}
	if err := fwd.ForwardTraces(context.Background(), traceReq); err != nil {
		t.Fatalf("ForwardTraces: %v", err)
	}

	metricReq := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "checkout"}}},
			}},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "http.request.count",
					Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						IsMonotonic: true,
						DataPoints: []*metricspb.NumberDataPoint{{
							Value: &metricspb.NumberDataPoint_AsInt{AsInt: 7},
						}},
					}},
				}},
			}},
		}},
	}
	if err := fwd.ForwardMetrics(context.Background(), metricReq); err != nil {
		t.Fatalf("ForwardMetrics: %v", err)
	}

	logReq := &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "checkout"}}},
			}},
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}},
				}},
			}},
		}},
	}
	if err := fwd.ForwardLogs(context.Background(), logReq); err != nil {
		t.Fatalf("ForwardLogs: %v", err)
	}

	cli.mu.Lock()
	defer cli.mu.Unlock()
	if len(cli.traces) != 1 || len(cli.metrics) != 1 || len(cli.logs) != 1 {
		t.Fatalf("each Send* should be called exactly once: traces=%d metrics=%d logs=%d",
			len(cli.traces), len(cli.metrics), len(cli.logs))
	}

	// Trace shape: tenant stamp + service name carried.
	traces, ok := cli.traces[0].([]transport.TraceRecord)
	if !ok {
		t.Fatalf("traces payload: want []transport.TraceRecord, got %T", cli.traces[0])
	}
	if traces[0].TenantID != "tenant-z" || traces[0].ServiceName != "checkout" {
		t.Errorf("trace tenant/service: %+v", traces[0])
	}

	// Metric shape: counter mapping preserved.
	metrics, ok := cli.metrics[0].([]transport.MetricRecord)
	if !ok {
		t.Fatalf("metrics payload: want []transport.MetricRecord, got %T", cli.metrics[0])
	}
	if metrics[0].MetricType != "counter" || metrics[0].Value != 7 {
		t.Errorf("metric mapping: %+v", metrics[0])
	}

	// Log shape: body preserved.
	logs, ok := cli.logs[0].([]transport.LogRecord)
	if !ok {
		t.Fatalf("logs payload: want []transport.LogRecord, got %T", cli.logs[0])
	}
	if logs[0].Body != "hello" {
		t.Errorf("log body: %q", logs[0].Body)
	}
}

func TestPathBForwarder_EmptyRequestsAreSkipped(t *testing.T) {
	cli := &recordingPathBClient{}
	fwd, err := NewPathBForwarder(cli, "tenant")
	if err != nil {
		t.Fatalf("NewPathBForwarder: %v", err)
	}

	if err := fwd.ForwardTraces(context.Background(), &collectortrace.ExportTraceServiceRequest{}); err != nil {
		t.Fatalf("empty traces: %v", err)
	}
	if err := fwd.ForwardMetrics(context.Background(), &collectormetrics.ExportMetricsServiceRequest{}); err != nil {
		t.Fatalf("empty metrics: %v", err)
	}
	if err := fwd.ForwardLogs(context.Background(), &collectorlogs.ExportLogsServiceRequest{}); err != nil {
		t.Fatalf("empty logs: %v", err)
	}

	cli.mu.Lock()
	defer cli.mu.Unlock()
	// No translated records → no Send*; verifies we don't churn the
	// AgenticUDP client with empty payloads (which would still cost a
	// JSON marshal + cycle header build per cycle).
	if len(cli.traces)+len(cli.metrics)+len(cli.logs) != 0 {
		t.Errorf("empty requests should not call Send*: traces=%d metrics=%d logs=%d",
			len(cli.traces), len(cli.metrics), len(cli.logs))
	}
}

func TestPathBForwarder_PropagatesClientErrors(t *testing.T) {
	cli := &recordingPathBClient{failOn: "traces"}
	fwd, err := NewPathBForwarder(cli, "tenant")
	if err != nil {
		t.Fatalf("NewPathBForwarder: %v", err)
	}
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{Name: "op", TraceId: make([]byte, 16), SpanId: make([]byte, 8)}},
			}},
		}},
	}
	err = fwd.ForwardTraces(context.Background(), req)
	if !errors.Is(err, errFail) {
		t.Errorf("client error should bubble: got %v", err)
	}
}

// TestPathBForwarder_CloseIsNoOp protects the documented invariant that
// pathBForwarder.Close() does NOT tear down the transport.Client (which
// is owned by the agent main and shared with the host-scraper).
func TestPathBForwarder_CloseIsNoOp(t *testing.T) {
	cli := &recordingPathBClient{}
	fwd, _ := NewPathBForwarder(cli, "t")
	if err := fwd.Close(); err != nil {
		t.Errorf("Close should be no-op: %v", err)
	}
	if err := fwd.Close(); err != nil {
		t.Errorf("Close should be idempotent: %v", err)
	}
	// Forwarder is still usable after Close — the client was never closed.
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{Name: "op", TraceId: make([]byte, 16), SpanId: make([]byte, 8)}},
			}},
		}},
	}
	if err := fwd.ForwardTraces(context.Background(), req); err != nil {
		t.Errorf("forwarder should still work after Close: %v", err)
	}
}

// Compile-time check that the exact transport types are what translate.go
// produces. If translate.go ever changes its return type, this test
// breaks at the type assertion above AND this guard breaks at compile.
var _ = func() {
	var _ []transport.TraceRecord = TranslateTraces(nil, "")
	var _ []transport.MetricRecord = TranslateMetrics(nil, "")
	var _ []transport.LogRecord = TranslateLogs(nil, "")
}

// reflectKindCheck guards against accidental reordering of the
// ForwarderKind enum (a pathA value sneaking into a pathB slot would be
// silent at runtime but visible here).
func TestForwarderKindEnumStable(t *testing.T) {
	if reflect.TypeOf(ForwarderPathA).Kind() != reflect.String {
		t.Fatal("ForwarderKind must remain a string-typed enum")
	}
	if ForwarderPathA == ForwarderPathB || ForwarderPathB == ForwarderPathC {
		t.Errorf("kind values must be distinct: %q %q %q",
			ForwarderPathA, ForwarderPathB, ForwarderPathC)
	}
}
