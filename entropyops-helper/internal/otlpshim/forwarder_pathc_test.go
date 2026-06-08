package otlpshim

import (
	"context"
	"errors"
	"sync"
	"testing"

	sppv1 "github.com/entropyops/entropyops-v2/pkg/sppv1"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// recordingPathCClient captures SendCycleProto calls. Path C's wire model
// is one signal-type-per-cycle (because the shim's listener delivers one
// signal type at a time), so each test cycle has exactly one of the three
// batches non-nil.
type recordingPathCClient struct {
	mu      sync.Mutex
	cycles  []pathCCycle
	failErr error
}

type pathCCycle struct {
	metrics *sppv1.MetricBatch
	traces  *sppv1.TraceBatch
	logs    *sppv1.LogBatch
}

func (r *recordingPathCClient) SendCycleProto(metrics *sppv1.MetricBatch, traces *sppv1.TraceBatch, logs *sppv1.LogBatch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failErr != nil {
		return r.failErr
	}
	r.cycles = append(r.cycles, pathCCycle{metrics: metrics, traces: traces, logs: logs})
	return nil
}

func TestNewPathCForwarder_NilClientRejected(t *testing.T) {
	if _, err := NewPathCForwarder(nil, "t"); err == nil {
		t.Fatal("nil client must be rejected")
	}
}

func TestPathCForwarder_TracesGoIntoTraceBatchOnly(t *testing.T) {
	cli := &recordingPathCClient{}
	fwd, err := NewPathCForwarder(cli, "tenant-c")
	if err != nil {
		t.Fatalf("NewPathCForwarder: %v", err)
	}
	if got := fwd.Kind(); got != ForwarderPathC {
		t.Errorf("Kind: want %q, got %q", ForwarderPathC, got)
	}

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "svc"}}},
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					Name:    "op",
					TraceId: make([]byte, 16),
					SpanId:  make([]byte, 8),
				}},
			}},
		}},
	}
	if err := fwd.ForwardTraces(context.Background(), req); err != nil {
		t.Fatalf("ForwardTraces: %v", err)
	}

	cli.mu.Lock()
	defer cli.mu.Unlock()
	if len(cli.cycles) != 1 {
		t.Fatalf("want 1 cycle, got %d", len(cli.cycles))
	}
	c := cli.cycles[0]
	if c.metrics != nil || c.logs != nil {
		t.Errorf("traces forward should send only TraceBatch (metrics=%v logs=%v)", c.metrics, c.logs)
	}
	if c.traces == nil || len(c.traces.Spans) != 1 {
		t.Fatalf("TraceBatch shape: %+v", c.traces)
	}
	if c.traces.Spans[0].TenantId != "tenant-c" || c.traces.Spans[0].ServiceName != "svc" {
		t.Errorf("Span tenant/service: %+v", c.traces.Spans[0])
	}
}

func TestPathCForwarder_MetricsGoIntoMetricBatchOnly(t *testing.T) {
	cli := &recordingPathCClient{}
	fwd, _ := NewPathCForwarder(cli, "tenant")
	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "api"}}},
			}},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "go.goroutines",
					Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
						DataPoints: []*metricspb.NumberDataPoint{{
							Value: &metricspb.NumberDataPoint_AsInt{AsInt: 42},
						}},
					}},
				}},
			}},
		}},
	}
	if err := fwd.ForwardMetrics(context.Background(), req); err != nil {
		t.Fatalf("ForwardMetrics: %v", err)
	}
	cli.mu.Lock()
	defer cli.mu.Unlock()
	if len(cli.cycles) != 1 {
		t.Fatalf("cycles: %d", len(cli.cycles))
	}
	c := cli.cycles[0]
	if c.traces != nil || c.logs != nil {
		t.Errorf("metrics forward should send only MetricBatch")
	}
	if c.metrics == nil || len(c.metrics.Metrics) != 1 {
		t.Fatalf("MetricBatch shape: %+v", c.metrics)
	}
	if c.metrics.Metrics[0].MetricName != "go.goroutines" || c.metrics.Metrics[0].Value != 42 {
		t.Errorf("metric mapping: %+v", c.metrics.Metrics[0])
	}
}

func TestPathCForwarder_LogsGoIntoLogBatchOnly(t *testing.T) {
	cli := &recordingPathCClient{}
	fwd, _ := NewPathCForwarder(cli, "tenant")
	req := &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "worker"}}},
			}},
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					SeverityText: "ERROR",
					Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "boom"}},
				}},
			}},
		}},
	}
	if err := fwd.ForwardLogs(context.Background(), req); err != nil {
		t.Fatalf("ForwardLogs: %v", err)
	}
	cli.mu.Lock()
	defer cli.mu.Unlock()
	if len(cli.cycles) != 1 {
		t.Fatalf("cycles: %d", len(cli.cycles))
	}
	c := cli.cycles[0]
	if c.metrics != nil || c.traces != nil {
		t.Errorf("logs forward should send only LogBatch")
	}
	if c.logs == nil || len(c.logs.Logs) != 1 {
		t.Fatalf("LogBatch shape: %+v", c.logs)
	}
	if c.logs.Logs[0].Body != "boom" || c.logs.Logs[0].SeverityText != "ERROR" {
		t.Errorf("log mapping: %+v", c.logs.Logs[0])
	}
}

func TestPathCForwarder_EmptyRequestsDoNotCallSendCycle(t *testing.T) {
	cli := &recordingPathCClient{}
	fwd, _ := NewPathCForwarder(cli, "tenant")

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
	if len(cli.cycles) != 0 {
		t.Errorf("empty requests should skip SendCycleProto entirely (got %d cycles)", len(cli.cycles))
	}
}

func TestPathCForwarder_PropagatesClientErrors(t *testing.T) {
	cli := &recordingPathCClient{failErr: errors.New("udp wedged")}
	fwd, _ := NewPathCForwarder(cli, "tenant")
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{Name: "op", TraceId: make([]byte, 16), SpanId: make([]byte, 8)}},
			}},
		}},
	}
	if err := fwd.ForwardTraces(context.Background(), req); err == nil {
		t.Error("client error should propagate so the shim can bump fwdErrs")
	}
}

func TestPathCForwarder_CloseIsNoOp(t *testing.T) {
	cli := &recordingPathCClient{}
	fwd, _ := NewPathCForwarder(cli, "t")
	if err := fwd.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
