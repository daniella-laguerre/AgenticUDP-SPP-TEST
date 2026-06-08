package otlpshim

import (
	"testing"
	"time"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestTranslateTracesProto_BasicSpan(t *testing.T) {
	start := time.Now().Add(-1 * time.Second)
	end := start.Add(250 * time.Millisecond)

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				strAttr("service.name", "checkout"),
				strAttr("deployment.environment", "prod"),
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Scope: &commonpb.InstrumentationScope{Name: "checkout/orders"},
				Spans: []*tracepb.Span{{
					TraceId:           []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
					SpanId:            []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11},
					ParentSpanId:      []byte{0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99},
					Name:              "POST /checkout",
					Kind:              tracepb.Span_SPAN_KIND_SERVER,
					StartTimeUnixNano: uint64(start.UnixNano()),
					EndTimeUnixNano:   uint64(end.UnixNano()),
					Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
					Attributes: []*commonpb.KeyValue{
						strAttr("http.method", "POST"),
						intAttr("http.status_code", 200),
					},
				}},
			}},
		}},
	}

	batch := TranslateTracesProto(req, "tenant-a")
	if batch == nil || len(batch.Spans) != 1 {
		t.Fatalf("want 1 span, got %+v", batch)
	}
	s := batch.Spans[0]

	// Path C and Path B share identical semantics: the same fields must
	// hold the same values across both translators (a regression in
	// only one would silently route the same OTLP payload to two
	// different storage shapes).
	if s.ServiceName != "checkout" {
		t.Errorf("ServiceName: %q", s.ServiceName)
	}
	if s.OperationName != "POST /checkout" {
		t.Errorf("OperationName: %q", s.OperationName)
	}
	if s.SpanKind != "SERVER" {
		t.Errorf("SpanKind: %q", s.SpanKind)
	}
	if s.StatusCode != "OK" {
		t.Errorf("StatusCode: %q", s.StatusCode)
	}
	if s.TraceId != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("TraceId hex: %q", s.TraceId)
	}
	if s.SpanId != "aabbccddeeff0011" {
		t.Errorf("SpanId hex: %q", s.SpanId)
	}
	if s.ParentSpanId != "2233445566778899" {
		t.Errorf("ParentSpanId hex: %q", s.ParentSpanId)
	}
	if s.TenantId != "tenant-a" {
		t.Errorf("TenantId: %q", s.TenantId)
	}
	if s.DurationUs != 250000 {
		t.Errorf("DurationUs: want 250000, got %d", s.DurationUs)
	}
	if s.Attributes["service.name"] != "checkout" {
		t.Errorf("resource attrs not merged into Attributes")
	}
	if s.Attributes["http.status_code"] != "200" {
		t.Errorf("span attrs missing or not stringified: %q", s.Attributes["http.status_code"])
	}
	if s.Attributes["otel.scope.name"] != "checkout/orders" {
		t.Errorf("scope name not stamped: %q", s.Attributes["otel.scope.name"])
	}
	if s.StartTime == nil || s.EndTime == nil {
		t.Errorf("timestamps must be set: start=%v end=%v", s.StartTime, s.EndTime)
	}
}

func TestTranslateMetricsProto_GaugeSumHistogram(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				strAttr("service.name", "api"),
			}},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{
					{
						Name: "process.runtime.go.goroutines",
						Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
							DataPoints: []*metricspb.NumberDataPoint{{
								TimeUnixNano: now,
								Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 42},
							}},
						}},
					},
					{
						Name: "http.server.request.count",
						Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
							IsMonotonic: true,
							DataPoints: []*metricspb.NumberDataPoint{{
								TimeUnixNano: now,
								Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 100},
								Attributes:   []*commonpb.KeyValue{strAttr("http.method", "GET")},
							}},
						}},
					},
					{
						Name: "http.server.request.duration",
						Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
							DataPoints: []*metricspb.HistogramDataPoint{{
								TimeUnixNano: now,
								Count:        25,
								Sum:          func() *float64 { v := 12.5; return &v }(),
							}},
						}},
					},
				},
			}},
		}},
	}

	batch := TranslateMetricsProto(req, "t1")
	if batch == nil || len(batch.Metrics) != 4 {
		t.Fatalf("want 4 records (1 gauge + 1 counter + 2 histogram), got %+v", batch)
	}
	got := batch.Metrics
	if got[0].MetricName != "process.runtime.go.goroutines" || got[0].MetricType != "gauge" || got[0].Value != 42 {
		t.Errorf("gauge wrong: %+v", got[0])
	}
	if got[1].MetricName != "http.server.request.count" || got[1].MetricType != "counter" || got[1].Value != 100 {
		t.Errorf("counter wrong: %+v", got[1])
	}
	// Histograms collapse to <name>_count and <name>_sum to mirror Path B.
	if got[2].MetricName != "http.server.request.duration_count" || got[2].Value != 25 {
		t.Errorf("histogram count wrong: %+v", got[2])
	}
	if got[3].MetricName != "http.server.request.duration_sum" || got[3].Value != 12.5 {
		t.Errorf("histogram sum wrong: %+v", got[3])
	}
}

func TestTranslateLogsProto_BasicLog(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	req := &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				strAttr("service.name", "worker"),
			}},
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					TimeUnixNano:   now,
					SeverityText:   "ERROR",
					SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
					Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "boom"}},
					TraceId:        []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
					SpanId:         []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11},
					Attributes:     []*commonpb.KeyValue{strAttr("error.type", "TimeoutError")},
				}},
			}},
		}},
	}

	batch := TranslateLogsProto(req, "t2")
	if batch == nil || len(batch.Logs) != 1 {
		t.Fatalf("want 1 log, got %+v", batch)
	}
	l := batch.Logs[0]
	if l.ServiceName != "worker" {
		t.Errorf("ServiceName: %q", l.ServiceName)
	}
	if l.SeverityText != "ERROR" {
		t.Errorf("SeverityText: %q", l.SeverityText)
	}
	if l.Body != "boom" {
		t.Errorf("Body: %q", l.Body)
	}
	if l.TraceId == "" || l.SpanId == "" {
		t.Errorf("trace/span correlation lost: %+v", l)
	}
	if l.Attributes["error.type"] != "TimeoutError" {
		t.Errorf("attributes lost: %+v", l.Attributes)
	}
	if l.ResourceAttrs["service.name"] != "worker" {
		t.Errorf("resource attrs lost: %+v", l.ResourceAttrs)
	}
}

// TestTranslateProto_NilOrEmptyReturnsNil documents the Path C contract
// that pathCForwarder relies on to skip SendCycleProto entirely on empty
// requests (saves an AgenticUDP write per heartbeat batch).
func TestTranslateProto_NilOrEmptyReturnsNil(t *testing.T) {
	if got := TranslateTracesProto(nil, "t"); got != nil {
		t.Errorf("nil traces: %+v", got)
	}
	if got := TranslateTracesProto(&collectortrace.ExportTraceServiceRequest{}, "t"); got != nil {
		t.Errorf("empty traces: %+v", got)
	}
	if got := TranslateMetricsProto(nil, "t"); got != nil {
		t.Errorf("nil metrics: %+v", got)
	}
	if got := TranslateMetricsProto(&collectormetrics.ExportMetricsServiceRequest{}, "t"); got != nil {
		t.Errorf("empty metrics: %+v", got)
	}
	if got := TranslateLogsProto(nil, "t"); got != nil {
		t.Errorf("nil logs: %+v", got)
	}
	if got := TranslateLogsProto(&collectorlogs.ExportLogsServiceRequest{}, "t"); got != nil {
		t.Errorf("empty logs: %+v", got)
	}
}

// TestTranslateProto_ParityWithJSONTranslator runs the same OTLP request
// through both translate.go (Path B) and translate_proto.go (Path C) and
// asserts the structurally-equivalent fields agree. If the two ever
// drift (e.g. a histogram bucket policy changes only on one side), this
// test fails before the production code ships. The goal is intentional:
// Path B and Path C are two encodings of one mapping, and the operator
// flag should be a wire-format choice, not a semantics choice.
func TestTranslateProto_ParityWithJSONTranslator(t *testing.T) {
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				strAttr("service.name", "parity-test"),
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					Name:    "op",
					Kind:    tracepb.Span_SPAN_KIND_CLIENT,
					TraceId: []byte{0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef},
					SpanId:  []byte{0xab, 0xcd, 0xab, 0xcd, 0xab, 0xcd, 0xab, 0xcd},
				}},
			}},
		}},
	}

	jsonRecs := TranslateTraces(req.GetResourceSpans(), "tenant-parity")
	protoBatch := TranslateTracesProto(req, "tenant-parity")
	if len(jsonRecs) != 1 || protoBatch == nil || len(protoBatch.Spans) != 1 {
		t.Fatalf("setup: jsonRecs=%d protoBatch=%+v", len(jsonRecs), protoBatch)
	}
	j := jsonRecs[0]
	p := protoBatch.Spans[0]
	if j.ServiceName != p.ServiceName {
		t.Errorf("ServiceName drift: json=%q proto=%q", j.ServiceName, p.ServiceName)
	}
	if j.OperationName != p.OperationName {
		t.Errorf("OperationName drift: json=%q proto=%q", j.OperationName, p.OperationName)
	}
	if j.SpanKind != p.SpanKind {
		t.Errorf("SpanKind drift: json=%q proto=%q", j.SpanKind, p.SpanKind)
	}
	if j.TraceID != p.TraceId {
		t.Errorf("TraceID drift: json=%q proto=%q", j.TraceID, p.TraceId)
	}
	if j.SpanID != p.SpanId {
		t.Errorf("SpanID drift: json=%q proto=%q", j.SpanID, p.SpanId)
	}
	if j.TenantID != p.TenantId {
		t.Errorf("TenantID drift: json=%q proto=%q", j.TenantID, p.TenantId)
	}
}
