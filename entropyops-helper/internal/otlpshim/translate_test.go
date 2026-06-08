package otlpshim

import (
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func intAttr(k string, v int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}}}
}

func TestTranslateTraces_BasicSpanIntoTraceRecord(t *testing.T) {
	start := time.Now().Add(-1 * time.Second)
	end := start.Add(250 * time.Millisecond)

	rss := []*tracepb.ResourceSpans{{
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
	}}

	got := TranslateTraces(rss, "tenant-a")
	if len(got) != 1 {
		t.Fatalf("want 1 record, got %d", len(got))
	}
	r := got[0]
	if r.ServiceName != "checkout" {
		t.Errorf("ServiceName: want 'checkout', got %q", r.ServiceName)
	}
	if r.OperationName != "POST /checkout" {
		t.Errorf("OperationName: want 'POST /checkout', got %q", r.OperationName)
	}
	if r.SpanKind != "SERVER" {
		t.Errorf("SpanKind: want 'SERVER', got %q", r.SpanKind)
	}
	if r.StatusCode != "OK" {
		t.Errorf("StatusCode: want 'OK', got %q", r.StatusCode)
	}
	if r.TraceID != "0102030405060708090a0b0c0d0e0f10" {
		t.Errorf("TraceID hex: want full 16-byte hex, got %q", r.TraceID)
	}
	if r.SpanID != "aabbccddeeff0011" {
		t.Errorf("SpanID hex: got %q", r.SpanID)
	}
	if r.ParentSpanID != "2233445566778899" {
		t.Errorf("ParentSpanID hex: got %q", r.ParentSpanID)
	}
	if r.TenantID != "tenant-a" {
		t.Errorf("TenantID: got %q", r.TenantID)
	}
	if r.DurationUS != 250000 {
		t.Errorf("DurationUS: want 250000, got %d", r.DurationUS)
	}
	if r.Attributes["service.name"] != "checkout" {
		t.Errorf("resource attrs not merged into Attributes")
	}
	if r.Attributes["http.status_code"] != "200" {
		t.Errorf("span attrs missing or not stringified: got %q", r.Attributes["http.status_code"])
	}
	if r.Attributes["otel.scope.name"] != "checkout/orders" {
		t.Errorf("scope name not stamped: got %q", r.Attributes["otel.scope.name"])
	}
}

func TestTranslateMetrics_GaugeSumHistogram(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rms := []*metricspb.ResourceMetrics{{
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
	}}

	got := TranslateMetrics(rms, "t1")
	if len(got) != 4 {
		t.Fatalf("want 4 records (1 gauge + 1 counter + 2 histogram), got %d: %+v", len(got), got)
	}
	if got[0].MetricName != "process.runtime.go.goroutines" || got[0].MetricType != "gauge" || got[0].Value != 42 {
		t.Errorf("gauge wrong: %+v", got[0])
	}
	if got[1].MetricName != "http.server.request.count" || got[1].MetricType != "counter" || got[1].Value != 100 {
		t.Errorf("counter wrong: %+v", got[1])
	}
	if got[2].MetricName != "http.server.request.duration_count" || got[2].Value != 25 {
		t.Errorf("histogram count wrong: %+v", got[2])
	}
	if got[3].MetricName != "http.server.request.duration_sum" || got[3].Value != 12.5 {
		t.Errorf("histogram sum wrong: %+v", got[3])
	}
}

func TestTranslateLogs_BasicLogIntoLogRecord(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	rls := []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			strAttr("service.name", "worker"),
		}},
		ScopeLogs: []*logspb.ScopeLogs{{
			LogRecords: []*logspb.LogRecord{{
				TimeUnixNano:   now,
				SeverityText:   "ERROR",
				SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_ERROR,
				Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "something broke"}},
				TraceId:        []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
				SpanId:         []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11},
				Attributes:     []*commonpb.KeyValue{strAttr("error.type", "TimeoutError")},
			}},
		}},
	}}

	got := TranslateLogs(rls, "t2")
	if len(got) != 1 {
		t.Fatalf("want 1 record, got %d", len(got))
	}
	r := got[0]
	if r.ServiceName != "worker" {
		t.Errorf("ServiceName: %q", r.ServiceName)
	}
	if r.SeverityText != "ERROR" {
		t.Errorf("SeverityText: %q", r.SeverityText)
	}
	if r.Body != "something broke" {
		t.Errorf("Body: %q", r.Body)
	}
	if r.TraceID == "" || r.SpanID == "" {
		t.Errorf("trace/span correlation missing: %+v", r)
	}
	if r.Attributes["error.type"] != "TimeoutError" {
		t.Errorf("attributes lost: %+v", r.Attributes)
	}
}

func TestTranslateTraces_EmptyResource(t *testing.T) {
	if got := TranslateTraces(nil, "t"); got != nil {
		t.Errorf("nil input should yield nil, got %+v", got)
	}
	if got := TranslateTraces([]*tracepb.ResourceSpans{}, "t"); got != nil {
		t.Errorf("empty input should yield nil, got %+v", got)
	}
}

func TestResourceServiceName_Fallback(t *testing.T) {
	if got := resourceServiceName(nil); got != "unknown-service" {
		t.Errorf("nil resource: %q", got)
	}
	res := &resourcepb.Resource{}
	if got := resourceServiceName(res); got != "unknown-service" {
		t.Errorf("empty resource: %q", got)
	}
}
