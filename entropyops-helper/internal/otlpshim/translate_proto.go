package otlpshim

import (
	"time"

	sppv1 "github.com/entropyops/entropyops-v2/pkg/sppv1"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// translate_proto.go is the Path C counterpart to translate.go. Where
// translate.go produces transport.{Trace,Metric,Log}Record (Go structs
// with JSON tags that mirror Core's storage.* types — used by the JSON
// envelope), this file produces sppv1.{Span,Metric,LogRecord} batched
// inside the matching *Batch type. Both translators share the exact same
// semantics — same fields, same conventions, same docstring caveats —
// because they're two encodings of one mapping. The differences live
// entirely in the wire format the *Batch then rides.
//
// The choice between the two paths is operational, not semantic:
//
//	Path B (JSON envelope)   — bigger on the wire, easier to debug, no DTLS
//	Path C (sppv1 protobuf)  — smaller on the wire, supports DTLS, harder to peek
//
// Either way, what hits Core is interpreted by the same routeToQueue
// (entropyops-v2/internal/ingest/receiver/agenticudp.go), which dispatches
// on the SignalType field of the envelope.
//
// Fidelity caveats are inherited from translate.go and re-stated here so
// they're discoverable when reading just this file:
//
//   - Histograms collapse to <name>_count and <name>_sum metrics.
//   - Summary and ExponentialHistogram are dropped.
//   - Span events and span links are not preserved (sppv1.Span lacks them,
//     matching Core's storage.Trace).
//   - MetricMapping → physics indicator translation is NOT applied here;
//     Core does it on the OTLP HTTP path. Path C records land as raw
//     storage rows. v2.14 will add a fifth signal type (otlp_raw) for
//     full-fidelity passthrough.

// TranslateTracesProto converts an OTLP ExportTraceServiceRequest into a
// sppv1.TraceBatch suitable for SendCycleProto. Empty input yields nil
// (the agenticudp client treats a nil batch as "skip this signal type"
// in SendCycleProto).
func TranslateTracesProto(req *collectortrace.ExportTraceServiceRequest, tenantID string) *sppv1.TraceBatch {
	if req == nil || len(req.GetResourceSpans()) == 0 {
		return nil
	}
	spans := make([]*sppv1.Span, 0, 8)
	for _, rs := range req.GetResourceSpans() {
		if rs == nil {
			continue
		}
		resAttrs := flattenAttrs(rs.GetResource().GetAttributes())
		serviceName := resAttrs["service.name"]
		if serviceName == "" {
			serviceName = "unknown-service"
		}
		for _, ss := range rs.GetScopeSpans() {
			if ss == nil {
				continue
			}
			scopeName := ss.GetScope().GetName()
			for _, sp := range ss.GetSpans() {
				if sp == nil {
					continue
				}
				attrs := mergeAttrs(resAttrs, flattenAttrs(sp.GetAttributes()))
				if scopeName != "" {
					attrs["otel.scope.name"] = scopeName
				}
				start := time.Unix(0, int64(sp.GetStartTimeUnixNano())).UTC()
				end := time.Unix(0, int64(sp.GetEndTimeUnixNano())).UTC()
				durUS := int64(0)
				if !end.Before(start) {
					durUS = end.Sub(start).Microseconds()
				}
				spans = append(spans, &sppv1.Span{
					TraceId:       hexBytes(sp.GetTraceId()),
					SpanId:        hexBytes(sp.GetSpanId()),
					ParentSpanId:  hexBytes(sp.GetParentSpanId()),
					ServiceName:   serviceName,
					OperationName: sp.GetName(),
					StartTime:     timestamppb.New(start),
					EndTime:       timestamppb.New(end),
					DurationUs:    durUS,
					StatusCode:    statusCodeString(sp.GetStatus()),
					SpanKind:      protoSpanKindString(sp.GetKind()),
					Attributes:    attrs,
					TenantId:      tenantID,
				})
			}
		}
	}
	if len(spans) == 0 {
		return nil
	}
	return &sppv1.TraceBatch{Spans: spans}
}

// TranslateMetricsProto converts an OTLP ExportMetricsServiceRequest into
// a sppv1.MetricBatch. The single-value sppv1.Metric forces histograms
// to expand into two records, matching translate.go's Path B behaviour.
func TranslateMetricsProto(req *collectormetrics.ExportMetricsServiceRequest, tenantID string) *sppv1.MetricBatch {
	if req == nil || len(req.GetResourceMetrics()) == 0 {
		return nil
	}
	metrics := make([]*sppv1.Metric, 0, 16)
	for _, rm := range req.GetResourceMetrics() {
		if rm == nil {
			continue
		}
		resAttrs := flattenAttrs(rm.GetResource().GetAttributes())
		serviceName := resAttrs["service.name"]
		if serviceName == "" {
			serviceName = "unknown-service"
		}
		for _, sm := range rm.GetScopeMetrics() {
			if sm == nil {
				continue
			}
			for _, m := range sm.GetMetrics() {
				if m == nil {
					continue
				}
				metrics = appendProtoMetricFamily(metrics, m, resAttrs, serviceName, tenantID)
			}
		}
	}
	if len(metrics) == 0 {
		return nil
	}
	return &sppv1.MetricBatch{Metrics: metrics}
}

// TranslateLogsProto converts an OTLP ExportLogsServiceRequest into a
// sppv1.LogBatch.
func TranslateLogsProto(req *collectorlogs.ExportLogsServiceRequest, tenantID string) *sppv1.LogBatch {
	if req == nil || len(req.GetResourceLogs()) == 0 {
		return nil
	}
	out := make([]*sppv1.LogRecord, 0, 8)
	for _, rl := range req.GetResourceLogs() {
		if rl == nil {
			continue
		}
		resAttrs := flattenAttrs(rl.GetResource().GetAttributes())
		serviceName := resAttrs["service.name"]
		if serviceName == "" {
			serviceName = "unknown-service"
		}
		for _, sl := range rl.GetScopeLogs() {
			if sl == nil {
				continue
			}
			for _, lr := range sl.GetLogRecords() {
				if lr == nil {
					continue
				}
				ts := time.Unix(0, int64(lr.GetTimeUnixNano())).UTC()
				if lr.GetTimeUnixNano() == 0 {
					ts = time.Now().UTC()
				}
				out = append(out, &sppv1.LogRecord{
					Timestamp:      timestamppb.New(ts),
					TenantId:       tenantID,
					TraceId:        hexBytes(lr.GetTraceId()),
					SpanId:         hexBytes(lr.GetSpanId()),
					ServiceName:    serviceName,
					SeverityText:   lr.GetSeverityText(),
					SeverityNumber: int32(lr.GetSeverityNumber()),
					Body:           anyValueAsString(lr.GetBody()),
					Attributes:     flattenAttrs(lr.GetAttributes()),
					ResourceAttrs:  resAttrs,
				})
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return &sppv1.LogBatch{Logs: out}
}

func appendProtoMetricFamily(
	out []*sppv1.Metric,
	m *metricspb.Metric,
	resAttrs map[string]string,
	serviceName, tenantID string,
) []*sppv1.Metric {
	name := m.GetName()
	if name == "" {
		return out
	}
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		for _, dp := range d.Gauge.GetDataPoints() {
			out = append(out, gaugeOrSumProtoPoint(name, "gauge", dp, resAttrs, serviceName, tenantID))
		}
	case *metricspb.Metric_Sum:
		mtype := "counter"
		if !d.Sum.GetIsMonotonic() {
			mtype = "gauge"
		}
		for _, dp := range d.Sum.GetDataPoints() {
			out = append(out, gaugeOrSumProtoPoint(name, mtype, dp, resAttrs, serviceName, tenantID))
		}
	case *metricspb.Metric_Histogram:
		for _, dp := range d.Histogram.GetDataPoints() {
			ts := timestamppb.New(datapointTime(dp.GetTimeUnixNano(), dp.GetStartTimeUnixNano()))
			labels := mergeAttrs(resAttrs, flattenAttrs(dp.GetAttributes()))
			out = append(out,
				&sppv1.Metric{
					Timestamp: ts, ServiceName: serviceName, MetricName: name + "_count",
					MetricType: "counter", Value: float64(dp.GetCount()),
					Labels: labels, TenantId: tenantID,
				},
				&sppv1.Metric{
					Timestamp: ts, ServiceName: serviceName, MetricName: name + "_sum",
					MetricType: "counter", Value: dp.GetSum(),
					Labels: labels, TenantId: tenantID,
				},
			)
		}
	default:
		// Summary, ExponentialHistogram — see translate.go docstring.
	}
	return out
}

func gaugeOrSumProtoPoint(
	name, metricType string,
	dp *metricspb.NumberDataPoint,
	resAttrs map[string]string,
	serviceName, tenantID string,
) *sppv1.Metric {
	ts := timestamppb.New(datapointTime(dp.GetTimeUnixNano(), dp.GetStartTimeUnixNano()))
	value := 0.0
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		value = v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		value = float64(v.AsInt)
	}
	return &sppv1.Metric{
		Timestamp:   ts,
		ServiceName: serviceName,
		MetricName:  name,
		MetricType:  metricType,
		Value:       value,
		Labels:      mergeAttrs(resAttrs, flattenAttrs(dp.GetAttributes())),
		TenantId:    tenantID,
	}
}

// protoSpanKindString matches the same convention as spanKindString
// (translate.go) — duplicated rather than shared to avoid coupling Path
// B and Path C through a SPAN_KIND_INTERNAL constant table that would
// have to round-trip the same string.
func protoSpanKindString(k tracepb.Span_SpanKind) string {
	switch k {
	case tracepb.Span_SPAN_KIND_INTERNAL:
		return "INTERNAL"
	case tracepb.Span_SPAN_KIND_SERVER:
		return "SERVER"
	case tracepb.Span_SPAN_KIND_CLIENT:
		return "CLIENT"
	case tracepb.Span_SPAN_KIND_PRODUCER:
		return "PRODUCER"
	case tracepb.Span_SPAN_KIND_CONSUMER:
		return "CONSUMER"
	default:
		return "INTERNAL"
	}
}

// Compile-time assurance that the OTLP and sppv1 import surfaces stay in
// sync — if a future protoc-gen-go bump renames something, this will
// fail to compile alongside the production code.
var (
	_ = (&sppv1.Span{}).TenantId
	_ = (&sppv1.Metric{}).TenantId
	_ = (&sppv1.LogRecord{}).TenantId
	_ = (&logspb.LogRecord{}).TimeUnixNano
)
