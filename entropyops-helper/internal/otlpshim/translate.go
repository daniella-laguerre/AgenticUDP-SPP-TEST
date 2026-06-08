// Package otlpshim is the local OTLP → AgenticUDP (Path B) translator that
// runs inside the entropyops-agent process. It exposes a loopback OTLP
// HTTP/gRPC endpoint that any stock OpenTelemetry SDK can target without a
// custom exporter, then forwards the decoded payloads as Path B JSON
// envelopes over the agent's existing AgenticUDP session to Core.
//
// The shim closes the gap that prompted v2.12: AgenticUDP's wire format has
// always supported the four signal types (metrics, traces, logs, fingerprint)
// — see entropyops-v2/internal/ingest/receiver/agenticudp.go routeToQueue —
// but until v2.12 nothing translated stock OTLP into those envelopes. App
// instrumentation therefore had to use the OTLP HTTP listener on the Core's
// API port (8000) or OTLP gRPC on 4317, which means a second port between
// app hosts and Core. With the shim, the only inter-host port is udp/4320.
//
// translate.go: pure functions that map OTLP protobuf request types
// (collector/{traces,metrics,logs}/v1) into the helper's
// transport.{MetricRecord,TraceRecord,LogRecord} types. Those Go structs
// have JSON tags that exactly mirror Core's storage.{Metric,Trace,LogRecord},
// which is the shape the Core's Path B receiver unmarshals at
// agenticudp.go:750-773. The agent's AgenticUDP Client.SendMetrics /
// SendTraces / SendLogs methods accept arbitrary `interface{}` and wrap them
// in the Path B envelope (`{"signal_type", "tenant_id", "data"}`), so the
// translator only has to produce the right Go slices.
//
// We use transport.* mirrors instead of importing
// `entropyops-v2/internal/storage` because Go forbids importing `internal/`
// packages across modules. Drift is prevented by the comment on each
// transport struct ("matches storage.X on the core server side") and by the
// e2e smoke test (deploy/e2e-smoke.sh after v2.12).
//
// Fidelity notes (intentional v2.12 simplifications):
//
//   - Histograms: emitted as two metrics — `<name>_count` and `<name>_sum`.
//     Bucket boundaries are dropped. Add proper bucket-preserving translation
//     in v2.13 once Core's PromQL frontend supports them over the Path B
//     envelope.
//   - Summary: skipped (rare in modern SDKs; emit as count+sum like
//     histogram if/when needed).
//   - Exponential histograms: skipped, same reason.
//   - Span events / span links: not preserved — Core's storage.Trace doesn't
//     carry them today (see entropyops-v2/internal/storage/interface.go).
//   - Metric semantic conventions (MetricMapping → physics indicator): NOT
//     applied here. Path B writes raw storage records; physics indicators
//     are only computed on Core's OTLP/HTTP path. v2.13 will add a fifth
//     signal_type ("otlp_raw") for full-fidelity passthrough when the
//     operator opts in.
package otlpshim

import (
	"strconv"
	"time"

	"github.com/entropyops/entropyops-helper/internal/transport"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// TranslateTraces converts an OTLP ExportTraceServiceRequest's
// resource_spans tree into Path B `transport.TraceRecord` values. One OTLP
// span becomes one TraceRecord.
func TranslateTraces(rss []*tracepb.ResourceSpans, tenantID string) []transport.TraceRecord {
	if len(rss) == 0 {
		return nil
	}
	out := make([]transport.TraceRecord, 0, 8)
	for _, rs := range rss {
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
				out = append(out, transport.TraceRecord{
					TraceID:       hexBytes(sp.GetTraceId()),
					SpanID:        hexBytes(sp.GetSpanId()),
					ParentSpanID:  hexBytes(sp.GetParentSpanId()),
					ServiceName:   serviceName,
					OperationName: sp.GetName(),
					StartTime:     start,
					EndTime:       end,
					DurationUS:    durUS,
					StatusCode:    statusCodeString(sp.GetStatus()),
					SpanKind:      spanKindString(sp.GetKind()),
					Attributes:    attrs,
					TenantID:      tenantID,
				})
			}
		}
	}
	return out
}

// TranslateMetrics converts an OTLP ExportMetricsServiceRequest's
// resource_metrics tree into Path B `transport.MetricRecord` values.
// Multi-point data (Histogram count+sum) is emitted as multiple records so
// each one fits the single-value MetricRecord shape.
func TranslateMetrics(rms []*metricspb.ResourceMetrics, tenantID string) []transport.MetricRecord {
	if len(rms) == 0 {
		return nil
	}
	out := make([]transport.MetricRecord, 0, 16)
	for _, rm := range rms {
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
				out = appendMetricFamily(out, m, resAttrs, serviceName, tenantID)
			}
		}
	}
	return out
}

// TranslateLogs converts an OTLP ExportLogsServiceRequest's resource_logs
// tree into Path B `transport.LogRecord` values.
func TranslateLogs(rls []*logspb.ResourceLogs, tenantID string) []transport.LogRecord {
	if len(rls) == 0 {
		return nil
	}
	out := make([]transport.LogRecord, 0, 8)
	for _, rl := range rls {
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
				out = append(out, transport.LogRecord{
					Timestamp:      ts,
					TenantID:       tenantID,
					TraceID:        hexBytes(lr.GetTraceId()),
					SpanID:         hexBytes(lr.GetSpanId()),
					ServiceName:    serviceName,
					SeverityText:   lr.GetSeverityText(),
					SeverityNumber: int(lr.GetSeverityNumber()),
					Body:           anyValueAsString(lr.GetBody()),
					Attributes:     flattenAttrs(lr.GetAttributes()),
					ResourceAttrs:  resAttrs,
				})
			}
		}
	}
	return out
}

// appendMetricFamily expands one OTLP metric (which may contain many data
// points and several point-types) into the appropriate number of
// `transport.MetricRecord` rows.
func appendMetricFamily(
	out []transport.MetricRecord,
	m *metricspb.Metric,
	resAttrs map[string]string,
	serviceName, tenantID string,
) []transport.MetricRecord {
	name := m.GetName()
	if name == "" {
		return out
	}
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		for _, dp := range d.Gauge.GetDataPoints() {
			out = append(out, gaugeOrSumPoint(name, "gauge", dp, resAttrs, serviceName, tenantID))
		}
	case *metricspb.Metric_Sum:
		mtype := "counter"
		if !d.Sum.GetIsMonotonic() {
			mtype = "gauge"
		}
		for _, dp := range d.Sum.GetDataPoints() {
			out = append(out, gaugeOrSumPoint(name, mtype, dp, resAttrs, serviceName, tenantID))
		}
	case *metricspb.Metric_Histogram:
		for _, dp := range d.Histogram.GetDataPoints() {
			ts := datapointTime(dp.GetTimeUnixNano(), dp.GetStartTimeUnixNano())
			labels := mergeAttrs(resAttrs, flattenAttrs(dp.GetAttributes()))
			out = append(out,
				transport.MetricRecord{
					Timestamp: ts, ServiceName: serviceName, MetricName: name + "_count",
					MetricType: "counter", Value: float64(dp.GetCount()),
					Labels: labels, TenantID: tenantID,
				},
				transport.MetricRecord{
					Timestamp: ts, ServiceName: serviceName, MetricName: name + "_sum",
					MetricType: "counter", Value: dp.GetSum(),
					Labels: labels, TenantID: tenantID,
				},
			)
		}
	default:
		// Summary, ExponentialHistogram, etc. — see package docstring.
	}
	return out
}

func gaugeOrSumPoint(
	name, metricType string,
	dp *metricspb.NumberDataPoint,
	resAttrs map[string]string,
	serviceName, tenantID string,
) transport.MetricRecord {
	ts := datapointTime(dp.GetTimeUnixNano(), dp.GetStartTimeUnixNano())
	value := 0.0
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		value = v.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		value = float64(v.AsInt)
	}
	return transport.MetricRecord{
		Timestamp:   ts,
		ServiceName: serviceName,
		MetricName:  name,
		MetricType:  metricType,
		Value:       value,
		Labels:      mergeAttrs(resAttrs, flattenAttrs(dp.GetAttributes())),
		TenantID:    tenantID,
	}
}

func datapointTime(timeUnixNano, startUnixNano uint64) time.Time {
	if timeUnixNano > 0 {
		return time.Unix(0, int64(timeUnixNano)).UTC()
	}
	if startUnixNano > 0 {
		return time.Unix(0, int64(startUnixNano)).UTC()
	}
	return time.Now().UTC()
}

// flattenAttrs collapses an OTLP KeyValue list into a map[string]string,
// stringifying every value type so the result fits storage's label maps.
func flattenAttrs(kvs []*commonpb.KeyValue) map[string]string {
	if len(kvs) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		if kv == nil || kv.Key == "" {
			continue
		}
		out[kv.Key] = anyValueAsString(kv.Value)
	}
	return out
}

func mergeAttrs(base, overlay map[string]string) map[string]string {
	if len(base) == 0 && len(overlay) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(base)+len(overlay))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range overlay {
		out[k] = v
	}
	return out
}

// anyValueAsString stringifies an OTLP AnyValue. Composite values (arrays,
// kvlists) are flattened to a compact string form so a single label slot
// can hold them — Core's PromQL frontend expects string labels.
func anyValueAsString(av *commonpb.AnyValue) string {
	if av == nil {
		return ""
	}
	switch v := av.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return v.StringValue
	case *commonpb.AnyValue_BoolValue:
		if v.BoolValue {
			return "true"
		}
		return "false"
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(v.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(v.DoubleValue, 'f', -1, 64)
	case *commonpb.AnyValue_BytesValue:
		return hexBytes(v.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		parts := make([]byte, 0, 16)
		parts = append(parts, '[')
		for i, item := range v.ArrayValue.GetValues() {
			if i > 0 {
				parts = append(parts, ',')
			}
			parts = append(parts, anyValueAsString(item)...)
		}
		parts = append(parts, ']')
		return string(parts)
	case *commonpb.AnyValue_KvlistValue:
		parts := make([]byte, 0, 16)
		parts = append(parts, '{')
		for i, kv := range v.KvlistValue.GetValues() {
			if i > 0 {
				parts = append(parts, ',')
			}
			parts = append(parts, kv.Key...)
			parts = append(parts, '=')
			parts = append(parts, anyValueAsString(kv.Value)...)
		}
		parts = append(parts, '}')
		return string(parts)
	}
	return ""
}

func hexBytes(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	const hexdigits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2] = hexdigits[c>>4]
		out[i*2+1] = hexdigits[c&0x0f]
	}
	return string(out)
}

// statusCodeString maps OTLP Status_StatusCode → Core's storage convention
// ("OK" / "ERROR" / "UNSET").
func statusCodeString(s *tracepb.Status) string {
	if s == nil {
		return "UNSET"
	}
	switch s.GetCode() {
	case tracepb.Status_STATUS_CODE_OK:
		return "OK"
	case tracepb.Status_STATUS_CODE_ERROR:
		return "ERROR"
	default:
		return "UNSET"
	}
}

// spanKindString maps OTLP Span_SpanKind → Core's string convention.
// Matches what entropyops-v2/internal/ingest/receiver/otlp.go produces.
func spanKindString(k tracepb.Span_SpanKind) string {
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

// resourceServiceName is exported for tests; it pulls service.name from a
// ResourceSpans/ResourceMetrics/ResourceLogs Resource and falls back to a
// stable sentinel.
func resourceServiceName(r *resourcepb.Resource) string {
	if r == nil {
		return "unknown-service"
	}
	for _, kv := range r.GetAttributes() {
		if kv.GetKey() == "service.name" {
			if s := anyValueAsString(kv.GetValue()); s != "" {
				return s
			}
		}
	}
	return "unknown-service"
}
