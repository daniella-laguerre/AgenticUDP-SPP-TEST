package otlpshim

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// recordingForwarder captures every Forward* call so tests can assert
// that the listeners hand the OTLP request to the active forwarder.
// Implements Forwarder so we don't need a Path A/B/C client during the
// listener test — that's what the per-forwarder unit tests are for.
type recordingForwarder struct {
	mu      sync.Mutex
	traces  []*collectortrace.ExportTraceServiceRequest
	metrics []*collectormetrics.ExportMetricsServiceRequest
	logs    []*collectorlogs.ExportLogsServiceRequest
	kind    ForwarderKind
}

func (r *recordingForwarder) ForwardTraces(_ context.Context, req *collectortrace.ExportTraceServiceRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.traces = append(r.traces, req)
	return nil
}

func (r *recordingForwarder) ForwardMetrics(_ context.Context, req *collectormetrics.ExportMetricsServiceRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics = append(r.metrics, req)
	return nil
}

func (r *recordingForwarder) ForwardLogs(_ context.Context, req *collectorlogs.ExportLogsServiceRequest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs = append(r.logs, req)
	return nil
}

func (r *recordingForwarder) Kind() ForwarderKind {
	if r.kind == "" {
		return "test"
	}
	return r.kind
}

func (r *recordingForwarder) Close() error { return nil }

// TestShimEndToEndHTTP exercises the full HTTP path: bind the shim on
// a known loopback port, POST a real OTLP/HTTP protobuf payload, and
// confirm that the recording forwarder receives the exact OTLP request
// (untranslated — translation now lives inside each Path*Forwarder).
//
// This proves the listener wiring works without needing a Core to run.
// Per-forwarder behaviour is covered by translate_test.go (Path B
// records), translate_proto_test.go (Path C sppv1 batches), and
// forwarder_patha_test.go (Path A bufconn).
func TestShimEndToEndHTTP(t *testing.T) {
	rec := &recordingForwarder{}
	const port = 14318

	shim, err := New(Config{
		Bind:     "127.0.0.1",
		HTTPPort: port,
		GRPCPort: 0,
		TenantID: "tenant-x",
	}, rec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := shim.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer shim.Stop()

	start := time.Now().Add(-100 * time.Millisecond)
	end := time.Now()
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "smoke-app"}}},
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					TraceId:           bytes.Repeat([]byte{0xab}, 16),
					SpanId:            bytes.Repeat([]byte{0xcd}, 8),
					Name:              "smoke",
					Kind:              tracepb.Span_SPAN_KIND_INTERNAL,
					StartTimeUnixNano: uint64(start.UnixNano()),
					EndTimeUnixNano:   uint64(end.UnixNano()),
				}},
			}},
		}},
	}
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	httpReq, _ := http.NewRequest(http.MethodPost,
		"http://127.0.0.1:14318/v1/traces", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: want 200, got %d", resp.StatusCode)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.traces) != 1 {
		t.Fatalf("forwarder traces: want 1, got %d", len(rec.traces))
	}
	got := rec.traces[0]
	if len(got.GetResourceSpans()) != 1 {
		t.Fatalf("ResourceSpans: want 1, got %d", len(got.GetResourceSpans()))
	}
	rs := got.GetResourceSpans()[0]
	svc := ""
	for _, kv := range rs.GetResource().GetAttributes() {
		if kv.GetKey() == "service.name" {
			svc = kv.GetValue().GetStringValue()
		}
	}
	if svc != "smoke-app" {
		t.Errorf("service.name: got %q", svc)
	}
	if name := rs.GetScopeSpans()[0].GetSpans()[0].GetName(); name != "smoke" {
		t.Errorf("span name: got %q", name)
	}

	stats := shim.Stats()
	if stats["traces_in"].(uint64) != 1 {
		t.Errorf("Stats traces_in: %v", stats)
	}
	if stats["transport"].(string) != "test" {
		t.Errorf("Stats transport label: got %v", stats["transport"])
	}

	// Every successful OTLP HTTP exchange must carry the transport
	// header so SDK exporters / curl -i can confirm the active path on
	// the same response that just shipped a span. If this regresses,
	// operators lose the per-request audit trail and the only remaining
	// signal is the agent log — which they may not have access to in a
	// hardened deploy.
	if got := resp.Header.Get(HeaderShimTransport); got != "test" {
		t.Errorf("%s on /v1/traces success: want %q, got %q", HeaderShimTransport, "test", got)
	}
}

// TestShimVisibilityEndpoints proves the operator-facing introspection
// surface works end-to-end: GET /eo/transport returns the runtime
// config (path, tenant, bind, ports) as JSON, GET /eo/stats returns the
// counters, and both stamp the X-EntropyOps-Shim-Transport header so
// the same request can be reused by curl-or-script-based health checks.
//
// Why we test this in the shim layer rather than only end-to-end on the
// agent: the agent flows are gated on the operator opting into the shim
// at startup, but the contract we actually promise customers is the
// HTTP shape — service desk, monitoring sidecars, and the
// deploy/instrumentation/verify-shim.* helper all parse it. A regression
// here would be silent in agent-only smoke tests but immediately break
// the documented "how do I know which path is live?" answer.
func TestShimVisibilityEndpoints(t *testing.T) {
	rec := &recordingForwarder{kind: ForwarderPathB}
	const port = 14328

	shim, err := New(Config{
		Bind:     "127.0.0.1",
		HTTPPort: port,
		GRPCPort: 0,
		TenantID: "acme-prod",
	}, rec)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := shim.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer shim.Stop()

	time.Sleep(100 * time.Millisecond)

	client := &http.Client{Timeout: 5 * time.Second}

	// ── /eo/transport ────────────────────────────────────────────────
	resp, err := client.Get("http://127.0.0.1:14328/eo/transport")
	if err != nil {
		t.Fatalf("GET /eo/transport: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/eo/transport status: want 200, got %d (body=%s)", resp.StatusCode, body)
	}
	if got := resp.Header.Get(HeaderShimTransport); got != string(ForwarderPathB) {
		t.Errorf("%s on /eo/transport: want %q, got %q", HeaderShimTransport, ForwarderPathB, got)
	}
	var info map[string]interface{}
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatalf("unmarshal /eo/transport body: %v\n%s", err, body)
	}
	if got := info["transport"]; got != string(ForwarderPathB) {
		t.Errorf("transport field: want %q, got %v", ForwarderPathB, got)
	}
	if got := info["tenant"]; got != "acme-prod" {
		t.Errorf("tenant field: want %q, got %v", "acme-prod", got)
	}
	if got := info["bind"]; got != "127.0.0.1" {
		t.Errorf("bind field: want %q, got %v", "127.0.0.1", got)
	}
	// JSON unmarshals numeric fields as float64; cast accordingly.
	if got, _ := info["http_port"].(float64); int(got) != port {
		t.Errorf("http_port field: want %d, got %v", port, info["http_port"])
	}
	if got := info["otlp_http_url"]; got != "http://127.0.0.1:14328" {
		t.Errorf("otlp_http_url field: got %v", got)
	}

	// ── /eo/stats ────────────────────────────────────────────────────
	resp, err = client.Get("http://127.0.0.1:14328/eo/stats")
	if err != nil {
		t.Fatalf("GET /eo/stats: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/eo/stats status: want 200, got %d (body=%s)", resp.StatusCode, body)
	}
	if got := resp.Header.Get(HeaderShimTransport); got != string(ForwarderPathB) {
		t.Errorf("%s on /eo/stats: want %q, got %q", HeaderShimTransport, ForwarderPathB, got)
	}
	var stats map[string]interface{}
	if err := json.Unmarshal(body, &stats); err != nil {
		t.Fatalf("unmarshal /eo/stats body: %v\n%s", err, body)
	}
	for _, k := range []string{"transport", "traces_in", "metrics_in", "logs_in", "forward_errors"} {
		if _, ok := stats[k]; !ok {
			t.Errorf("/eo/stats missing key %q (got %v)", k, stats)
		}
	}
}
