package receiver

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/entropyops/entropyops-v2/internal/storage"
)

// stubBackend records which Write* method was called, so the
// OTLPReceiver dispatch helpers can be exercised without touching
// real storage.
type stubBackend struct {
	storage.Backend // satisfy the rest of the surface area via nil embedding

	bulkTraces    atomic.Int32
	perRowTraces  atomic.Int32
	bulkMetrics   atomic.Int32
	perRowMetrics atomic.Int32
	bulkLogs      atomic.Int32
	perRowLogs    atomic.Int32
}

func (s *stubBackend) WriteTraces(ctx context.Context, traces []storage.Trace) error {
	s.bulkTraces.Add(1)
	return nil
}
func (s *stubBackend) WriteTracesPerRow(ctx context.Context, traces []storage.Trace) error {
	s.perRowTraces.Add(1)
	return nil
}
func (s *stubBackend) WriteMetrics(ctx context.Context, metrics []storage.Metric) error {
	s.bulkMetrics.Add(1)
	return nil
}
func (s *stubBackend) WriteMetricsPerRow(ctx context.Context, metrics []storage.Metric) error {
	s.perRowMetrics.Add(1)
	return nil
}
func (s *stubBackend) WriteLogs(ctx context.Context, logs []storage.LogRecord) error {
	s.bulkLogs.Add(1)
	return nil
}
func (s *stubBackend) WriteLogsPerRow(ctx context.Context, logs []storage.LogRecord) error {
	s.perRowLogs.Add(1)
	return nil
}

// TestOTLPReceiver_BaselineDispatch_StandardMode is the most important
// test in this package right now: it locks in the invariant that with
// the default env, the OTLP HTTP/gRPC dispatch helpers route to the
// per-row "off-the-shelf OTLP backend" path. Any future refactor that
// silently sends OTLP traffic through the bulk path again — making
// AgenticUDP look artificially comparable to a tuned HTTP backend in
// the cross-protocol bench — will fail this test.
func TestOTLPReceiver_BaselineDispatch_StandardMode(t *testing.T) {
	t.Setenv("ENTROPYOPS_STANDARD_BASELINE_MODE", "")
	stub := &stubBackend{}
	r := &OTLPReceiver{store: stub}

	if err := r.writeTracesOTLP(context.Background(), []storage.Trace{{}}); err != nil {
		t.Fatalf("writeTracesOTLP: %v", err)
	}
	if err := r.writeMetricsOTLP(context.Background(), []storage.Metric{{}}); err != nil {
		t.Fatalf("writeMetricsOTLP: %v", err)
	}
	if err := r.writeLogsOTLP(context.Background(), []storage.LogRecord{{}}); err != nil {
		t.Fatalf("writeLogsOTLP: %v", err)
	}

	if got := stub.perRowTraces.Load(); got != 1 {
		t.Fatalf("standard-baseline: traces should hit per-row path; got %d", got)
	}
	if got := stub.bulkTraces.Load(); got != 0 {
		t.Fatalf("standard-baseline: traces must NOT hit bulk path; got %d (this is the regression that masks AgenticUDP's value)", got)
	}
	if got := stub.perRowMetrics.Load(); got != 1 {
		t.Fatalf("standard-baseline: metrics should hit per-row path; got %d", got)
	}
	if got := stub.bulkMetrics.Load(); got != 0 {
		t.Fatalf("standard-baseline: metrics must NOT hit bulk path; got %d", got)
	}
	if got := stub.perRowLogs.Load(); got != 1 {
		t.Fatalf("standard-baseline: logs should hit per-row path; got %d", got)
	}
	if got := stub.bulkLogs.Load(); got != 0 {
		t.Fatalf("standard-baseline: logs must NOT hit bulk path; got %d", got)
	}
}

// TestOTLPReceiver_BaselineDispatch_TunedMode verifies the opt-in
// "absolute ceiling" configuration. When the operator explicitly sets
// ENTROPYOPS_STANDARD_BASELINE_MODE=false the OTLP HTTP/gRPC paths
// share AgenticUDP's bulk write path. This is the platform-self-test
// configuration; the test exists so a future refactor can't accidentally
// disable the override.
func TestOTLPReceiver_BaselineDispatch_TunedMode(t *testing.T) {
	t.Setenv("ENTROPYOPS_STANDARD_BASELINE_MODE", "false")
	stub := &stubBackend{}
	r := &OTLPReceiver{store: stub}

	if err := r.writeTracesOTLP(context.Background(), []storage.Trace{{}}); err != nil {
		t.Fatalf("writeTracesOTLP: %v", err)
	}
	if err := r.writeMetricsOTLP(context.Background(), []storage.Metric{{}}); err != nil {
		t.Fatalf("writeMetricsOTLP: %v", err)
	}
	if err := r.writeLogsOTLP(context.Background(), []storage.LogRecord{{}}); err != nil {
		t.Fatalf("writeLogsOTLP: %v", err)
	}

	if got := stub.bulkTraces.Load(); got != 1 {
		t.Fatalf("tuned mode: traces should hit bulk path; got %d", got)
	}
	if got := stub.perRowTraces.Load(); got != 0 {
		t.Fatalf("tuned mode: traces must NOT hit per-row path; got %d", got)
	}
	if got := stub.bulkMetrics.Load(); got != 1 {
		t.Fatalf("tuned mode: metrics should hit bulk path; got %d", got)
	}
	if got := stub.bulkLogs.Load(); got != 1 {
		t.Fatalf("tuned mode: logs should hit bulk path; got %d", got)
	}
}

// TestOTLPReceiver_BaselineDispatch_EmptyAndNilGuards covers the
// two no-op shortcuts. Empty slices must NOT fan out to either path,
// and a nil store must short-circuit instead of panicking — the
// receiver is constructed before storage in some boot orderings.
func TestOTLPReceiver_BaselineDispatch_EmptyAndNilGuards(t *testing.T) {
	t.Setenv("ENTROPYOPS_STANDARD_BASELINE_MODE", "")
	stub := &stubBackend{}
	r := &OTLPReceiver{store: stub}

	if err := r.writeTracesOTLP(context.Background(), nil); err != nil {
		t.Fatalf("nil traces: %v", err)
	}
	if err := r.writeMetricsOTLP(context.Background(), nil); err != nil {
		t.Fatalf("nil metrics: %v", err)
	}
	if err := r.writeLogsOTLP(context.Background(), nil); err != nil {
		t.Fatalf("nil logs: %v", err)
	}
	if stub.perRowTraces.Load()+stub.bulkTraces.Load() != 0 {
		t.Fatalf("empty traces should not call any storage method")
	}

	rNil := &OTLPReceiver{store: nil}
	if err := rNil.writeTracesOTLP(context.Background(), []storage.Trace{{}}); err != nil {
		t.Fatalf("nil store traces: %v", err)
	}
	if err := rNil.writeMetricsOTLP(context.Background(), []storage.Metric{{}}); err != nil {
		t.Fatalf("nil store metrics: %v", err)
	}
	if err := rNil.writeLogsOTLP(context.Background(), []storage.LogRecord{{}}); err != nil {
		t.Fatalf("nil store logs: %v", err)
	}
}
