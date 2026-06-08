package receiver

import (
	"context"

	"github.com/entropyops/entropyops-v2/internal/storage"
)

// Why this file exists
//
// The standard OTLP HTTP and OTLP gRPC handlers in this package, plus
// the shared ProcessOTLP* helpers in process.go, all need to dispatch
// their storage writes through ONE choke point so the
// "standard-baseline vs tuned" mode flag is honored uniformly. Without
// a single dispatch helper, each call site has to duplicate the
// `if sqlite.IsStandardBaselineMode() { WritePerRow } else { Write }`
// branch — and it's exactly that kind of inconsistency that lets a
// future refactor accidentally re-introduce the "all paths share the
// optimized backend, comparison is meaningless" problem the bench is
// supposed to expose.
//
// The AgenticUDP receiver does NOT call these helpers — it always
// uses r.store.WriteTraces (the bulk path) because receiver-level
// batching is one of AgenticUDP's product-level differentiators that
// applies regardless of the storage tier. The OTLP HTTP / gRPC paths
// represent the off-the-shelf comparison.

func (r *OTLPReceiver) writeTracesOTLP(ctx context.Context, traces []storage.Trace) error {
	if r.store == nil || len(traces) == 0 {
		return nil
	}
	if storage.IsStandardBaselineMode() {
		return r.store.WriteTracesPerRow(ctx, traces)
	}
	return r.store.WriteTraces(ctx, traces)
}

func (r *OTLPReceiver) writeMetricsOTLP(ctx context.Context, metrics []storage.Metric) error {
	if r.store == nil || len(metrics) == 0 {
		return nil
	}
	if storage.IsStandardBaselineMode() {
		return r.store.WriteMetricsPerRow(ctx, metrics)
	}
	return r.store.WriteMetrics(ctx, metrics)
}

func (r *OTLPReceiver) writeLogsOTLP(ctx context.Context, logs []storage.LogRecord) error {
	if r.store == nil || len(logs) == 0 {
		return nil
	}
	if storage.IsStandardBaselineMode() {
		return r.store.WriteLogsPerRow(ctx, logs)
	}
	return r.store.WriteLogs(ctx, logs)
}
