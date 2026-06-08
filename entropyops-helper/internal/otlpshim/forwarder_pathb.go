package otlpshim

import (
	"context"
	"fmt"

	"github.com/entropyops/entropyops-helper/internal/transport"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// PathBClient is the subset of *transport.Client that pathBForwarder needs.
// Defined as an interface so the unit tests can stub it without standing
// up a full AgenticUDP session. The agent main passes its real
// *transport.Client which already implements these three methods.
type PathBClient interface {
	SendMetrics(data interface{}) error
	SendTraces(data interface{}) error
	SendLogs(data interface{}) error
}

// pathBForwarder turns OTLP requests into transport.{Trace,Metric,Log}Record
// slices and ships them as AgenticUDP JSON envelopes (signal_type =
// "traces"/"metrics"/"logs"). This is the v2.12 default path: the JSON
// envelope predates the shim and is what the agent's host-scraper has
// been emitting since v2.7, so Core's AgenticUDP receiver routeToQueue
// already knows how to land these in storage.
//
// JSON envelope wins on operator simplicity (tcpdump-readable in dev,
// jq-pipeable in tests) and protocol compatibility with the existing
// receiver. It loses on wire size vs Path C — for an app emitting
// thousands of spans per minute, prefer Path C if Core is on a metered
// link.
type pathBForwarder struct {
	client   PathBClient
	tenantID string
}

// NewPathBForwarder constructs the JSON-envelope forwarder. The client
// MUST already be Connect()'d; calling SendTraces on an un-connected
// client returns an error per agenticudp.go:233 ("not connected").
func NewPathBForwarder(client PathBClient, tenantID string) (Forwarder, error) {
	if client == nil {
		return nil, fmt.Errorf("otlpshim/pathB: client is required")
	}
	if tenantID == "" {
		tenantID = "default"
	}
	return &pathBForwarder{client: client, tenantID: tenantID}, nil
}

func (f *pathBForwarder) ForwardTraces(_ context.Context, req *collectortrace.ExportTraceServiceRequest) error {
	records := TranslateTraces(req.GetResourceSpans(), f.tenantID)
	if len(records) == 0 {
		return nil
	}
	return f.client.SendTraces(records)
}

func (f *pathBForwarder) ForwardMetrics(_ context.Context, req *collectormetrics.ExportMetricsServiceRequest) error {
	records := TranslateMetrics(req.GetResourceMetrics(), f.tenantID)
	if len(records) == 0 {
		return nil
	}
	return f.client.SendMetrics(records)
}

func (f *pathBForwarder) ForwardLogs(_ context.Context, req *collectorlogs.ExportLogsServiceRequest) error {
	records := TranslateLogs(req.GetResourceLogs(), f.tenantID)
	if len(records) == 0 {
		return nil
	}
	return f.client.SendLogs(records)
}

func (f *pathBForwarder) Kind() ForwarderKind { return ForwarderPathB }

// Close is a no-op for Path B because pathBForwarder doesn't own the
// transport.Client — the agent's main goroutine constructs and Close()s it.
// Closing here would tear down the host-scraper's transport too.
func (f *pathBForwarder) Close() error { return nil }

// Compile-time interface satisfaction check. Cheap insurance against the
// Forwarder interface drifting away from any of the three implementations.
var _ Forwarder = (*pathBForwarder)(nil)

// transport-record types are re-exported here only as a convenience so
// tests in this package can construct expected values without a separate
// import line. Production code uses the transport package directly.
type (
	bTraceRecord  = transport.TraceRecord
	bMetricRecord = transport.MetricRecord
	bLogRecord    = transport.LogRecord
)
