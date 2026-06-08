package otlpshim

import (
	"context"
	"fmt"

	sppv1 "github.com/entropyops/entropyops-v2/pkg/sppv1"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// PathCClient is the subset of *transport.Client that pathCForwarder
// needs. Production code passes a *transport.Client that's been opened
// with UseProtobuf=true; tests pass a stub.
//
// We deliberately use SendCycleProto (which sends per-signal-type
// envelopes at the proper tier) instead of three separate sends, because
// the shim's per-call payload is naturally one-signal-type-at-a-time
// already and SendCycleProto's tier mapping (Besteff for metrics,
// Reliable for logs, Guaranteed for traces) is already what we want.
type PathCClient interface {
	SendCycleProto(metrics *sppv1.MetricBatch, traces *sppv1.TraceBatch, logs *sppv1.LogBatch) error
}

// pathCForwarder ships translated OTLP requests as sppv1 protobuf
// envelopes over AgenticUDP, with whatever DTLS mode the underlying
// transport.Client was constructed with. Wire-compatible with what the
// agent already emits for its own host telemetry when run with
// `-wire-format=proto`, so Core's AgenticUDP receiver doesn't need a
// new code path for shim-forwarded data.
//
// Pick Path C over Path B when:
//
//   - Bandwidth matters more than easy on-wire debugging (proto is ~30%
//     smaller than the equivalent JSON envelope for the same OTLP
//     payload, more for wide-attribute spans).
//   - The udp/4320 leg crosses an untrusted network and DTLS is wired
//     up via -tls-mode=psk or -tls-mode=cert.
//   - Operators want fingerprint reports to share the same encoding;
//     fingerprints already use Path C exclusively (see
//     transport.SendFingerprintProto).
type pathCForwarder struct {
	client   PathCClient
	tenantID string
}

// NewPathCForwarder constructs the proto forwarder. The caller is
// responsible for ensuring the underlying transport.Client was opened
// with UseProtobuf=true (otherwise the wire envelope and the FLAG_PROTOBUF
// header bit will disagree and Core's receiver will JSON-decode garbage).
func NewPathCForwarder(client PathCClient, tenantID string) (Forwarder, error) {
	if client == nil {
		return nil, fmt.Errorf("otlpshim/pathC: client is required")
	}
	if tenantID == "" {
		tenantID = "default"
	}
	return &pathCForwarder{client: client, tenantID: tenantID}, nil
}

func (f *pathCForwarder) ForwardTraces(_ context.Context, req *collectortrace.ExportTraceServiceRequest) error {
	batch := TranslateTracesProto(req, f.tenantID)
	if batch == nil {
		return nil
	}
	return f.client.SendCycleProto(nil, batch, nil)
}

func (f *pathCForwarder) ForwardMetrics(_ context.Context, req *collectormetrics.ExportMetricsServiceRequest) error {
	batch := TranslateMetricsProto(req, f.tenantID)
	if batch == nil {
		return nil
	}
	return f.client.SendCycleProto(batch, nil, nil)
}

func (f *pathCForwarder) ForwardLogs(_ context.Context, req *collectorlogs.ExportLogsServiceRequest) error {
	batch := TranslateLogsProto(req, f.tenantID)
	if batch == nil {
		return nil
	}
	return f.client.SendCycleProto(nil, nil, batch)
}

func (f *pathCForwarder) Kind() ForwarderKind { return ForwarderPathC }

// Close is a no-op for the same reason as pathBForwarder.Close: the
// transport.Client lifecycle is owned by the agent main, not the shim.
func (f *pathCForwarder) Close() error { return nil }

var _ Forwarder = (*pathCForwarder)(nil)
