package otlpshim

import (
	"context"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// ForwarderKind names the AgenticUDP-family transport that carries the
// translated OTLP payload from the agent to Core. The three values match
// the codebase's pre-existing Path A / Path B / Path C scheme so logs,
// metrics, and docs all line up:
//
//	Path A — OTLP/gRPC over tcp/4317 (the conventional OTLP transport, but
//	         flowing through the agent so the app sees a single egress point
//	         and the agent owns auth, batching, and retries).
//	Path B — AgenticUDP V2 datagram with a JSON envelope, udp/4320
//	         cleartext. The v2.12 default; lossy-friendly and zero TLS cost.
//	Path C — AgenticUDP V2 datagram with a sppv1 protobuf Envelope and
//	         optional DTLS, udp/4320. Smaller wire size + encryption for
//	         hostile networks, at the cost of needing a DTLS PSK or cert.
//
// The shim's transport selector lets an operator pick one path for ALL
// app instrumentation forwarded through this agent, independently of the
// agent's own host-scraper transport. The "right" choice is a
// network-policy + workload decision; see docs/architecture/
// INGEST_PIPELINE_TRACKS.md (Track 1a) for a decision matrix.
type ForwarderKind string

const (
	ForwarderPathA ForwarderKind = "pathA"
	ForwarderPathB ForwarderKind = "pathB"
	ForwarderPathC ForwarderKind = "pathC"
)

// Forwarder ships translated OTLP payloads to Core via one of the three
// AgenticUDP-family transports. Implementations live in:
//
//	forwarder_patha.go — pathAForwarder (OTLP/gRPC client to Core)
//	forwarder_pathb.go — pathBForwarder (AgenticUDP JSON via *transport.Client)
//	forwarder_pathc.go — pathCForwarder (AgenticUDP proto via *transport.Client)
//
// All three accept the raw OTLP collector request types so the listener
// (HTTP or gRPC) can hand off without an intermediate Go-typed
// translation. Path A is then a near-zero-cost passthrough; Path B and C
// translate inside the implementation. This keeps the listener layer
// transport-agnostic and lets us add a fourth path later by adding a new
// forwarder file with no churn elsewhere.
type Forwarder interface {
	// ForwardTraces ships an OTLP ExportTraceServiceRequest. ctx carries
	// the listener's request context so cancellation propagates to the
	// outbound call (matters for pathA — pathB/C already fire-and-forget).
	ForwardTraces(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) error
	ForwardMetrics(ctx context.Context, req *collectormetrics.ExportMetricsServiceRequest) error
	ForwardLogs(ctx context.Context, req *collectorlogs.ExportLogsServiceRequest) error

	// Kind reports which transport this forwarder uses. Used for log
	// labels and the shim's Stats() output so operators can confirm at a
	// glance which path they actually wired up.
	Kind() ForwarderKind

	// Close releases any resources owned by the forwarder (e.g. pathA's
	// gRPC client conn). pathB and pathC delegate to *transport.Client
	// owned by the agent main, so their Close is a no-op — the agent
	// closes the transport client itself on shutdown.
	Close() error
}
