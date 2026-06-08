package otlpshim

import (
	"context"
	"fmt"
	"time"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// PathAConfig parameterises the OTLP/gRPC forwarder. All fields except
// Target may be left zero; sensible defaults are filled in by
// NewPathAForwarder.
type PathAConfig struct {
	// Target is the host:port of Core's OTLP gRPC listener. Conventionally
	// tcp/4317 (matches entropyops-v2/internal/config/config.go DefaultGRPCPort).
	// Required — the constructor returns an error if empty.
	Target string

	// TenantID is sent as the gRPC `x-tenant` metadata header on every
	// outbound Export call. Core's authorizeContext (otlp.go:2343-2371)
	// uses it to scope writes; missing values fall back to Core's
	// default tenant.
	TenantID string

	// APIKey, when non-empty, is sent as `x-api-key`. Required only when
	// Core has requireAPIKey=true; harmless to send to a Core that doesn't
	// check it.
	APIKey string

	// AgentToken, when non-empty, is sent as `x-agent-token` for Cores
	// running with enableAgentIdentity=true (mTLS-style agent identity).
	AgentToken string

	// TLS toggles transport security. Cleartext (insecure.NewCredentials)
	// is the default because the v2.13 shim's main intended deployment
	// is on the same host as the app. Cross-host Path A SHOULD set TLS
	// true and configure server certs out-of-band; we don't stand up a
	// custom TLS config here to keep the surface narrow — system trust
	// store is used.
	TLS bool

	// DialTimeout caps how long we'll wait for the initial DialContext to
	// resolve and TCP-connect. Defaults to 5s. The first Export will
	// trigger the connection; we don't WithBlock() so the agent never
	// stalls startup waiting for Core.
	DialTimeout time.Duration

	// CallTimeout caps each per-Export RPC. Defaults to 10s. Apps using
	// stock OTLP exporters retry on DeadlineExceeded so a short timeout
	// here is preferable to a long one that would back-pressure the
	// listener.
	CallTimeout time.Duration
}

// pathAForwarder dials Core's OTLP gRPC listener once and reuses the
// connection for every Export. The shim's HTTP/gRPC listeners feed
// requests in unchanged: the OTLP wire format is the SAME inbound and
// outbound, so this forwarder is a thin auth-stamping passthrough.
//
// Why route OTLP through the agent at all when the app could speak gRPC
// to Core directly? Three reasons that came up repeatedly in operator
// reviews:
//
//  1. Single egress endpoint. Apps target 127.0.0.1:{4317,4318}; the
//     agent is the only thing on the host that talks outbound. Easy
//     firewall rule, easy to swap transports later by reconfiguring the
//     agent instead of every app.
//  2. Auth ownership. The app no longer needs the API key / agent token;
//     the agent injects them. Rotations stop touching app deployments.
//  3. Path swap without app redeploy. Same OTel SDK config drives Path
//     A, B, or C; operator picks the wire by changing one agent flag.
type pathAForwarder struct {
	cfg     PathAConfig
	conn    *grpc.ClientConn
	traces  collectortrace.TraceServiceClient
	metrics collectormetrics.MetricsServiceClient
	logs    collectorlogs.LogsServiceClient
}

// NewPathAForwarder validates cfg, dials Core, and returns a Forwarder
// ready to ship OTLP requests. The Dial uses WithBlock=false so a
// transiently-unreachable Core doesn't fail agent startup; per-Export
// errors will surface in the shim's fwdErrs counter when Core is down.
func NewPathAForwarder(cfg PathAConfig) (Forwarder, error) {
	if cfg.Target == "" {
		return nil, fmt.Errorf("otlpshim/pathA: Target is required (e.g. core:4317)")
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.CallTimeout <= 0 {
		cfg.CallTimeout = 10 * time.Second
	}
	if cfg.TenantID == "" {
		cfg.TenantID = "default"
	}

	dialCtx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	opts := []grpc.DialOption{
		// Accept gzip from Core; OTLP exporters sometimes turn this on.
		grpc.WithDefaultCallOptions(grpc.UseCompressor("gzip")),
	}
	if !cfg.TLS {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	// Note: when cfg.TLS is true we let the gRPC default (system trust
	// store) take over. A future enhancement could plumb a *tls.Config
	// through; for v2.13 the loopback-default deployment doesn't need it.

	conn, err := grpc.DialContext(dialCtx, cfg.Target, opts...)
	if err != nil {
		return nil, fmt.Errorf("otlpshim/pathA: dial %s: %w", cfg.Target, err)
	}

	return &pathAForwarder{
		cfg:     cfg,
		conn:    conn,
		traces:  collectortrace.NewTraceServiceClient(conn),
		metrics: collectormetrics.NewMetricsServiceClient(conn),
		logs:    collectorlogs.NewLogsServiceClient(conn),
	}, nil
}

// applyMetadata stamps tenant and (optional) auth headers on the outbound
// context. Core's otlp_grpc.go grpcMetadata reads these exact keys:
// `x-tenant`, `x-api-key`, `x-agent-token`. Anything else is ignored by
// Core but harmless on the wire.
func (f *pathAForwarder) applyMetadata(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(ctx, f.cfg.CallTimeout)
	md := metadata.New(nil)
	md.Set("x-tenant", f.cfg.TenantID)
	if f.cfg.APIKey != "" {
		md.Set("x-api-key", f.cfg.APIKey)
	}
	if f.cfg.AgentToken != "" {
		md.Set("x-agent-token", f.cfg.AgentToken)
	}
	return metadata.NewOutgoingContext(ctx, md), cancel
}

func (f *pathAForwarder) ForwardTraces(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) error {
	if req == nil || len(req.GetResourceSpans()) == 0 {
		return nil
	}
	ctx, cancel := f.applyMetadata(ctx)
	defer cancel()
	_, err := f.traces.Export(ctx, req)
	return err
}

func (f *pathAForwarder) ForwardMetrics(ctx context.Context, req *collectormetrics.ExportMetricsServiceRequest) error {
	if req == nil || len(req.GetResourceMetrics()) == 0 {
		return nil
	}
	ctx, cancel := f.applyMetadata(ctx)
	defer cancel()
	_, err := f.metrics.Export(ctx, req)
	return err
}

func (f *pathAForwarder) ForwardLogs(ctx context.Context, req *collectorlogs.ExportLogsServiceRequest) error {
	if req == nil || len(req.GetResourceLogs()) == 0 {
		return nil
	}
	ctx, cancel := f.applyMetadata(ctx)
	defer cancel()
	_, err := f.logs.Export(ctx, req)
	return err
}

func (f *pathAForwarder) Kind() ForwarderKind { return ForwarderPathA }

// Close tears down the gRPC connection. Safe to call multiple times.
func (f *pathAForwarder) Close() error {
	if f.conn == nil {
		return nil
	}
	return f.conn.Close()
}

var _ Forwarder = (*pathAForwarder)(nil)
