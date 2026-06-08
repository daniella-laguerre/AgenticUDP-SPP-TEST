package otlpshim

import (
	"context"
	"fmt"
	"log"
	"net"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // accept gzip-compressed OTLP/gRPC
)

// grpcServer registers the three OTLP service handlers and runs them on a
// loopback listener. This mirrors entropyops-v2/internal/ingest/receiver/
// otlp_grpc.go's StartGRPCOTLP, but instead of writing to storage it
// translates and forwards through the AgenticUDP Path B Forwarder.
type grpcServer struct {
	srv *grpc.Server
	lis net.Listener
}

func startGRPC(_ context.Context, bind string, port int, sink *Shim) (*grpcServer, error) {
	addr := fmt.Sprintf("%s:%d", bind, port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	srv := grpc.NewServer()
	collectortrace.RegisterTraceServiceServer(srv, &otlpTraceService{sink: sink})
	collectormetrics.RegisterMetricsServiceServer(srv, &otlpMetricsService{sink: sink})
	collectorlogs.RegisterLogsServiceServer(srv, &otlpLogsService{sink: sink})

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Printf("otlpshim: grpc serve %s: %v", addr, err)
		}
	}()
	return &grpcServer{srv: srv, lis: lis}, nil
}

func (g *grpcServer) stop() {
	g.srv.GracefulStop()
	_ = g.lis.Close()
}

// Service implementations. Each one delegates straight into the shim's
// translate+forward pipeline; we don't enforce auth here because the
// listener is loopback-by-default — the trust boundary is the host, not
// the gRPC handler.
type otlpTraceService struct {
	collectortrace.UnimplementedTraceServiceServer
	sink *Shim
}

func (s *otlpTraceService) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	s.sink.recvTraces(ctx, req)
	return &collectortrace.ExportTraceServiceResponse{}, nil
}

type otlpMetricsService struct {
	collectormetrics.UnimplementedMetricsServiceServer
	sink *Shim
}

func (s *otlpMetricsService) Export(ctx context.Context, req *collectormetrics.ExportMetricsServiceRequest) (*collectormetrics.ExportMetricsServiceResponse, error) {
	s.sink.recvMetrics(ctx, req)
	return &collectormetrics.ExportMetricsServiceResponse{}, nil
}

type otlpLogsService struct {
	collectorlogs.UnimplementedLogsServiceServer
	sink *Shim
}

func (s *otlpLogsService) Export(ctx context.Context, req *collectorlogs.ExportLogsServiceRequest) (*collectorlogs.ExportLogsServiceResponse, error) {
	s.sink.recvLogs(ctx, req)
	return &collectorlogs.ExportLogsServiceResponse{}, nil
}
