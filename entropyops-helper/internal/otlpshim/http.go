package otlpshim

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// HeaderShimTransport is set on every successful OTLP HTTP response so
// SDK exporters that surface response headers can confirm which forward
// path their telemetry actually rode. Centralised as a constant so docs
// and tests can reference it without typo risk.
const HeaderShimTransport = "X-EntropyOps-Shim-Transport"

// httpServer wraps a *http.Server bound to a loopback OTLP endpoint.
// Routes mirror the OTLP/HTTP spec: POST /v1/traces, /v1/metrics, /v1/logs.
// The shim accepts both `application/x-protobuf` (default for stock OTel
// SDK exporters) and `application/json` (curl-friendly, used by docs).
type httpServer struct {
	srv *http.Server
	lis net.Listener
}

const (
	maxBodyBytes = 16 << 20 // 16 MiB — generous; OTLP exporters batch but
	// rarely exceed a few MiB. Cap exists to bound memory if a misconfigured
	// app fires huge payloads.
)

func startHTTP(_ context.Context, bind string, port int, sink *Shim) (*httpServer, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", makeOTLPHandler(sink, decodeTraces))
	mux.HandleFunc("/v1/metrics", makeOTLPHandler(sink, decodeMetrics))
	mux.HandleFunc("/v1/logs", makeOTLPHandler(sink, decodeLogs))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// Visibility endpoints — operators MUST be able to confirm at runtime
	// which forward path is live without reading the agent log. /eo/transport
	// is the small, stable shape used by deploy/instrumentation/verify-shim.*
	// and by the per-language pack READMEs. /eo/stats exposes the running
	// counters so a tester can see whether their app's spans are actually
	// arriving at the shim before chasing Core-side issues.
	mux.HandleFunc("/eo/transport", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(HeaderShimTransport, string(sink.fwd.Kind()))
		_ = json.NewEncoder(w).Encode(sink.transportInfo())
	})
	mux.HandleFunc("/eo/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(HeaderShimTransport, string(sink.fwd.Kind()))
		_ = json.NewEncoder(w).Encode(sink.Stats())
	})

	addr := fmt.Sprintf("%s:%d", bind, port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		if err := srv.Serve(lis); err != nil && err != http.ErrServerClosed {
			log.Printf("otlpshim: http serve %s: %v", addr, err)
		}
	}()
	return &httpServer{srv: srv, lis: lis}, nil
}

func (h *httpServer) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = h.srv.Shutdown(ctx)
	_ = h.lis.Close()
}

// otlpDecoder pulls the raw body off the wire (handling gzip + content-type)
// and routes it through one of the three OTLP signal-type decoders. Each
// decoder owns the translation + forwarding for its signal so the
// per-route handler stays uniform.
type otlpDecoder func(ctx context.Context, sink *Shim, contentType string, body []byte) (status int, err error)

func makeOTLPHandler(sink *Shim, dec otlpDecoder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ct := r.Header.Get("Content-Type")
		body, err := readBody(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		status, derr := dec(r.Context(), sink, ct, body)
		if derr != nil {
			http.Error(w, derr.Error(), status)
			return
		}
		// OTLP/HTTP spec: 200 with a (possibly empty) Export*ServiceResponse
		// proto. Returning an empty body is allowed by the spec for
		// success and is what most receivers do; SDKs accept it cleanly.
		// We stamp X-EntropyOps-Shim-Transport on every success so any
		// SDK exporter that surfaces response headers (or any sidecar
		// proxy, or `curl -i`) can confirm the active path on the same
		// HTTP exchange that just shipped a span. Cheap insurance
		// against a misconfigured agent silently dropping payloads on
		// the wrong path.
		w.Header().Set(HeaderShimTransport, string(sink.fwd.Kind()))
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}
}

func readBody(r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	var reader io.Reader = http.MaxBytesReader(nil, r.Body, maxBodyBytes)
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("gzip: %w", err)
		}
		defer gz.Close()
		reader = gz
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return body, nil
}

func decodeTraces(ctx context.Context, sink *Shim, contentType string, body []byte) (int, error) {
	req := &collectortrace.ExportTraceServiceRequest{}
	if err := unmarshalOTLP(contentType, body, req); err != nil {
		return http.StatusBadRequest, fmt.Errorf("decode traces: %w", err)
	}
	sink.recvTraces(ctx, req)
	return http.StatusOK, nil
}

func decodeMetrics(ctx context.Context, sink *Shim, contentType string, body []byte) (int, error) {
	req := &collectormetrics.ExportMetricsServiceRequest{}
	if err := unmarshalOTLP(contentType, body, req); err != nil {
		return http.StatusBadRequest, fmt.Errorf("decode metrics: %w", err)
	}
	sink.recvMetrics(ctx, req)
	return http.StatusOK, nil
}

func decodeLogs(ctx context.Context, sink *Shim, contentType string, body []byte) (int, error) {
	req := &collectorlogs.ExportLogsServiceRequest{}
	if err := unmarshalOTLP(contentType, body, req); err != nil {
		return http.StatusBadRequest, fmt.Errorf("decode logs: %w", err)
	}
	sink.recvLogs(ctx, req)
	return http.StatusOK, nil
}

// unmarshalOTLP handles the two content types every OTLP HTTP receiver
// must accept: protobuf and JSON. We use protojson for JSON to honor OTLP's
// proto-defined field-naming (lowerCamelCase) instead of Go's default.
func unmarshalOTLP(contentType string, body []byte, msg proto.Message) error {
	if strings.HasPrefix(strings.ToLower(contentType), "application/json") {
		return protojson.Unmarshal(body, msg)
	}
	return proto.Unmarshal(body, msg)
}
