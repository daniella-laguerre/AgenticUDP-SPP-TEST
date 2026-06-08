package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/entropyops/entropyops-v2/internal/ingest/receiver"
	"github.com/entropyops/entropyops-v2/internal/storage"
)

// handleIngestTransports returns configured ingest listeners, runtime toggle state,
// verification counters, and last-batch timing for cross-path comparison.
//
// GET /api/ingest/transports
func (s *Server) handleIngestTransports(w http.ResponseWriter, r *http.Request) {
	_ = r
	cfg := s.cfg
	httpBatches, grpcBatches := receiver.OTLPIngestBatchCounts()
	httpLast, httpDur, httpHaveLast := receiver.OTLPIngestLastTiming("http")
	grpcLast, grpcDur, grpcHaveLast := receiver.OTLPIngestLastTiming("grpc")
	agenticLast, agenticDur, agenticHaveLast := receiver.AgenticUDPLastTiming()

	base := strings.TrimSuffix(strings.TrimSpace(cfg.PublicURL), "/")
	if base == "" {
		base = "http://127.0.0.1:" + strconv.Itoa(cfg.HTTPPort)
	}

	s.ingestToggleMu.RLock()
	httpOn := s.ingestOTLPHTTPEnabled
	grpcOn := s.ingestOTLPGRPCEnabled
	agenticOn := s.ingestAgenticUDPEnabled
	s.ingestToggleMu.RUnlock()

	httpRow := map[string]interface{}{
		"id":                     "otlp-http",
		"label":                  "OTLP over HTTP",
		"protocol":               "tcp",
		"port":                   cfg.HTTPPort,
		"listener_configured":    true,
		"ingest_enabled":         httpOn,
		"batches_received_total": httpBatches,
		"traffic_seen":           httpBatches > 0,
		"endpoint_hint":          base + "/ingest/<tenant>/v1/traces",
		"paths": []string{"/v1/metrics", "/v1/traces", "/v1/logs", "/v1/profiles", "/ingest/{tenant}/v1/..."},
		"timing_by_signal":       otlpSignalTimingMap("http", []string{"metrics", "traces", "logs", "profiles"}),
	}
	if httpHaveLast {
		httpRow["last_batch_at"] = httpLast.UTC().Format(time.RFC3339Nano)
		httpRow["last_batch_duration_ms"] = float64(httpDur.Nanoseconds()) / 1e6
	} else {
		httpRow["last_batch_at"] = nil
		httpRow["last_batch_duration_ms"] = nil
	}

	grpcRow := map[string]interface{}{
		"id":                     "otlp-grpc",
		"label":                  "OTLP gRPC",
		"protocol":               "tcp",
		"port":                   cfg.GRPCPort,
		"listener_configured":    true,
		"ingest_enabled":         grpcOn,
		"batches_received_total": grpcBatches,
		"traffic_seen":           grpcBatches > 0,
		"endpoint_hint":          ":" + strconv.Itoa(cfg.GRPCPort) + " (gRPC)",
		"paths": []string{
			"opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
			"opentelemetry.proto.collector.trace.v1.TraceService/Export",
			"opentelemetry.proto.collector.logs.v1.LogsService/Export",
		},
		"timing_by_signal": otlpSignalTimingMap("grpc", []string{"metrics", "traces", "logs"}),
	}
	if grpcHaveLast {
		grpcRow["last_batch_at"] = grpcLast.UTC().Format(time.RFC3339Nano)
		grpcRow["last_batch_duration_ms"] = float64(grpcDur.Nanoseconds()) / 1e6
	} else {
		grpcRow["last_batch_at"] = nil
		grpcRow["last_batch_duration_ms"] = nil
	}

	transports := []map[string]interface{}{httpRow, grpcRow}

	if cfg.EnableAgenticUDP && s.agenticUDP != nil {
		st := s.agenticUDP.Stats()
		seen := st.PacketsReceived > 0 || st.DatagramsAccepted > 0 || st.MetricsRecv > 0
		udpRow := map[string]interface{}{
			"id":                  "agentic-udp",
			"label":               "AgenticUDP (entropyops-agent)",
			"protocol":            "udp",
			"port":                cfg.AgenticUDPPort,
			"listener_configured": true,
			"ingest_enabled":      agenticOn,
			"tls_mode":            cfg.AgenticUDPTLSMode,
			"stats":               st,
			"traffic_seen":        seen,
			"endpoint_hint":       "udp://0.0.0.0:" + strconv.Itoa(cfg.AgenticUDPPort),
			"verification":        "traffic_seen when packets_received / datagrams_accepted / metrics_recv > 0 since startup",
			"legacy_port_note":    "Pre-v2.11 agents used udp/4318; pin ENTROPYOPS_AGENTIC_UDP_PORT if needed.",
		}
		if agenticHaveLast {
			udpRow["last_batch_at"] = agenticLast.UTC().Format(time.RFC3339Nano)
			udpRow["last_batch_duration_ms"] = float64(agenticDur.Nanoseconds()) / 1e6
		} else {
			udpRow["last_batch_at"] = nil
			udpRow["last_batch_duration_ms"] = nil
		}
		transports = append(transports, udpRow)
	} else {
		note := "Set ENTROPYOPS_ENABLE_AGENTIC_UDP=true to enable (disabled on some SaaS / UDP-less deployments)."
		if cfg.DeploymentMode == "saas" {
			note = "AgenticUDP is typically off in SaaS shapes without UDP ingress."
		}
		transports = append(transports, map[string]interface{}{
			"id":                  "agentic-udp",
			"label":               "AgenticUDP (entropyops-agent)",
			"protocol":            "udp",
			"port":                cfg.AgenticUDPPort,
			"listener_configured": false,
			"ingest_enabled":      false,
			"traffic_seen":        false,
			"endpoint_hint":       "",
			"note":                note,
		})
	}

	// bench_mode lets eo-ingest-bench (and any human reading the JSON)
	// know which write semantics the OTLP HTTP/gRPC handlers are using
	// for this run. "standard-baseline" = per-row INSERT against a
	// default-tuned SQLite, mirroring what an off-the-shelf OTLP
	// backend would do. "tuned" = bulk INSERT + WAL checkpointer +
	// NORMAL fsync, the absolute-ceiling configuration.
	benchMode := "tuned"
	if storage.IsStandardBaselineMode() {
		benchMode = "standard-baseline"
	}

	writeJSON(w, map[string]interface{}{
		"server_uptime_seconds": time.Since(s.startTime).Seconds(),
		"deployment_mode":       cfg.DeploymentMode,
		"http_api_port":         cfg.HTTPPort,
		"grpc_otlp_port":        cfg.GRPCPort,
		"public_url":            cfg.PublicURL,
		"toggle_persist_path":   ingestTogglePath(s),
		"transports":            transports,
		"bench_mode":            benchMode,
		"bench_mode_note":       "standard-baseline = OTLP HTTP/gRPC use per-row INSERT against a default-tuned SQLite (mirrors an off-the-shelf OTLP backend). tuned = all paths share the optimized backend (platform-self-test only). AgenticUDP always uses receiver-level batching.",
	})
}

func otlpSignalTimingMap(protocol string, signals []string) map[string]interface{} {
	out := make(map[string]interface{}, len(signals))
	for _, signal := range signals {
		last, dur, batches, ok := receiver.OTLPIngestSignalLastTiming(protocol, signal)
		row := map[string]interface{}{
			"batches_received_total": batches,
			"traffic_seen":           batches > 0,
			"last_batch_at":          nil,
			"last_batch_duration_ms": nil,
		}
		if ok {
			row["last_batch_at"] = last.UTC().Format(time.RFC3339Nano)
			row["last_batch_duration_ms"] = float64(dur.Nanoseconds()) / 1e6
		}
		out[signal] = row
	}
	return out
}
