// eo-ingest-bench is the official EntropyOps three-path ingest benchmark.
//
// It exists because server-side per-handler timers (the
// last_batch_duration_ms numbers exposed at /api/ingest/transports) measure
// DIFFERENT scopes of work for HTTP vs gRPC vs AgenticUDP, so they CANNOT
// be compared directly:
//
//	OTLP HTTP  : auth + parse + sampler + k8s join + WriteTraces + 3 fan-out goroutines
//	OTLP gRPC  : auth + parse + WriteTraces (no sampler, no k8s join)
//	AgenticUDP : decode envelope + WriteTraces  (no sampler, no k8s join)
//
// To produce client-side wall-clock numbers operators can reproduce, this tool:
//
//   - Holds a single persistent connection per path (HTTP keepalive, gRPC
//     ClientConn, AgenticUDP session). No per-iteration handshake.
//   - Sends an IDENTICAL trace payload (N spans, configurable) on every path.
//   - Generates fresh random trace/span IDs per iteration so the AgenticUDP
//     receiver's content-id dedup does not silently skip work.
//   - Measures CLIENT wall-clock per request from "send byte 1" to:
//     HTTP:  HTTP 2xx response received
//     gRPC:  Export() RPC returns
//     UDP :  AgenticUDP server ACK received (Stats().Acked counter advances)
//   - Reports p50 / p90 / p99 / avg / min / max + ms-per-span so different
//     batch sizes are still comparable across paths.
//
// Important: even with the corrections above, this benchmark CANNOT today
// be read as a clean head-to-head between the three transports. The
// asymmetric server handlers (HTTP runs sampler + k8s join; gRPC and
// AgenticUDP do not) and the still-being-tuned AgenticUDP bulk-send path
// mean the numbers are best read as a lower bound on HTTP and an upper
// bound on gRPC + AgenticUDP. The footer of every run reprints those
// limits so they travel with any screenshot.
//
// Bench mode (the most important caveat to understand):
//
//	The OTLP HTTP and gRPC handlers in this repo write to the SAME
//	SQLite backend AgenticUDP writes to. By default that backend is
//	in "standard-baseline" mode: per-row INSERT, default fsync, no
//	out-of-band WAL checkpointer — i.e. it behaves like an off-the-
//	shelf SQL-backed OTLP receiver, which is what a customer's
//	existing OTel collector would be talking to. The AgenticUDP
//	receiver always uses receiver-level batching because that's part
//	of the AgenticUDP product, not a backend tuning. THIS IS THE
//	BENCH CONFIG THAT REPRESENTS THE INDUSTRY GAP AGENTICUDP EXISTS
//	TO CLOSE.
//
//	Setting ENTROPYOPS_STANDARD_BASELINE_MODE=false on the server
//	flips every path to the heavily tuned bulk-INSERT backend with
//	out-of-band WAL checkpointing. That is platform-self-test mode;
//	it is NOT an honest comparison vs Jaeger / Tempo / off-the-shelf
//	OTLP collector. The bench tool fetches the active mode from
//	GET /api/ingest/transports and prints it in both the header and
//	the JSON output so any captured numbers are self-describing.
//
//	For the gold-standard apples-to-apples comparison see
//	docs/operator/THIRD_PARTY_BACKEND_BENCH_PLAN.md, which points
//	-server / -grpc at an actual third-party OTLP backend (Jaeger,
//	OTel Collector debug exporter) instead of at the in-tree one.
//
// Run:
//
//	eo-ingest-bench \
//	  -server http://localhost:8000 \
//	  -grpc localhost:4317 \
//	  -udp 127.0.0.1:4320 \
//	  -tenant Trading-dev \
//	  -spans 200 -iters 50 -warmup 5
//
// Honest interpretation hints printed in the footer summarize what each
// p50 means and why ms-per-span is the cross-path comparable.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/entropyops/entropyops-helper/internal/benchstats"
	"github.com/entropyops/entropyops-helper/internal/transport"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// pathResult is an alias for the shared schema in internal/benchstats so
// that eo-bench-merge produces byte-for-byte identical JSON to a single
// live run.
type pathResult = benchstats.PathResult

func main() {
	var (
		serverURL    = flag.String("server", "http://localhost:8000", "Core HTTP base URL (used for OTLP HTTP traces and the /api/ingest/transports preflight)")
		grpcAddr     = flag.String("grpc", "localhost:4317", "Core OTLP gRPC address")
		udpAddr      = flag.String("udp", "127.0.0.1:4320", "Core AgenticUDP address")
		tenant       = flag.String("tenant", "Trading-dev", "Tenant ID (sent as x-tenant for OTLP, encoded in AgenticUDP envelope)")
		apiKey       = flag.String("api-key", "", "Optional x-api-key header for OTLP")
		bearer       = flag.String("bearer", "", "Optional Bearer token (Authorization header) for OTLP HTTP/gRPC")
		spansPerCall = flag.Int("spans", 200, "Number of spans per request")
		iters        = flag.Int("iters", 50, "Iterations per path")
		warmup       = flag.Int("warmup", 5, "Warmup iterations (excluded from stats)")
		paths        = flag.String("paths", "http,grpc,udp", "Comma-separated paths to test")
		ackTimeoutMs    = flag.Int("udp-ack-timeout-ms", 10000, "Per-request ACK wait timeout for AgenticUDP (covers all chunk ACKs for one logical batch)")
		udpRetransmitMs = flag.Int("udp-retransmit-ms", 0, "AgenticUDP base retransmit timeout in milliseconds (0 = use ENTROPYOPS_AGENT_UDP_RETRANSMIT_MS or built-in default of 2000). Larger values reduce retransmit fan-out under server-side coalescer backpressure; the per-packet timeout doubles up to 30s on each retry.")
		jsonOut         = flag.String("json", "", "Optional path to write JSON results")
		keepSamples     = flag.Bool("keep-samples", false, "Include raw per-iteration samples in JSON output")
	)
	flag.Parse()

	if *spansPerCall <= 0 || *iters <= 0 {
		log.Fatalf("spans and iters must be > 0")
	}

	enabled, err := parsePathsFlag(*paths)
	if err != nil {
		log.Fatalf("-paths: %v", err)
	}

	fmt.Printf("eo-ingest-bench  spans=%d  iters=%d  warmup=%d  tenant=%s\n", *spansPerCall, *iters, *warmup, *tenant)
	fmt.Printf("targets:  http=%s  grpc=%s  udp=%s\n", *serverURL, *grpcAddr, *udpAddr)

	benchMode, modeErr := probeBenchMode(*serverURL)
	switch {
	case modeErr != nil:
		fmt.Printf("server bench_mode: UNKNOWN (probe failed: %v) — assume standard-baseline if you didn't override\n", modeErr)
		benchMode = "unknown"
	case benchMode == "standard-baseline":
		fmt.Println("server bench_mode: standard-baseline (OTLP HTTP/gRPC use per-row INSERT against default-tuned SQLite — represents the industry gap)")
	case benchMode == "tuned":
		fmt.Println("server bench_mode: TUNED (ENTROPYOPS_STANDARD_BASELINE_MODE=false on the server). All paths share the optimized backend; this is platform-self-test mode, NOT an apples-to-apples vs an off-the-shelf OTLP backend.")
	default:
		fmt.Printf("server bench_mode: %s\n", benchMode)
	}
	fmt.Println()

	results := make([]pathResult, 0, 3)

	if enabled["http"] {
		r := runHTTP(*serverURL, *tenant, *apiKey, *bearer, *spansPerCall, *iters, *warmup)
		results = append(results, r)
	}
	if enabled["grpc"] {
		r := runGRPC(*grpcAddr, *tenant, *apiKey, *bearer, *spansPerCall, *iters, *warmup)
		results = append(results, r)
	}
	if enabled["udp"] {
		r := runUDP(*udpAddr, *tenant, *spansPerCall, *iters, *warmup, time.Duration(*ackTimeoutMs)*time.Millisecond, *udpRetransmitMs)
		results = append(results, r)
	}

	printTable(results)
	printFooter(results, benchMode)

	if *jsonOut != "" {
		if !*keepSamples {
			for i := range results {
				results[i].Samples = nil
				results[i].FailedSamples = nil
			}
		}
		out := map[string]interface{}{
			"generated_at":   time.Now().UTC().Format(time.RFC3339),
			"spans_per_call": *spansPerCall,
			"iterations":     *iters,
			"warmup":         *warmup,
			"tenant":         *tenant,
			"bench_mode":     benchMode,
			"results":        results,
		}
		f, err := os.Create(*jsonOut)
		if err != nil {
			log.Fatalf("write json: %v", err)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			log.Fatalf("encode json: %v", err)
		}
		fmt.Printf("\nWrote JSON results to %s\n", *jsonOut)
	}
}

// ── HTTP ───────────────────────────────────────────────────────────────────

// httpClientTimeout is the per-request HTTP timeout the bench enforces.
// It mirrors the generous end of OTLP exporter defaults in the field
// (OpenTelemetry SDK OTLP exporters default to 10 s; OTel Collector
// defaults to 5 s; AWS X-Ray and Datadog Agent are around 30 s). A real
// customer's exporter would have already given up at 5–10 s; we use 30 s
// so the bench measurement extends slightly past where production
// exporters would, while still capping the unbounded tail.
//
// When a request exceeds this timeout the failure is COUNTED (in
// PathResult.Timeouts) and the actual wall-clock attempt duration is
// captured (in PathResult.FailedSamples when -keep-samples is set), so
// the censored tail is recoverable rather than silently dropped.
const httpClientTimeout = 30 * time.Second

// errBenchTimeout is the canonical sentinel used to classify a failed
// iteration as a transport-level timeout vs a non-2xx response vs some
// other error. We classify by inspecting the error string for the
// well-known Go net/http and context timeout markers; this is fragile
// in principle but stable in practice because Go's stdlib error shapes
// for these conditions have not changed in a decade. The classification
// only affects the counter the failure is bucketed into; total failure
// count is correct regardless.
func classifyHTTPError(err error) (timeout bool, nonOK bool) {
	if err == nil {
		return false, false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true, false
	}
	var ue *url.Error
	if errors.As(err, &ue) && ue.Timeout() {
		return true, false
	}
	msg := err.Error()
	if strings.Contains(msg, "Client.Timeout exceeded") || strings.Contains(msg, "context deadline exceeded") {
		return true, false
	}
	if strings.HasPrefix(msg, "http ") {
		return false, true
	}
	return false, false
}

func runHTTP(serverURL, tenant, apiKey, bearer string, spans, iters, warmup int) pathResult {
	client := &http.Client{
		Timeout: httpClientTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        4,
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     60 * time.Second,
			DisableCompression:  true,
		},
	}
	defer client.CloseIdleConnections()

	endpoint := serverURL + "/v1/traces"
	samples := make([]float64, 0, iters)
	failedSamples := make([]float64, 0)

	// send returns elapsed wall-clock for the attempt regardless of
	// whether the request ultimately succeeded — including the case
	// where Client.Do returns a timeout error before resp arrives. This
	// is the load-bearing change vs the previous implementation, which
	// returned 0 on error and silently dropped the iteration from
	// every downstream statistic.
	send := func() (time.Duration, error) {
		req := buildOTLPRequest(spans, "bench-http")
		body, err := proto.Marshal(req)
		if err != nil {
			return 0, fmt.Errorf("marshal: %w", err)
		}
		httpReq, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return 0, err
		}
		httpReq.Header.Set("Content-Type", "application/x-protobuf")
		httpReq.Header.Set("X-Tenant", tenant)
		if apiKey != "" {
			httpReq.Header.Set("X-Api-Key", apiKey)
		}
		if bearer != "" {
			httpReq.Header.Set("Authorization", "Bearer "+bearer)
		}
		start := time.Now()
		resp, err := client.Do(httpReq)
		elapsed := time.Since(start)
		if err != nil {
			return elapsed, err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return elapsed, fmt.Errorf("http %d", resp.StatusCode)
		}
		return elapsed, nil
	}

	for i := 0; i < warmup; i++ {
		_, _ = send()
	}
	// Wall-clock window starts AFTER warmup so the published throughput
	// reflects only the iters that contribute to the published samples.
	wallStart := time.Now()
	var fb benchstats.FailureBreakdown
	for i := 0; i < iters; i++ {
		d, err := send()
		if err != nil {
			fb.Last = err.Error()
			timeout, nonOK := classifyHTTPError(err)
			switch {
			case timeout:
				fb.Timeouts++
			case nonOK:
				fb.NonOK++
			default:
				fb.OtherErrors++
			}
			if d > 0 {
				failedSamples = append(failedSamples, float64(d.Microseconds())/1000.0)
			}
			continue
		}
		samples = append(samples, float64(d.Microseconds())/1000.0)
	}
	wallEnd := time.Now()
	fb.FailedSamples = failedSamples
	r := benchstats.SummarizeWithFailures("OTLP HTTP", samples, fb, spans)
	r.AttachWallClock(wallStart, wallEnd)
	return r
}

// ── gRPC ───────────────────────────────────────────────────────────────────

// grpcCallTimeout matches httpClientTimeout and serves the same role —
// the bench's per-request ceiling, intentionally near the upper end of
// real OTLP exporter defaults so the measurement extends slightly past
// where production exporters would give up while still capping the
// unbounded tail. Failures are counted (PathResult.Timeouts /
// PathResult.NonOK / PathResult.OtherErrors) and the actual attempt
// duration is captured (PathResult.FailedSamples) so nothing is
// silently censored.
const grpcCallTimeout = 30 * time.Second

// classifyGRPCError buckets a gRPC error into timeout vs non-OK vs
// other. gRPC's status.Code(codes.DeadlineExceeded) is returned when
// the per-call context.WithTimeout we set elapses; any other status
// code (Unavailable, ResourceExhausted, Unauthenticated, ...) counts
// as a non-OK server response, distinguishable from low-level
// dial/marshal errors which we bucket as OtherErrors.
func classifyGRPCError(err error) (timeout bool, nonOK bool) {
	if err == nil {
		return false, false
	}
	st, ok := status.FromError(err)
	if !ok {
		// Not a gRPC status error — treat as other (dial / marshal).
		if errors.Is(err, context.DeadlineExceeded) {
			return true, false
		}
		return false, false
	}
	if st.Code() == codes.DeadlineExceeded {
		return true, false
	}
	if st.Code() == codes.OK {
		return false, false
	}
	return false, true
}

func runGRPC(grpcAddr, tenant, apiKey, bearer string, spans, iters, warmup int) pathResult {
	dialCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(
		dialCtx, grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return pathResult{Path: "OTLP gRPC", Status: "FAIL_DIAL", Note: err.Error(), LastFailureMsg: err.Error(), SpansPerCall: spans}
	}
	defer conn.Close()
	client := collectortrace.NewTraceServiceClient(conn)

	mdPairs := []string{"x-tenant", tenant}
	if apiKey != "" {
		mdPairs = append(mdPairs, "x-api-key", apiKey)
	}
	if bearer != "" {
		mdPairs = append(mdPairs, "authorization", "Bearer "+bearer)
	}
	mdContext := metadata.AppendToOutgoingContext(context.Background(), mdPairs...)

	samples := make([]float64, 0, iters)
	failedSamples := make([]float64, 0)
	send := func() (time.Duration, error) {
		req := buildOTLPRequest(spans, "bench-grpc")
		ctx, c2 := context.WithTimeout(mdContext, grpcCallTimeout)
		defer c2()
		start := time.Now()
		_, err := client.Export(ctx, req)
		elapsed := time.Since(start)
		return elapsed, err
	}
	for i := 0; i < warmup; i++ {
		_, _ = send()
	}
	wallStart := time.Now()
	var fb benchstats.FailureBreakdown
	for i := 0; i < iters; i++ {
		d, err := send()
		if err != nil {
			fb.Last = err.Error()
			timeout, nonOK := classifyGRPCError(err)
			switch {
			case timeout:
				fb.Timeouts++
			case nonOK:
				fb.NonOK++
			default:
				fb.OtherErrors++
			}
			if d > 0 {
				failedSamples = append(failedSamples, float64(d.Microseconds())/1000.0)
			}
			continue
		}
		samples = append(samples, float64(d.Microseconds())/1000.0)
	}
	wallEnd := time.Now()
	fb.FailedSamples = failedSamples
	r := benchstats.SummarizeWithFailures("OTLP gRPC", samples, fb, spans)
	r.AttachWallClock(wallStart, wallEnd)
	return r
}

// ── AgenticUDP ─────────────────────────────────────────────────────────────

// connectUDPWithRetry dials and handshakes the AgenticUDP session up to N
// times. Loopback handshake packets are occasionally lost when a previous
// process recently closed its socket on the same address; one retry fixes
// it almost every time, three is plenty.
func connectUDPWithRetry(udpAddr, tenant string, ctx context.Context, attempts int) (*transport.Client, error) {
	var lastErr error
	for i := 0; i < attempts; i++ {
		cli, err := transport.NewClient(udpAddr, tenant)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if err := cli.Connect(ctx); err != nil {
			cli.Close()
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return cli, nil
	}
	return nil, lastErr
}

func runUDP(udpAddr, tenant string, spans, iters, warmup int, ackTimeout time.Duration, baseRetransmitMs int) pathResult {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli, err := connectUDPWithRetry(udpAddr, tenant, ctx, 3)
	if err != nil {
		return pathResult{Path: "AgenticUDP", Status: "FAIL_HANDSHAKE", Note: err.Error(), LastFailureMsg: err.Error(), SpansPerCall: spans}
	}
	defer cli.Close()

	// Optional override of the transport's base retransmit timeout. The
	// transport reads ENTROPYOPS_AGENT_UDP_RETRANSMIT_MS at construction
	// time, so this only takes effect when the operator explicitly sets
	// the flag. We deliberately leave 0 as a no-op (rather than e.g.
	// "use 2000") so the env var still works for callers who wired it
	// up that way; the bench flag is purely an override knob.
	if baseRetransmitMs > 0 {
		cli.SetBaseRTO(baseRetransmitMs)
	}

	samples := make([]float64, 0, iters)
	failedSamples := make([]float64, 0)

	// sendOne measures only the CLIENT-SIDE transmission time: from "call
	// SendCycle" to "SendCycle returns". This includes time blocked inside
	// waitForInflightWindow (the natural server-side backpressure channel:
	// new sends pause when maxInflight unACKed datagrams are outstanding)
	// but NOT the full ACK round-trip to durable storage.
	//
	// This matches production agent behaviour. The agent calls SendCycle in
	// a tight loop without waiting for ACKs between calls; ACKs flow back
	// asynchronously via ackLoop; the retransmit loop handles drops. Serial
	// ACK-gating (the old pattern: send → wait for all ACKs → send next)
	// serialises every iteration behind the server's SQLite write latency
	// (~3–5 s/flush under standard-baseline sustained load), making UDP
	// appear slower than HTTP/gRPC even when the protocol itself is faster.
	//
	// Per-iteration samples here represent "client hand-off latency" (what
	// the producer side experiences). The end-to-end reliable-delivery
	// throughput is captured by WallClockSeconds (wallStart → final ACK
	// drain below), which is the publishable system-level figure.
	sendOne := func() (time.Duration, bool, error) {
		traces := buildStorageTracesJSON(spans)
		sentBefore := cli.Stats().Sent
		start := time.Now()
		if err := cli.SendCycle(nil, traces, nil); err != nil {
			return time.Since(start), false, err
		}
		if cli.Stats().Sent == sentBefore {
			return time.Since(start), false, fmt.Errorf("no datagrams sent (payload may be empty)")
		}
		return time.Since(start), true, nil
	}

	// Warmup: fire N iterations without ACK-waiting, then drain so the
	// inflight map is empty before the timed window opens. Without this
	// drain, late warmup ACKs arriving during the main loop would inflate
	// ackedAtStart and cause the final ACK-drain check to under-count.
	for i := 0; i < warmup; i++ {
		_, _, _ = sendOne()
	}
	warmupSentTotal := cli.Stats().Sent
	warmupDrainDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(warmupDrainDeadline) {
		if cli.Stats().Acked >= warmupSentTotal {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	wallStart := time.Now()
	sentAtStart := cli.Stats().Sent
	ackedAtStart := cli.Stats().Acked
	var fb benchstats.FailureBreakdown
	for i := 0; i < iters; i++ {
		d, ok, err := sendOne()
		if err != nil {
			fb.Last = err.Error()
			fb.OtherErrors++
			if d > 0 {
				failedSamples = append(failedSamples, float64(d.Microseconds())/1000.0)
			}
			continue
		}
		if !ok {
			fb.Timeouts++
			if d > 0 {
				failedSamples = append(failedSamples, float64(d.Microseconds())/1000.0)
			}
			continue
		}
		samples = append(samples, float64(d.Microseconds())/1000.0)
	}

	// ACK drain: wait for every datagram sent during the main loop to be
	// acknowledged. wallEnd is set here — not at the end of the send loop —
	// so WallClockSeconds spans the full reliable-delivery window (first
	// send → last ACK confirmed), not just the client-side send window.
	// This is the number that honestly answers "how long did the full
	// guaranteed-delivery cycle take?" and is what spans/sec_wall reports.
	totalMainSent := cli.Stats().Sent - sentAtStart
	ackDrainDeadline := time.Now().Add(ackTimeout)
	for time.Now().Before(ackDrainDeadline) {
		if cli.Stats().Acked-ackedAtStart >= totalMainSent {
			break
		}
		time.Sleep(100 * time.Microsecond)
	}
	wallEnd := time.Now()

	// Count datagrams still unACKed after the drain as timeouts so
	// Fail% reflects the incomplete deliveries in the JSON artifact.
	unacked := int64(totalMainSent) - int64(cli.Stats().Acked-ackedAtStart)
	if unacked > 0 {
		fb.Timeouts += int(unacked)
		if fb.Last == "" {
			fb.Last = fmt.Sprintf("%d datagrams unACKed after %v drain", unacked, ackTimeout)
		}
	}

	fb.FailedSamples = failedSamples
	r := benchstats.SummarizeWithFailures("AgenticUDP", samples, fb, spans)

	// Promote AgenticUDP protocol counters from the old free-form Note
	// string to first-class JSON fields so the artifact is fully
	// introspectable (no need to regex-parse Note).
	finalStats := cli.Stats()
	r.UDPSent = finalStats.Sent
	r.UDPAcked = finalStats.Acked
	r.UDPRetransmits = finalStats.Retransmits
	r.UDPDropped = finalStats.Dropped
	r.AttachWallClock(wallStart, wallEnd)
	return r
}

// ── Payload builders ───────────────────────────────────────────────────────

// buildOTLPRequest constructs an OTLP ExportTraceServiceRequest with N
// spans, fresh random trace/span IDs every call so server-side dedup or
// trace caches do not skew results.
func buildOTLPRequest(n int, serviceName string) *collectortrace.ExportTraceServiceRequest {
	now := uint64(time.Now().UnixNano())
	end := now + 15_000_000 // +15ms span duration

	spans := make([]*tracepb.Span, n)
	for i := 0; i < n; i++ {
		spans[i] = &tracepb.Span{
			TraceId:           randBytes(16),
			SpanId:            randBytes(8),
			Name:              "bench-span",
			Kind:              tracepb.Span_SPAN_KIND_INTERNAL,
			StartTimeUnixNano: now,
			EndTimeUnixNano:   end,
		}
	}
	return &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key:   "service.name",
							Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: serviceName}},
						},
					},
				},
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Scope: &commonpb.InstrumentationScope{Name: "eo-ingest-bench"},
						Spans: spans,
					},
				},
			},
		},
	}
}

// buildStorageTracesJSON constructs the JSON shape AgenticUDP's "traces"
// signal expects (a []storage.Trace). Fields match
// entropyops-v2/internal/storage/interface.go Trace struct json tags.
func buildStorageTracesJSON(n int) []map[string]interface{} {
	now := time.Now().UTC()
	end := now.Add(15 * time.Millisecond)
	out := make([]map[string]interface{}, n)
	for i := 0; i < n; i++ {
		out[i] = map[string]interface{}{
			"trace_id":       hex.EncodeToString(randBytes(16)),
			"span_id":        hex.EncodeToString(randBytes(8)),
			"parent_span_id": "",
			"service_name":   "bench-udp",
			"operation_name": "bench-span",
			"start_time":     now.Format(time.RFC3339Nano),
			"end_time":       end.Format(time.RFC3339Nano),
			"duration_us":    int64(15_000),
			"status_code":    "OK",
			"span_kind":      "INTERNAL",
			"attributes":     map[string]string{},
		}
	}
	return out
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand should never fail in practice; fall back to deterministic
		// non-zero bytes so the dedup hash still varies per call (start time).
		for i := range b {
			b[i] = byte((time.Now().UnixNano() + int64(i)) & 0xff)
		}
	}
	return b
}

// ── Stats ──────────────────────────────────────────────────────────────────
//
// All percentile / mean / rounding logic lives in
// internal/benchstats so that eo-bench-merge — which concatenates
// per-iteration samples from multiple chunk files — produces
// statistically and byte-identical output to a single live run.
//
// The bench tool no longer wraps benchstats.Summarize directly; it
// uses benchstats.SummarizeWithFailures so the failure breakdown
// captured by each runXxx function is recorded on the PathResult
// alongside the success-sample percentiles. The merger continues to
// use benchstats.Summarize because it concatenates already-summarized
// success samples; its merge-equivalence test (stats_test.go) is
// unaffected.

// ── Output ─────────────────────────────────────────────────────────────────

func printTable(rs []pathResult) {
	// Two throughput columns by design:
	//   spans/sec_avg  = (spans_per_call * 1000) / avg_ms over SUCCESS samples.
	//                    Per-iter throughput; matches the long-standing field.
	//   spans/sec_wall = (attempted * spans_per_call) / wall_clock_seconds.
	//                    Operator-stopwatch throughput; INCLUDES failed iters
	//                    in the numerator. This is the publishable number.
	// They diverge under sustained load because failed iters consume wall
	// clock without contributing to AvgMs; printing both side-by-side
	// surfaces the divergence at a glance.
	fmt.Printf("%-12s %6s %8s %8s %8s %8s %14s %14s %14s %10s %8s %8s %s\n",
		"Path", "N", "p50_ms", "p90_ms", "p99_ms", "avg_ms", "ms/span_p50", "spans/sec_avg", "spans/sec_wall", "wall_s", "Fails", "Fail%", "Status")
	for _, r := range rs {
		fmt.Printf("%-12s %6d %8.2f %8.2f %8.2f %8.2f %14.3f %14.1f %14.1f %10.1f %8d %7.2f%% %s",
			r.Path, r.N, r.P50Ms, r.P90Ms, r.P99Ms, r.AvgMs, r.MsPerSpanP50, r.SpansPerSecond,
			r.SpansPerSecondWall, r.WallClockSeconds,
			r.Failures, r.FailureRatePct, r.Status)
		// Append the human-readable counter summary the old Note used
		// to provide. We synthesize it from the structured fields so
		// the table line is the same shape every run regardless of
		// whether failures occurred.
		extras := []string{}
		if r.Failures > 0 {
			extras = append(extras, fmt.Sprintf("attempted=%d", r.Attempted))
			if r.Timeouts > 0 {
				extras = append(extras, fmt.Sprintf("timeouts=%d", r.Timeouts))
			}
			if r.NonOK > 0 {
				extras = append(extras, fmt.Sprintf("non_ok=%d", r.NonOK))
			}
			if r.OtherErrors > 0 {
				extras = append(extras, fmt.Sprintf("other_errors=%d", r.OtherErrors))
			}
		}
		if r.UDPSent > 0 || r.UDPAcked > 0 {
			extras = append(extras, fmt.Sprintf("sent=%d acked=%d retx=%d dropped=%d",
				r.UDPSent, r.UDPAcked, r.UDPRetransmits, r.UDPDropped))
		}
		if len(extras) > 0 {
			fmt.Printf("  (%s)", strings.Join(extras, "; "))
		}
		if r.LastFailureMsg != "" {
			fmt.Printf("  last_err=%q", r.LastFailureMsg)
		} else if r.Note != "" {
			fmt.Printf("  note=%q", r.Note)
		}
		fmt.Println()
	}
}

func printFooter(rs []pathResult, benchMode string) {
	fmt.Println()
	fmt.Println("Interpretation:")
	fmt.Println("  - All three columns measure CLIENT wall-clock per request, with persistent connections.")
	fmt.Println("  - HTTP / gRPC: time to receive the protocol response.")
	fmt.Println("  - AgenticUDP: time until the server ACK is observed via Stats().Acked.")
	fmt.Println("  - ms/span normalises across batch sizes; lower is better.")
	fmt.Println()
	fmt.Println("WHAT THESE NUMBERS DO NOT TELL YOU — read before quoting them:")
	fmt.Println()
	switch benchMode {
	case "standard-baseline":
		fmt.Println("  (0) Server bench_mode = standard-baseline (DEFAULT, the realistic configuration).")
		fmt.Println("      The OTLP HTTP and gRPC handlers in this build write to the in-tree SQLite")
		fmt.Println("      backend with default PRAGMAs (FULL fsync, autocheckpoint=1000, no large mmap)")
		fmt.Println("      using a per-row INSERT loop. That mirrors what an off-the-shelf SQL-backed")
		fmt.Println("      OTLP receiver does — i.e. it represents the kind of stack a customer is")
		fmt.Println("      already running. AgenticUDP keeps its receiver-level coalescer + bulk INSERT")
		fmt.Println("      because that is part of the AgenticUDP product, not a backend tuning, and it")
		fmt.Println("      ships in any deployment that uses AgenticUDP.")
		fmt.Println()
		fmt.Println("      Caveat: this is still NOT the gold-standard comparison. A real customer's")
		fmt.Println("      OTLP backend is Jaeger / Tempo / Postgres / ClickHouse, not SQLite. To run")
		fmt.Println("      the gold-standard apples-to-apples bench (which points -server / -grpc at")
		fmt.Println("      an actual third-party OTLP collector), see")
		fmt.Println("      docs/operator/THIRD_PARTY_BACKEND_BENCH_PLAN.md.")
	case "tuned":
		fmt.Println("  (0) Server bench_mode = TUNED (ENTROPYOPS_STANDARD_BASELINE_MODE=false on the")
		fmt.Println("      server). All three paths share the same heavily tuned SQLite backend:")
		fmt.Println("      multi-row bulk INSERT, NORMAL fsync, out-of-band WAL checkpointer, large")
		fmt.Println("      mmap. This configuration is platform-self-test (e.g. is the WAL checkpointer")
		fmt.Println("      doing its job?). It does NOT represent any deployment a customer would")
		fmt.Println("      actually compare AgenticUDP against — their existing OTLP backend has none")
		fmt.Println("      of these optimizations applied to the HTTP/gRPC ingest path. Any 'all three")
		fmt.Println("      are roughly comparable' reading from this mode is an artifact, not a result.")
		fmt.Println("      Re-run with the env var unset (or set to true) for the realistic comparison.")
	default:
		fmt.Println("  (0) Server bench_mode = unknown. The probe to /api/ingest/transports failed or")
		fmt.Println("      the server is older than the bench-mode field. Re-run against a v2.15+")
		fmt.Println("      server, or assume standard-baseline if you didn't override the env var.")
	}
	fmt.Println()
	fmt.Println("  (1) The three server-side ingest handlers do NOT execute the same scope of work.")
	fmt.Println("      As of v2.15.x:")
	fmt.Println("        - OTLP HTTP traces handler: auth + parse + head/tail sampler + k8s-attribute")
	fmt.Println("          joiner + WriteTraces(PerRow) + 3 background fan-out goroutines.")
	fmt.Println("        - OTLP gRPC traces handler: auth + parse + WriteTraces(PerRow).")
	fmt.Println("          (No sampler, no k8s join.)")
	fmt.Println("        - AgenticUDP traces path: decode envelope + (coalesced) WriteTraces.")
	fmt.Println("          (No sampler, no k8s join.)")
	fmt.Println("      The HTTP handler does enrichment work the other two paths skip, so a 'win'")
	fmt.Println("      for gRPC or AgenticUDP here is partly a real protocol difference and partly")
	fmt.Println("      an artifact of that handler asymmetry. See docs/operator/HANDLER_PARITY_PLAN.md.")
	fmt.Println()
	fmt.Println("  (2) Single-host loopback. No real network, no NAT, no firewall, no L7 proxy.")
	fmt.Println("      WAN behaviour (where TCP head-of-line is most expensive and UDP's lack of")
	fmt.Println("      ordering matters most) is NOT exercised; WAN numbers will move the ranking.")
	fmt.Println()
	fmt.Println("  (3) Sample-size discipline. p50 (median) is robust at every N reported here.")
	fmt.Println("      Tail percentiles are not. Rule-of-thumb ladder for tail claims:")
	fmt.Println("        - N >= 10000 -> publishable p99 (9900th-ranked sample, neighbours 9899/9901")
	fmt.Println("                        for stability sanity check). p99.9 (9990th) starts to be")
	fmt.Println("                        meaningful -- this is the percentile that actually matters")
	fmt.Println("                        for SLO budgets at production volume.")
	fmt.Println("        - N >= 1000  -> defensible p99 (990th-ranked, neighbours 989/991). p99.9")
	fmt.Println("                        is one sample, do not interpret.")
	fmt.Println("        - N ~= 100   -> bare minimum to say anything about p99 with a straight face.")
	fmt.Println("        - N <= 20    -> p99 is the single worst iteration, p90 is the second worst.")
	fmt.Println("                        1 or 2 background outliers (GC pause, Windows fsync flush,")
	fmt.Println("                        antivirus scan, host contention) dominate. NOT publishable")
	fmt.Println("                        as a tail finding; use these iterations for medians only.")
	fmt.Println("      avg, max, and spans/sec inherit the same outlier sensitivity as p99, because")
	fmt.Println("      they are all computed from total elapsed time per request. If you can't make")
	fmt.Println("      a p99 claim at this N, you can't make an avg or spans/sec claim either.")
	fmt.Println()
	fmt.Println("  (4) Failures are counted, not censored. The Fails / Fail% columns and the")
	fmt.Println("      structured failures / timeouts / non_ok / other_errors fields in the JSON")
	fmt.Println("      apply to ALL three paths symmetrically. Earlier versions of this tool")
	fmt.Println("      silently dropped failed HTTP/gRPC iterations from the published percentiles")
	fmt.Println("      while exposing AgenticUDP timeouts loudly -- an asymmetry that systematically")
	fmt.Println("      understated HTTP/gRPC failures vs UDP failures in any side-by-side comparison.")
	fmt.Println("      That asymmetry is fixed: every path now reports its full attempt count, its")
	fmt.Println("      successful sample distribution, AND its failure breakdown. The censored-tail")
	fmt.Println("      iterations are recoverable via failed_samples_ms in the JSON when you pass")
	fmt.Println("      -keep-samples. PASS_WITH_FAILURES is the new status for a path that had any")
	fmt.Println("      successful samples but at least one failure; the percentile fields above")
	fmt.Println("      describe the SUCCESSFUL distribution only -- read Fail% beside them.")
	fmt.Println()
	fmt.Println("  (5) Two throughput columns, by design.")
	fmt.Println("      spans/sec_avg  is the per-iter throughput from the SUCCESS-only AvgMs.")
	fmt.Println("      spans/sec_wall is (attempted * spans_per_call) / wall_clock_seconds.")
	fmt.Println("                     Use this when quoting 'how long did the run actually take'.")
	fmt.Println("      Under sustained load they diverge: timed-out iters consume wall_seconds")
	fmt.Println("      but contribute zero to AvgMs, so spans/sec_avg can overstate real-world")
	fmt.Println("      throughput. spans/sec_wall is the publishable number.")
	fmt.Println()
	fmt.Println("  (6) UDP per-iteration timing is CLIENT SEND LATENCY, not round-trip latency.")
	fmt.Println("      HTTP / gRPC: the iter timer runs until the server's 2xx / RPC OK arrives,")
	fmt.Println("      which includes the durable storage write. p50_ms = write latency.")
	fmt.Println("      AgenticUDP: the iter timer runs until SendCycle returns, which includes")
	fmt.Println("      any backpressure from the inflight window (maxInflight cap) but NOT the")
	fmt.Println("      ACK round-trip. This matches the production agent — it fires SendCycle in")
	fmt.Println("      a loop without ACK-gating each call. The full reliable-delivery throughput")
	fmt.Println("      is in spans/sec_wall (wall_s covers first-send → last-ACK-drained).")
	fmt.Println("      Comparing p50_ms across protocols therefore answers different questions:")
	fmt.Println("        HTTP / gRPC p50 = 'how long does one durable write take?'")
	fmt.Println("        AgenticUDP p50  = 'how long does handing off one batch to the socket take?'")
	fmt.Println("      Use spans/sec_wall for the apples-to-apples system throughput comparison.")

	var best, bestPerSpan *pathResult
	for i := range rs {
		r := &rs[i]
		if r.Status != "PASS" && r.Status != "PASS_WITH_FAILURES" {
			continue
		}
		if best == nil || r.P50Ms < best.P50Ms {
			best = r
		}
		if bestPerSpan == nil || r.MsPerSpanP50 < bestPerSpan.MsPerSpanP50 {
			bestPerSpan = r
		}
	}
	fmt.Println()
	if best != nil {
		extra := ""
		if best.Failures > 0 {
			extra = fmt.Sprintf(" [WARN: %d/%d iterations failed (%.2f%%); p50 reflects the SUCCESSFUL %d only]",
				best.Failures, best.Attempted, best.FailureRatePct, best.N)
		}
		fmt.Printf("Best p50 (per request): %s (%.2f ms) — bench_mode=%s%s\n", best.Path, best.P50Ms, benchMode, extra)
	}
	if bestPerSpan != nil {
		extra := ""
		if bestPerSpan.Failures > 0 {
			extra = fmt.Sprintf(" [WARN: %d/%d iterations failed (%.2f%%); p50 reflects the SUCCESSFUL %d only]",
				bestPerSpan.Failures, bestPerSpan.Attempted, bestPerSpan.FailureRatePct, bestPerSpan.N)
		}
		fmt.Printf("Best p50 ms-per-span : %s (%.3f ms/span @ %d spans/call) — bench_mode=%s%s\n",
			bestPerSpan.Path, bestPerSpan.MsPerSpanP50, bestPerSpan.SpansPerCall, benchMode, extra)
	}
}

// probeBenchMode asks the server which write semantics the OTLP HTTP /
// gRPC handlers are using so the bench output is self-describing. We
// hit /api/ingest/transports because that endpoint already exists for
// preflight, requires no auth, and is cheap. Returns the mode string
// (currently "standard-baseline" or "tuned") or an error if the probe
// failed; callers should fall back to printing "unknown" rather than
// aborting the bench, since the probe is informational only.
func probeBenchMode(serverURL string) (string, error) {
	url := serverURL + "/api/ingest/transports"
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("http %d from %s", resp.StatusCode, url)
	}
	var body struct {
		BenchMode string `json:"bench_mode"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	if body.BenchMode == "" {
		return "", fmt.Errorf("server response missing bench_mode field (older server build?)")
	}
	return body.BenchMode, nil
}

func splitCSV(s string) []string {
	out := []string{}
	cur := ""
	for _, c := range s {
		if c == ',' || c == ' ' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
		} else {
			cur += string(c)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

// parsePathsFlag splits and validates the comma-separated -paths
// value. Allowed tokens: http, grpc, udp (case-insensitive). The
// empty string and any unknown token are errors so the operator
// finds out at flag-parse time instead of after the bench prints
// an empty results table and exits 0.
//
// Returning a fully-validated map keeps the call site in main()
// trivially correct: enabled["http"] / enabled["grpc"] /
// enabled["udp"] are the only keys that can ever be true.
func parsePathsFlag(s string) (map[string]bool, error) {
	tokens := splitCSV(strings.ToLower(s))
	if len(tokens) == 0 {
		return nil, fmt.Errorf("must include at least one of: http, grpc, udp (got empty value)")
	}
	allowed := map[string]bool{"http": true, "grpc": true, "udp": true}
	enabled := map[string]bool{}
	for _, t := range tokens {
		if !allowed[t] {
			return nil, fmt.Errorf("invalid path %q (allowed: http, grpc, udp)", t)
		}
		enabled[t] = true
	}
	return enabled, nil
}
