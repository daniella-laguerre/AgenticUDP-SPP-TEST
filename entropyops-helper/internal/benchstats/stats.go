// Package benchstats is the shared statistics + JSON schema used by both
// eo-ingest-bench (which produces per-path results from a live run) and
// eo-bench-merge (which concatenates per-iteration samples from multiple
// chunk runs and recomputes the same statistics).
//
// The package exists so the merger's output is byte-for-byte the same
// shape and computed with the same algorithm as a single live run. If
// a future refactor changes how p99 is computed in the bench, the
// merger inherits the change automatically — there is no second
// implementation to keep in sync.
//
// Why this matters: the n=10,000 follow-up at 5000 spans is too long
// to fit in a single VM session for many users. The chunked recipe
// (n=200 × 50, or n=1000 × 10) is the only way most operators can
// actually run it. For the chunked output to be cited in the same
// publishable claims as a single big run, the merger must produce a
// statistically identical artifact. Sharing the implementation is the
// cheapest way to guarantee that.
package benchstats

import (
	"math"
	"sort"
	"time"
)

// PathResult is the JSON shape eo-ingest-bench writes per path. The
// merger reads files containing this shape, concatenates the per-path
// Samples, and writes the same shape with recomputed statistics.
//
// Failure accounting is symmetric across HTTP, gRPC, and AgenticUDP.
// Earlier versions silently dropped failed iterations from the HTTP and
// gRPC sample distributions while AgenticUDP exposed timeouts loudly —
// an asymmetry rooted in the original n=20 cold-burst design where
// HTTP/gRPC never failed and only UDP had a meaningful failure mode.
// Under sustained load against the standard-baseline backend the
// failure-prone paths invert (HTTP/gRPC routinely exceed exporter
// timeouts; UDP retx stays at 0), so the asymmetric accounting was
// systematically understating HTTP/gRPC failures relative to UDP in
// any side-by-side comparison. The Failures / Timeouts / NonOK /
// OtherErrors / FailureRatePct / FailedSamples fields close that gap
// for every path. UDPSent / UDPAcked / UDPRetransmits / UDPDropped
// promote the previously-Note-encoded UDP protocol counters to
// first-class fields so the JSON is fully introspectable without
// parsing the Note string.
type PathResult struct {
	Path           string    `json:"path"`
	Status         string    `json:"status"`
	N              int       `json:"n"`
	SpansPerCall   int       `json:"spans_per_call"`
	P50Ms          float64   `json:"p50_ms"`
	P90Ms          float64   `json:"p90_ms"`
	P99Ms          float64   `json:"p99_ms"`
	AvgMs          float64   `json:"avg_ms"`
	MinMs          float64   `json:"min_ms"`
	MaxMs          float64   `json:"max_ms"`
	MsPerSpanP50   float64   `json:"ms_per_span_p50"`
	MsPerSpanP99   float64   `json:"ms_per_span_p99"`
	SpansPerSecond float64   `json:"spans_per_second"`
	Note           string    `json:"note,omitempty"`
	Samples        []float64 `json:"samples_ms,omitempty"`

	// Symmetric failure accounting (added so HTTP/gRPC failure rates
	// can be cited the same way UDP failure rates already could).
	// All omitempty so old chunk files (no failures recorded) decode
	// cleanly with these zero-valued.
	Attempted      int       `json:"attempted,omitempty"`         // N + Failures (sample-eligible iterations attempted, excluding warmup)
	Failures       int       `json:"failures,omitempty"`          // Timeouts + NonOK + OtherErrors
	Timeouts       int       `json:"timeouts,omitempty"`          // Subset of Failures: hit transport-level timeout (HTTP client.Timeout, gRPC context deadline, UDP ack deadline)
	NonOK          int       `json:"non_ok,omitempty"`            // Subset of Failures: server returned a non-2xx HTTP / non-OK gRPC status
	OtherErrors    int       `json:"other_errors,omitempty"`      // Subset of Failures: marshal/dial/encode errors before the response would have arrived
	FailureRatePct float64   `json:"failure_rate_pct,omitempty"`  // 100 * Failures / Attempted, rounded to 2 dp
	FailedSamples  []float64 `json:"failed_samples_ms,omitempty"` // Per-iteration wall-clock for FAILED iterations (when -keep-samples). Captured even on timeout/error so the censored tail is recoverable.
	LastFailureMsg string    `json:"last_failure_msg,omitempty"`  // Last error string seen on this path (parity with prior Note behavior; for human readability)

	// AgenticUDP protocol counters, promoted from the free-form Note
	// string they used to be encoded in. Zero on non-UDP paths.
	UDPSent        uint64 `json:"udp_sent,omitempty"`
	UDPAcked       uint64 `json:"udp_acked,omitempty"`
	UDPRetransmits uint64 `json:"udp_retransmits,omitempty"`
	UDPDropped     uint64 `json:"udp_dropped,omitempty"`

	// Wall-clock accounting. SpansPerSecond above is the per-iteration
	// throughput computed from AvgMs over SUCCESS samples only — useful
	// for "how fast is one request" comparisons but biased low when a
	// run has timeouts (the timed-out iter's wall-clock time IS spent
	// but is NOT in AvgMs). The fields below capture the raw stopwatch
	// the operator actually experienced:
	//
	//   StartedAt          : wall clock when the iter loop started
	//                        (after warmup, before iter 0).
	//   FinishedAt         : wall clock when the iter loop returned
	//                        (after the last iter's elapsed was recorded,
	//                        whether success or failure).
	//   WallClockSeconds   : FinishedAt - StartedAt, seconds.
	//   SpansPerSecondWall : (Attempted * SpansPerCall) / WallClockSeconds.
	//                        Counts ALL attempted iters in the numerator,
	//                        including failed ones, so the figure matches
	//                        a stopwatch held by an operator. This is the
	//                        publishable throughput number.
	//
	// All four are omitempty so older chunk files (no wall clock
	// captured) decode cleanly with these zero-valued; the merger then
	// sums WallClockSeconds across chunks and recomputes
	// SpansPerSecondWall from the merged Attempted total.
	StartedAt          time.Time `json:"started_at,omitempty"`
	FinishedAt         time.Time `json:"finished_at,omitempty"`
	WallClockSeconds   float64   `json:"wall_clock_seconds,omitempty"`
	SpansPerSecondWall float64   `json:"spans_per_second_wall_clock,omitempty"`
}

// FailureBreakdown is the input to SummarizeWithFailures. Callers
// (runHTTP / runGRPC / runUDP) tally each category as they iterate
// and pass the totals here. FailedSamples is the per-iteration
// wall-clock for the failed iterations (captured at the call site
// even on transport timeout, so the censored tail is recoverable
// for post-hoc analysis).
type FailureBreakdown struct {
	Timeouts      int
	NonOK         int
	OtherErrors   int
	FailedSamples []float64
	Last          string
}

// Total returns the total failure count across all categories.
func (f FailureBreakdown) Total() int {
	return f.Timeouts + f.NonOK + f.OtherErrors
}

// Summarize turns a raw per-iteration latency sample (in milliseconds)
// into a PathResult with all the percentile fields populated. Empty
// input returns a FAIL_NO_DATA result so callers can still emit it.
//
// The algorithm intentionally matches the original implementation in
// eo-ingest-bench so that summarize-after-merge is identical to
// summarize-after-single-run with the concatenated samples.
//
// Summarize does not record failures; it is preserved unchanged so
// the merger's invariant "Summarize over the concat equals Summarize
// over either chunk reassembled" continues to hold for the success-
// only sample distribution. Use SummarizeWithFailures from the live
// bench tool to also record failure accounting.
func Summarize(name string, samples []float64, spansPerCall int) PathResult {
	r := PathResult{Path: name, SpansPerCall: spansPerCall}
	if len(samples) == 0 {
		r.Status = "FAIL_NO_DATA"
		return r
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	p50 := Percentile(sorted, 50)
	p90 := Percentile(sorted, 90)
	p99 := Percentile(sorted, 99)
	avg := Mean(sorted)
	r.Status = "PASS"
	r.N = len(samples)
	r.P50Ms = Round2(p50)
	r.P90Ms = Round2(p90)
	r.P99Ms = Round2(p99)
	r.AvgMs = Round2(avg)
	r.MinMs = Round2(sorted[0])
	r.MaxMs = Round2(sorted[len(sorted)-1])
	r.MsPerSpanP50 = Round3(p50 / float64(spansPerCall))
	r.MsPerSpanP99 = Round3(p99 / float64(spansPerCall))
	if avg > 0 {
		r.SpansPerSecond = Round1(float64(spansPerCall) * 1000.0 / avg)
	}
	r.Samples = samples
	return r
}

// SummarizeWithFailures wraps Summarize and additionally records the
// failure breakdown captured by the call site during the iteration
// loop. The success-sample percentile / mean / spans-per-second
// fields are computed exactly the way Summarize computes them — i.e.
// from the successful samples only, the same way every percentile in
// the writeup has always been computed — but the result also exposes
// the count of failed iterations broken down by category, the
// fraction of attempted iterations that failed, and (when the caller
// retains them) the per-iteration wall-clock of the failed
// iterations.
//
// Status hierarchy:
//
//   - 0 successful samples + 0 failures             → FAIL_NO_DATA (unchanged)
//   - 0 successful samples + ≥1 failures            → FAIL_TIMEOUT (every attempt failed; pessimistic label since timeout is the most common cause of total failure under sustained load)
//   - ≥1 successful samples + 0 failures            → PASS (unchanged)
//   - ≥1 successful samples + ≥1 failures           → PASS_WITH_FAILURES (the new state for the standard-baseline-mode-under-load case)
//
// PASS_WITH_FAILURES is the load-bearing addition. Without it, an
// HTTP path that completed 600/1000 iterations and timed out on 400
// would have reported as PASS with n=600 and silently-censored
// percentiles — exactly the methodology asymmetry the writeup's TL;DR
// explicitly says it exists to avoid.
func SummarizeWithFailures(name string, okSamples []float64, fb FailureBreakdown, spansPerCall int) PathResult {
	r := Summarize(name, okSamples, spansPerCall)
	totalFailures := fb.Total()
	r.Attempted = r.N + totalFailures
	if totalFailures == 0 {
		// Successful run (or FAIL_NO_DATA for the empty case);
		// no failure metadata to attach.
		return r
	}
	r.Failures = totalFailures
	r.Timeouts = fb.Timeouts
	r.NonOK = fb.NonOK
	r.OtherErrors = fb.OtherErrors
	if r.Attempted > 0 {
		r.FailureRatePct = Round2(float64(totalFailures) * 100.0 / float64(r.Attempted))
	}
	if len(fb.FailedSamples) > 0 {
		r.FailedSamples = fb.FailedSamples
	}
	if fb.Last != "" {
		r.LastFailureMsg = fb.Last
	}
	switch {
	case r.N == 0:
		// Every attempt failed. Label as FAIL_TIMEOUT since under
		// sustained-load on standard-baseline that is the
		// overwhelmingly common cause; the per-category counters
		// disambiguate if it isn't.
		r.Status = "FAIL_TIMEOUT"
	default:
		r.Status = "PASS_WITH_FAILURES"
	}
	return r
}

// AttachWallClock records the operator-experienced stopwatch on a
// PathResult and recomputes SpansPerSecondWall from it. Called by the
// live bench once the iter loop has returned (wrapping warmup is the
// caller's choice — the bench currently excludes warmup so the field
// is comparable to the published AvgMs/SpansPerSecond which also
// exclude warmup). Called by the merger after summing per-chunk
// WallClockSeconds.
//
// Why the throughput formula uses Attempted (not just N):
//
//	A run that completed 600/1000 iters with 400 timeouts spent the
//	full wall clock attempting all 1000. The operator's stopwatch
//	reflects all 1000. Counting only the successful 600 in the
//	numerator would underreport the throughput operators actually
//	saw, which is exactly the asymmetry the schema's failure
//	accounting was added to remove. So the wall-clock throughput
//	uses the same denominator as AvgMs (success-only), but the
//	numerator includes every attempted iter.
//
// Empty (zero) start/end is a no-op so callers that don't capture
// timestamps (e.g. unit tests of pure stats logic) still produce
// stable output.
func (r *PathResult) AttachWallClock(start, end time.Time) {
	if start.IsZero() || end.IsZero() || !end.After(start) {
		return
	}
	dur := end.Sub(start).Seconds()
	if dur <= 0 {
		return
	}
	r.StartedAt = start.UTC()
	r.FinishedAt = end.UTC()
	r.WallClockSeconds = Round3(dur)
	attempted := r.Attempted
	if attempted == 0 {
		// Older callers that fill PathResult by hand may not have
		// set Attempted yet; fall back to N+Failures inline so the
		// throughput is still correct.
		attempted = r.N + r.Failures
	}
	if attempted > 0 && r.SpansPerCall > 0 {
		r.SpansPerSecondWall = Round1(float64(attempted) * float64(r.SpansPerCall) / dur)
	}
}

// SetWallClockSeconds sets the wall-clock total directly (for the
// merger, which sums per-chunk WallClockSeconds without having a
// single contiguous start/end timestamp pair) and recomputes
// SpansPerSecondWall. StartedAt/FinishedAt are NOT set — they are
// meaningful only for a single contiguous run.
func (r *PathResult) SetWallClockSeconds(seconds float64) {
	if seconds <= 0 {
		return
	}
	r.WallClockSeconds = Round3(seconds)
	attempted := r.Attempted
	if attempted == 0 {
		attempted = r.N + r.Failures
	}
	if attempted > 0 && r.SpansPerCall > 0 {
		r.SpansPerSecondWall = Round1(float64(attempted) * float64(r.SpansPerCall) / seconds)
	}
}

// Percentile returns the p-th percentile (0-100) of an already-sorted
// ascending slice using the canonical nearest-rank definition: the
// k-th smallest sample where k = ceil(p/100 * n).
//
// We compute k as ceil(p/100*n - epsilon) to dodge a floating-point
// surprise: 99.9 / 100 * 10000 evaluates to 9990.000000000001 in
// float64, and a naive ceil bumps it to 9991. The epsilon (1e-9)
// nudges that back to 9990 so the function returns the documented
// rank. Every "nice" percentile (p50/p90/p99 of small n; integer
// products) lands on an integer-distance from k anyway, so the
// epsilon does not perturb them — verified by stats_test.go.
//
// Documented ranks the writeup and the eo-ingest-bench footer cite:
//
//   - p50 of n=20    = 10th-ranked
//   - p99 of n=100   = 99th-ranked
//   - p99 of n=1000  = 990th-ranked
//   - p99 of n=10000 = 9900th-ranked
//   - p99.9 of n=10000 = 9990th-ranked
func Percentile(sortedAsc []float64, p float64) float64 {
	if len(sortedAsc) == 0 {
		return 0
	}
	const eps = 1e-9
	idx := int(math.Ceil(p/100.0*float64(len(sortedAsc))-eps)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sortedAsc) {
		idx = len(sortedAsc) - 1
	}
	return sortedAsc[idx]
}

// Mean is the arithmetic mean of the sample. NaN-free guard: empty
// input returns 0 so callers can still emit a result.
func Mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

// Round2 / Round3 / Round1 keep the JSON output identical between a
// single live run and a merged run — without these the merged file
// would differ in the last decimal place and diff tools would flag
// every line.
func Round2(v float64) float64 { return math.Round(v*100) / 100 }
func Round3(v float64) float64 { return math.Round(v*1000) / 1000 }
func Round1(v float64) float64 { return math.Round(v*10) / 10 }
