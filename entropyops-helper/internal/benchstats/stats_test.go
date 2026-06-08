package benchstats

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

// TestPercentile_DocumentedEdges pins the exact percentile indexing
// the eo-ingest-bench footer documents (and the writeup cites). If a
// future refactor changes the indexing without updating the docs,
// this test fails before users find the discrepancy.
func TestPercentile_DocumentedEdges(t *testing.T) {
	// Sorted ascending 1..100. p99 = 99 (the 99th-ranked value, idx 98),
	// p50 = 50 (idx 49), p100 = 100 (idx 99).
	xs := make([]float64, 100)
	for i := range xs {
		xs[i] = float64(i + 1)
	}
	cases := []struct {
		p    float64
		want float64
	}{
		{50, 50},
		{90, 90},
		{99, 99},
		{100, 100},
		{0.1, 1}, // first sample
	}
	for _, c := range cases {
		got := Percentile(xs, c.p)
		if got != c.want {
			t.Errorf("Percentile(1..100, p=%v) = %v, want %v", c.p, got, c.want)
		}
	}

	// Documented quote in the writeup: "n=10000 → p99 ~= 9900th-ranked
	// sample". Verify directly.
	big := make([]float64, 10000)
	for i := range big {
		big[i] = float64(i + 1)
	}
	if got := Percentile(big, 99); got != 9900 {
		t.Errorf("p99 of 1..10000 should be the 9900th-ranked sample (=9900), got %v", got)
	}
	if got := Percentile(big, 99.9); got != 9990 {
		t.Errorf("p99.9 of 1..10000 should be the 9990th-ranked sample (=9990), got %v", got)
	}
}

// TestSummarize_MergeEquivalence is the load-bearing invariant for
// eo-bench-merge. The merger concatenates per-iteration samples from
// multiple chunk files and re-runs Summarize on the concatenation;
// the merged JSON is published as if it were a single live run with
// the same total n. For that to be a true claim, Summarize on the
// concat of two samples must equal what Summarize would produce on a
// single sample with the same total contents.
//
// This is trivially true for the current implementation (Summarize is
// a pure function of the multiset of samples), but a future
// "optimization" — e.g. streaming percentiles, hyper-loglog,
// reservoir sampling — would silently break the merger. This test
// catches that.
func TestSummarize_MergeEquivalence(t *testing.T) {
	rng := rand.New(rand.NewSource(2026))

	// Two chunks of 1000 samples each, mimicking the AgenticUDP-shaped
	// bimodal distribution: ~95% near 200ms, ~5% in the 5-7s range.
	gen := func(n int) []float64 {
		out := make([]float64, n)
		for i := range out {
			if rng.Float64() < 0.05 {
				out[i] = 5000 + rng.Float64()*2000
			} else {
				out[i] = 180 + rng.Float64()*40
			}
		}
		return out
	}
	chunkA := gen(1000)
	chunkB := gen(1000)

	concat := append([]float64(nil), chunkA...)
	concat = append(concat, chunkB...)

	wantSingleRun := Summarize("AgenticUDP", concat, 5000)

	// Now mimic the merger: summarize each chunk (as a live run
	// would), then concat the per-iteration samples and re-summarize.
	// The bench tool keeps the raw samples on the result when
	// -keep-samples is set; the merger reads them back and re-runs
	// Summarize on the concat. So we should be able to round-trip.
	chunk1Result := Summarize("AgenticUDP", chunkA, 5000)
	chunk2Result := Summarize("AgenticUDP", chunkB, 5000)
	mergedSamples := append([]float64(nil), chunk1Result.Samples...)
	mergedSamples = append(mergedSamples, chunk2Result.Samples...)
	gotMerged := Summarize("AgenticUDP", mergedSamples, 5000)

	// Compare every published statistic. We don't compare Samples
	// directly because order doesn't matter for a multiset and the
	// concat order is deterministic anyway; we compare the things
	// the writeup actually cites.
	wantSingleRun.Samples = nil
	gotMerged.Samples = nil
	if !reflect.DeepEqual(wantSingleRun, gotMerged) {
		t.Errorf("merge equivalence broken:\nsingle-run: %+v\nmerged:     %+v",
			wantSingleRun, gotMerged)
	}

	// Sanity: the merged n should be the sum of chunk n's. If a
	// future refactor caps n or downsamples, this catches it.
	if gotMerged.N != len(chunkA)+len(chunkB) {
		t.Errorf("merged N = %d, want %d (no downsampling expected)", gotMerged.N, len(chunkA)+len(chunkB))
	}
}

// TestSummarize_SpansPerSecondFromAvg locks in the spans/sec formula
// (spans_per_call * 1000 / avg_ms). This is the formula cited in the
// writeup's "throughput claim" section and the footer's caveat (3),
// and it's how spans/sec inherits the same outlier sensitivity as
// avg — a property the writeup relies on when bounding the
// throughput claim to 200 and 1000 spans only.
func TestSummarize_SpansPerSecondFromAvg(t *testing.T) {
	samples := []float64{100, 100, 100, 100, 100} // avg = 100ms
	r := Summarize("test", samples, 200)
	want := Round1(200.0 * 1000.0 / 100.0) // = 2000
	if r.SpansPerSecond != want {
		t.Errorf("spans/sec = %v, want %v (formula: spans*1000/avg_ms)", r.SpansPerSecond, want)
	}
	// Verify outlier sensitivity: replace one sample with a 5s outlier.
	// New avg = (100*4 + 5000)/5 = 1080. spans/sec drops dramatically.
	samples[4] = 5000
	r2 := Summarize("test", samples, 200)
	wantOutlier := Round1(200.0 * 1000.0 / 1080.0)
	if r2.SpansPerSecond != wantOutlier {
		t.Errorf("spans/sec under outlier = %v, want %v", r2.SpansPerSecond, wantOutlier)
	}
	if r2.SpansPerSecond >= r.SpansPerSecond {
		t.Errorf("expected outlier to drag spans/sec down, got before=%v after=%v", r.SpansPerSecond, r2.SpansPerSecond)
	}
}

// TestSummarize_EmptyInput exercises the FAIL_NO_DATA fast-path so
// the merger can rely on Summarize never panicking on degenerate
// chunks.
func TestSummarize_EmptyInput(t *testing.T) {
	r := Summarize("nothing", nil, 1000)
	if r.Status != "FAIL_NO_DATA" {
		t.Errorf("expected FAIL_NO_DATA on empty samples, got %q", r.Status)
	}
	if r.N != 0 || r.P50Ms != 0 || r.AvgMs != 0 {
		t.Errorf("expected zero stats on empty samples, got %+v", r)
	}
}

// TestSummarizeWithFailures_Statuses locks in the status hierarchy
// the bench tool, the merger, and the writeup all rely on. PASS
// continues to mean "every iteration succeeded"; PASS_WITH_FAILURES
// is the new state that catches the standard-baseline-mode case
// where some HTTP/gRPC iterations time out under sustained load
// while some succeed. FAIL_TIMEOUT is reserved for the case where
// EVERY iteration failed (i.e. zero successful samples), and
// FAIL_NO_DATA is preserved as the empty-input sentinel.
//
// Without these bands, an HTTP run that completed 600/1000 and
// timed out on 400 would still report as PASS with n=600 and
// silently-censored percentiles — exactly the methodology
// asymmetry the writeup explicitly says it exists to avoid.
func TestSummarizeWithFailures_Statuses(t *testing.T) {
	cases := []struct {
		name       string
		samples    []float64
		fb         FailureBreakdown
		wantStatus string
		wantN      int
		wantFails  int
		wantPct    float64
	}{
		{
			name:       "all_success_no_failures",
			samples:    []float64{100, 110, 120},
			fb:         FailureBreakdown{},
			wantStatus: "PASS",
			wantN:      3,
			wantFails:  0,
			wantPct:    0,
		},
		{
			name:       "some_success_some_timeouts_is_pass_with_failures",
			samples:    []float64{100, 110, 120, 130, 140, 150},
			fb:         FailureBreakdown{Timeouts: 4},
			wantStatus: "PASS_WITH_FAILURES",
			wantN:      6,
			wantFails:  4,
			wantPct:    40, // 4 / 10 attempted
		},
		{
			name:       "some_success_mixed_failure_categories",
			samples:    []float64{100, 200},
			fb:         FailureBreakdown{Timeouts: 1, NonOK: 2, OtherErrors: 5},
			wantStatus: "PASS_WITH_FAILURES",
			wantN:      2,
			wantFails:  8,
			wantPct:    80, // 8 / 10 attempted
		},
		{
			name:       "all_iterations_failed_is_fail_timeout",
			samples:    nil,
			fb:         FailureBreakdown{Timeouts: 5, NonOK: 3, OtherErrors: 2},
			wantStatus: "FAIL_TIMEOUT",
			wantN:      0,
			wantFails:  10,
			wantPct:    100,
		},
		{
			name:       "empty_input_no_failures_is_fail_no_data",
			samples:    nil,
			fb:         FailureBreakdown{},
			wantStatus: "FAIL_NO_DATA",
			wantN:      0,
			wantFails:  0,
			wantPct:    0,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := SummarizeWithFailures("test", c.samples, c.fb, 1000)
			if r.Status != c.wantStatus {
				t.Errorf("status = %q, want %q", r.Status, c.wantStatus)
			}
			if r.N != c.wantN {
				t.Errorf("N = %d, want %d", r.N, c.wantN)
			}
			if r.Failures != c.wantFails {
				t.Errorf("Failures = %d, want %d", r.Failures, c.wantFails)
			}
			if r.FailureRatePct != c.wantPct {
				t.Errorf("FailureRatePct = %v, want %v", r.FailureRatePct, c.wantPct)
			}
			if c.wantFails > 0 {
				if r.Attempted != c.wantN+c.wantFails {
					t.Errorf("Attempted = %d, want %d (N + Failures)", r.Attempted, c.wantN+c.wantFails)
				}
			}
		})
	}
}

// TestSummarizeWithFailures_PercentilesIgnoreFailedSamples is the
// core honesty invariant: the percentile fields ALWAYS describe the
// successful-sample distribution, never the failed-sample
// distribution. The writeup cites p50/p90/p99 as wall-clock per
// successful request; the failed-sample distribution is published
// separately (failed_samples_ms when -keep-samples) so a reader who
// wants the censored-tail story has the raw data, but the headline
// percentiles never silently include failed iterations.
//
// This is the inverse of the original asymmetric bug: that bug
// silently dropped failed iterations from the comparison; the fix
// keeps them visible in failed_samples_ms but does not blend them
// into the success percentile so two runs with different failure
// rates can still be compared on the SAME statistic (success-only
// p99) while the failure rates are reported alongside.
func TestSummarizeWithFailures_PercentilesIgnoreFailedSamples(t *testing.T) {
	successSamples := []float64{100, 100, 100, 100, 100} // p50=100, p99=100
	fb := FailureBreakdown{
		Timeouts:      3,
		FailedSamples: []float64{30000, 30000, 30000}, // would heavily skew if blended
	}
	r := SummarizeWithFailures("test", successSamples, fb, 1000)
	if r.P50Ms != 100 {
		t.Errorf("p50 should reflect success-only samples (=100ms), got %v", r.P50Ms)
	}
	if r.P99Ms != 100 {
		t.Errorf("p99 should reflect success-only samples (=100ms), got %v", r.P99Ms)
	}
	if r.AvgMs != 100 {
		t.Errorf("avg should reflect success-only samples (=100ms), got %v", r.AvgMs)
	}
	if len(r.FailedSamples) != 3 {
		t.Errorf("failed_samples should be preserved separately for post-hoc analysis, got len=%d", len(r.FailedSamples))
	}
	if r.Status != "PASS_WITH_FAILURES" {
		t.Errorf("status = %q, want PASS_WITH_FAILURES", r.Status)
	}
}

// TestSummarizeWithFailures_OldDataDecodesCleanly makes sure that
// chunk files written by the OLD bench tool (which did not record
// any failure metadata) continue to merge cleanly under the new
// schema. omitempty + zero-valued failure fields means an old chunk
// looks like a no-failure run when re-read, which is the right
// answer — the old tool literally couldn't tell us whether failures
// occurred, so assuming zero is the conservative default.
func TestSummarizeWithFailures_OldDataDecodesCleanly(t *testing.T) {
	r := SummarizeWithFailures("test", []float64{100, 200, 300}, FailureBreakdown{}, 1000)
	if r.Status != "PASS" {
		t.Errorf("zero failures must map to PASS (preserves old behavior), got %q", r.Status)
	}
	if r.Failures != 0 || r.Timeouts != 0 || r.NonOK != 0 || r.OtherErrors != 0 {
		t.Errorf("zero failure breakdown must produce zero counters, got %+v", r)
	}
	// Attempted is also zero when no failures were attempted-and-
	// counted; this is intentional. The bench tool's iters input
	// equals N in that case so the operator can recover Attempted
	// from the existing iterations field if needed.
	if r.Attempted != 3 {
		t.Errorf("Attempted should equal N when there are no failures, got %d (want 3)", r.Attempted)
	}
}

// TestRound_Stability locks in that the rounding helpers produce
// values that are stable under JSON round-trip (no floating-point
// surprise like 0.30000000000000004). The merged JSON has to be
// diffable against a single live run's JSON for the chunked recipe
// to produce a publishable artifact.
func TestRound_Stability(t *testing.T) {
	cases := []struct {
		round func(float64) float64
		in    float64
		want  float64
	}{
		{Round2, 1.234567, 1.23},
		{Round2, 1.235, 1.24}, // ties to even is NOT what math.Round does — it rounds half-away-from-zero
		{Round3, 1.2345, 1.235},
		{Round1, 9999.66, 9999.7},
	}
	for _, c := range cases {
		got := c.round(c.in)
		if math.Abs(got-c.want) > 1e-9 {
			t.Errorf("round(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestAttachWallClock_HealthyRun pins the documented relationship
// between WallClockSeconds, Attempted, SpansPerCall, and the derived
// SpansPerSecondWall: it must be (Attempted * SpansPerCall) /
// WallClockSeconds. This is the field we publish as the
// stopwatch-equivalent throughput, so its formula is load-bearing.
func TestAttachWallClock_HealthyRun(t *testing.T) {
	r := SummarizeWithFailures("test", []float64{100, 200, 300}, FailureBreakdown{}, 1000)
	start := time.Date(2026, 5, 1, 3, 24, 11, 0, time.UTC)
	end := start.Add(2 * time.Second)
	r.AttachWallClock(start, end)

	if r.WallClockSeconds != 2.0 {
		t.Errorf("WallClockSeconds = %v, want 2.0", r.WallClockSeconds)
	}
	// 3 attempted iters * 1000 spans/iter / 2 seconds = 1500 spans/sec
	if r.SpansPerSecondWall != 1500.0 {
		t.Errorf("SpansPerSecondWall = %v, want 1500.0 (= 3 attempted * 1000 spans / 2s)", r.SpansPerSecondWall)
	}
	if r.StartedAt.IsZero() || r.FinishedAt.IsZero() {
		t.Errorf("StartedAt/FinishedAt should be populated, got %v / %v", r.StartedAt, r.FinishedAt)
	}
}

// TestAttachWallClock_FailedItersAreInThroughputNumerator is the
// load-bearing test for the bug the n=1000 thread surfaced. A run
// where 60% of iters succeeded fast and 40% timed out slow MUST
// report a wall-clock throughput that includes ALL 100% in the
// numerator — otherwise the published number understates the wall
// clock the operator actually experienced (the very asymmetry the
// failure-accounting fix removed for the per-iter throughput).
func TestAttachWallClock_FailedItersAreInThroughputNumerator(t *testing.T) {
	// 6 success samples (avg 100ms each) + 4 timeouts. Attempted = 10.
	okSamples := []float64{100, 100, 100, 100, 100, 100}
	fb := FailureBreakdown{
		Timeouts:      4,
		FailedSamples: []float64{60000, 60000, 60000, 60000}, // 4 iters that hit the 60s ACK timeout
	}
	r := SummarizeWithFailures("test", okSamples, fb, 5000)

	// Wall clock = sum of per-iter wall clocks, success + failed:
	// 6 * 0.1s + 4 * 60s = 0.6 + 240 = 240.6 seconds.
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Duration(240.6 * float64(time.Second)))
	r.AttachWallClock(start, end)

	// SpansPerSecondWall must use Attempted (10), NOT just N (6).
	// Expected: 10 * 5000 / 240.6 ≈ 207.8 spans/sec.
	wantWall := 207.8
	if math.Abs(r.SpansPerSecondWall-wantWall) > 0.5 {
		t.Errorf("SpansPerSecondWall = %v, want ~%v (= 10 attempted * 5000 / 240.6s)", r.SpansPerSecondWall, wantWall)
	}

	// Sanity: the success-only SpansPerSecond field should differ
	// from SpansPerSecondWall in the same direction the bug
	// produced. (5000 spans / 100ms avg = 50,000 spans/sec
	// per-iter, which is dramatically higher than 207.8.) The
	// gap between the two is the artifact we want operators to
	// see; this test pins that the gap exists.
	if r.SpansPerSecond <= r.SpansPerSecondWall*2 {
		t.Errorf("SpansPerSecond (%v) should be >> SpansPerSecondWall (%v) when failures dominate; the gap is the diagnostic signal we want visible",
			r.SpansPerSecond, r.SpansPerSecondWall)
	}
}

// TestAttachWallClock_RejectsZeroOrInverted ensures pathological
// inputs (zero timestamps, end-before-start, equal start/end) are
// no-ops rather than producing nonsensical infinities or negative
// throughputs in the JSON. The test also verifies that the function
// is idempotent in the no-op case (calling it twice with bad input
// doesn't corrupt prior good values).
func TestAttachWallClock_RejectsZeroOrInverted(t *testing.T) {
	r := SummarizeWithFailures("test", []float64{100}, FailureBreakdown{}, 1000)

	// First populate it with a valid wall clock.
	good := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	r.AttachWallClock(good, good.Add(time.Second))
	wantWall := 1.0
	wantThroughput := r.SpansPerSecondWall

	// Now try several bad inputs; each must be a no-op.
	bad := []struct {
		name       string
		start, end time.Time
	}{
		{"zero start", time.Time{}, good},
		{"zero end", good, time.Time{}},
		{"end == start", good, good},
		{"end before start", good.Add(time.Second), good},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			r.AttachWallClock(c.start, c.end)
			if r.WallClockSeconds != wantWall {
				t.Errorf("bad input %q overwrote good WallClockSeconds: got %v, want %v", c.name, r.WallClockSeconds, wantWall)
			}
			if r.SpansPerSecondWall != wantThroughput {
				t.Errorf("bad input %q overwrote good SpansPerSecondWall: got %v, want %v", c.name, r.SpansPerSecondWall, wantThroughput)
			}
		})
	}
}

// TestSetWallClockSeconds_MergerPath is the merger's contract: it
// receives a summed wall clock from N chunks and recomputes the
// throughput WITHOUT setting StartedAt/FinishedAt (those are
// meaningful only for a single contiguous run, not a chunked one).
func TestSetWallClockSeconds_MergerPath(t *testing.T) {
	r := SummarizeWithFailures("test", []float64{100, 100, 100, 100}, FailureBreakdown{Timeouts: 1, FailedSamples: []float64{60000}}, 5000)
	// Attempted should be 5 (4 success + 1 timeout). Operator
	// summed two chunks' wall clocks: chunk-A=10s, chunk-B=15s
	// → 25s total.
	r.SetWallClockSeconds(25.0)
	if r.WallClockSeconds != 25.0 {
		t.Errorf("WallClockSeconds = %v, want 25.0", r.WallClockSeconds)
	}
	// 5 attempted * 5000 spans / 25s = 1000 spans/sec
	if r.SpansPerSecondWall != 1000.0 {
		t.Errorf("SpansPerSecondWall = %v, want 1000.0", r.SpansPerSecondWall)
	}
	if !r.StartedAt.IsZero() || !r.FinishedAt.IsZero() {
		t.Errorf("merger path must NOT set StartedAt/FinishedAt: got %v / %v", r.StartedAt, r.FinishedAt)
	}
}

// TestSetWallClockSeconds_RejectsNonPositive is the safety net
// for the merger encountering chunk files where wall_clock_seconds
// was missing (older bench builds). Sum stays zero, no-op, no
// divide-by-zero or infinity in the JSON.
func TestSetWallClockSeconds_RejectsNonPositive(t *testing.T) {
	r := SummarizeWithFailures("test", []float64{100}, FailureBreakdown{}, 1000)
	for _, v := range []float64{0, -1, math.Inf(-1)} {
		r.SetWallClockSeconds(v)
		if r.WallClockSeconds != 0 {
			t.Errorf("SetWallClockSeconds(%v) should be a no-op; got WallClockSeconds=%v", v, r.WallClockSeconds)
		}
		if r.SpansPerSecondWall != 0 {
			t.Errorf("SetWallClockSeconds(%v) should not set SpansPerSecondWall; got %v", v, r.SpansPerSecondWall)
		}
	}
}
