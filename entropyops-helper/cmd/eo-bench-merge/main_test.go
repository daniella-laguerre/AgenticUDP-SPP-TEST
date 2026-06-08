package main

import (
	"testing"

	"github.com/entropyops/entropyops-helper/internal/benchstats"
)

// TestIsCrossPathIsolated_True is the canonical "isolated per-
// path" run protocol the writeup recommends for headline cross-
// path numbers: bring server up with fresh ./data, run
// -paths http, tear down, repeat for grpc and udp. Each chunk
// file then has results for exactly one path and no two chunks
// share a path.
func TestIsCrossPathIsolated_True(t *testing.T) {
	chunks := []chunkFile{
		{Results: []benchstats.PathResult{{Path: "OTLP HTTP"}}},
		{Results: []benchstats.PathResult{{Path: "OTLP gRPC"}}},
		{Results: []benchstats.PathResult{{Path: "AgenticUDP"}}},
	}
	if !isCrossPathIsolated(chunks) {
		t.Fatal("three single-path chunks with disjoint paths must be classified as isolated")
	}
}

// TestIsCrossPathIsolated_FalseSinglePath is the chunked-same-
// path use case the merger was originally built for (n=10 × 1000
// HTTP-only chunks merged into one n=10000 HTTP result). This
// must NOT be detected as isolated, because the per-path warning
// "path X present in N/M chunks" is a real methodology check
// here — if one chunk silently lacks a path the merge is wrong.
func TestIsCrossPathIsolated_FalseSamePathChunked(t *testing.T) {
	chunks := []chunkFile{
		{Results: []benchstats.PathResult{{Path: "OTLP HTTP"}}},
		{Results: []benchstats.PathResult{{Path: "OTLP HTTP"}}},
		{Results: []benchstats.PathResult{{Path: "OTLP HTTP"}}},
	}
	if isCrossPathIsolated(chunks) {
		t.Fatal("three same-path chunks must NOT be classified as isolated (this is the chunked-same-path mode, not cross-path isolated)")
	}
}

// TestIsCrossPathIsolated_FalseAllInOne covers the single-
// invocation default (one chunk with all three paths). Calling
// the merger on this is unusual but legal; it must not be called
// "isolated" because the original cross-path comparison story
// (one server's DB warmed by HTTP before gRPC etc.) still
// applies.
func TestIsCrossPathIsolated_FalseAllInOne(t *testing.T) {
	chunks := []chunkFile{
		{Results: []benchstats.PathResult{
			{Path: "OTLP HTTP"}, {Path: "OTLP gRPC"}, {Path: "AgenticUDP"},
		}},
	}
	if isCrossPathIsolated(chunks) {
		t.Fatal("a single chunk with all paths is not isolated by definition (need ≥2 chunks)")
	}
}

// TestIsCrossPathIsolated_FalseEmptyChunk catches a degenerate
// input shape: a chunk with zero results contributes no paths,
// which is suspicious and should NOT silently flip the merge
// into isolated mode.
func TestIsCrossPathIsolated_FalseEmptyChunk(t *testing.T) {
	chunks := []chunkFile{
		{Results: []benchstats.PathResult{{Path: "OTLP HTTP"}}},
		{Results: []benchstats.PathResult{}},
	}
	if isCrossPathIsolated(chunks) {
		t.Fatal("a chunk with zero results must disqualify the merge from isolated mode")
	}
}

// TestIsCrossPathIsolated_FalseOverlap covers the partial-
// overlap case: chunk A has HTTP+gRPC, chunk B has gRPC+UDP.
// gRPC appears in both → not isolated. The cross-path comparison
// is still tainted because gRPC's measurement window saw two
// different DB-warmth states.
func TestIsCrossPathIsolated_FalseOverlap(t *testing.T) {
	chunks := []chunkFile{
		{Results: []benchstats.PathResult{{Path: "OTLP HTTP"}, {Path: "OTLP gRPC"}}},
		{Results: []benchstats.PathResult{{Path: "OTLP gRPC"}, {Path: "AgenticUDP"}}},
	}
	if isCrossPathIsolated(chunks) {
		t.Fatal("overlapping path between chunks must NOT be classified as isolated")
	}
}

// TestMergeChunks_CrossPathIsolated is the end-to-end test for
// Option B: three single-path chunks (HTTP, gRPC, UDP) each
// with their own samples, merged into one multi-path JSON. Asserts
// (a) the merge succeeds with zero warning notes (so -strict=true
// doesn't escalate to fatal), (b) all three paths appear in the
// output with the correct n, and (c) the merged percentile math
// for each path matches a single-chunk Summarize over the same
// samples (i.e. concatenation of one chunk is a no-op).
func TestMergeChunks_CrossPathIsolated(t *testing.T) {
	httpSamples := []float64{100, 110, 120, 130, 140}
	grpcSamples := []float64{200, 210, 220, 230, 240, 250}
	udpSamples := []float64{4, 5, 6, 7, 8, 9, 10}

	chunks := []chunkFile{
		{
			SpansPerCall: 5000,
			Iterations:   5,
			Tenant:       "Trading-dev",
			BenchMode:    "standard-baseline",
			Results: []benchstats.PathResult{
				{Path: "OTLP HTTP", N: len(httpSamples), Status: "PASS", SpansPerCall: 5000, Samples: httpSamples},
			},
		},
		{
			SpansPerCall: 5000,
			Iterations:   6,
			Tenant:       "Trading-dev",
			BenchMode:    "standard-baseline",
			Results: []benchstats.PathResult{
				{Path: "OTLP gRPC", N: len(grpcSamples), Status: "PASS", SpansPerCall: 5000, Samples: grpcSamples},
			},
		},
		{
			SpansPerCall: 5000,
			Iterations:   7,
			Tenant:       "Trading-dev",
			BenchMode:    "standard-baseline",
			Results: []benchstats.PathResult{
				{Path: "AgenticUDP", N: len(udpSamples), Status: "PASS", SpansPerCall: 5000, Samples: udpSamples,
					UDPSent: 7, UDPAcked: 7},
			},
		},
	}
	files := []string{"http.json", "grpc.json", "udp.json"}
	hashes := []string{"abc", "def", "ghi"}

	merged, notes, fatal := mergeChunks(files, chunks, hashes)
	if fatal != nil {
		t.Fatalf("merge unexpectedly returned fatal: %v", fatal)
	}
	if len(notes) != 0 {
		t.Fatalf("isolated cross-path merge must produce zero warning notes (-strict=true would escalate them); got %d: %v", len(notes), notes)
	}
	if len(merged.Results) != 3 {
		t.Fatalf("merged output must have 3 paths; got %d", len(merged.Results))
	}

	wantN := map[string]int{"OTLP HTTP": len(httpSamples), "OTLP gRPC": len(grpcSamples), "AgenticUDP": len(udpSamples)}
	for _, r := range merged.Results {
		if want, ok := wantN[r.Path]; !ok || r.N != want {
			t.Errorf("path %q: N=%d, want %d (or path unexpected)", r.Path, r.N, want)
		}
		if r.Status != "PASS" {
			t.Errorf("path %q: status=%q, want PASS (every chunk reported PASS)", r.Path, r.Status)
		}
	}

	for _, r := range merged.Results {
		if r.Path == "AgenticUDP" {
			if r.UDPSent != 7 || r.UDPAcked != 7 {
				t.Errorf("AgenticUDP: udp_sent=%d udp_acked=%d, want 7/7 (counters must propagate from contributing chunk)", r.UDPSent, r.UDPAcked)
			}
		}
	}

	if merged.Iterations != 5+6+7 {
		t.Errorf("merged.Iterations=%d, want %d (sum across chunks)", merged.Iterations, 5+6+7)
	}
}

// TestMergeChunks_CrossPathIsolated_WithFailures covers the
// realistic case: each chunk's path has both successful samples
// and failures (e.g. HTTP got 600/1000 with 400 timeouts, gRPC
// got 800/1000 with 200 timeouts). The merged JSON must carry
// each path's failure totals AND mark the path PASS_WITH_FAILURES,
// not silently downgrade to PASS just because the merger re-runs
// SummarizeWithFailures.
func TestMergeChunks_CrossPathIsolated_WithFailures(t *testing.T) {
	chunks := []chunkFile{
		{
			SpansPerCall: 5000,
			Iterations:   1000,
			Tenant:       "Trading-dev",
			BenchMode:    "standard-baseline",
			Results: []benchstats.PathResult{{
				Path: "OTLP HTTP", N: 600, Status: "PASS_WITH_FAILURES", SpansPerCall: 5000,
				Samples:     []float64{100, 200, 300},
				Attempted:   1000,
				Failures:    400,
				Timeouts:    400,
				FailedSamples: []float64{30000, 30000, 30000, 30000},
			}},
		},
		{
			SpansPerCall: 5000,
			Iterations:   1000,
			Tenant:       "Trading-dev",
			BenchMode:    "standard-baseline",
			Results: []benchstats.PathResult{{
				Path: "OTLP gRPC", N: 800, Status: "PASS_WITH_FAILURES", SpansPerCall: 5000,
				Samples:     []float64{500, 600, 700, 800},
				Attempted:   1000,
				Failures:    200,
				Timeouts:    200,
				FailedSamples: []float64{30000, 30000},
			}},
		},
	}
	files := []string{"http.json", "grpc.json"}
	hashes := []string{"a", "b"}

	merged, notes, fatal := mergeChunks(files, chunks, hashes)
	if fatal != nil {
		t.Fatalf("merge unexpectedly fatal: %v", fatal)
	}
	if len(notes) != 0 {
		t.Fatalf("expected zero notes for cross-path isolated merge with failures; got %v", notes)
	}

	for _, r := range merged.Results {
		if r.Status != "PASS_WITH_FAILURES" {
			t.Errorf("path %q: merged status=%q, want PASS_WITH_FAILURES (contributing chunks reported failures)", r.Path, r.Status)
		}
		if r.Failures == 0 {
			t.Errorf("path %q: failures=0 in merged output, want nonzero (contributing chunk had failures)", r.Path)
		}
	}
}

// TestMergeChunks_SumsWallClockSeconds is the new wall-clock
// contract: when chunk files carry per-path wall_clock_seconds,
// the merged output's wall_clock_seconds must be the SUM (chunks
// run sequentially, so total wall clock = sum of chunk walls), and
// SpansPerSecondWall must be recomputed from that sum using the
// merged Attempted total. Without this, a chunked n=10000 run
// would report a wall-clock throughput indistinguishable from a
// single chunk's, hiding the very stopwatch reality the field was
// added to expose.
//
// Realistic shape: each chunk has N successful samples in
// Samples (one entry per success iter), AND a failure count, so
// Attempted = N + Failures. This is the asymmetric case where
// the wall-clock formula's "use Attempted, not N" matters; if
// the formula incorrectly used N the throughput would be off by
// the success/total ratio.
func TestMergeChunks_SumsWallClockSeconds(t *testing.T) {
	// Two chunks of 5 success iters + 5 failed iters each.
	// Per chunk: N=5, Attempted=10. Merged: N=10, Attempted=20.
	makeChunk := func(wallSec float64) chunkFile {
		return chunkFile{
			SpansPerCall: 5000,
			Iterations:   10,
			Tenant:       "Trading-dev",
			BenchMode:    "standard-baseline",
			Results: []benchstats.PathResult{{
				Path: "AgenticUDP", N: 5, Status: "PASS_WITH_FAILURES", SpansPerCall: 5000,
				Samples:          []float64{100, 200, 300, 400, 500},
				Attempted:        10,
				Failures:         5,
				Timeouts:         5,
				FailedSamples:    []float64{60000, 60000, 60000, 60000, 60000},
				WallClockSeconds: wallSec,
			}},
		}
	}
	chunks := []chunkFile{
		makeChunk(600.0), // chunk A: 10 minutes
		makeChunk(900.0), // chunk B: 15 minutes
	}
	files := []string{"udp-A.json", "udp-B.json"}
	hashes := []string{"a", "b"}

	merged, _, fatal := mergeChunks(files, chunks, hashes)
	if fatal != nil {
		t.Fatalf("merge unexpectedly fatal: %v", fatal)
	}
	if len(merged.Results) != 1 {
		t.Fatalf("expected 1 merged path (AgenticUDP), got %d", len(merged.Results))
	}
	r := merged.Results[0]

	// 600 + 900 = 1500 seconds total wall clock.
	if r.WallClockSeconds != 1500.0 {
		t.Errorf("merged WallClockSeconds = %v, want 1500.0 (sum of 600 + 900)", r.WallClockSeconds)
	}
	// Merged Attempted = 20 (10 N + 10 failures across both chunks).
	if r.Attempted != 20 {
		t.Errorf("merged Attempted = %d, want 20", r.Attempted)
	}
	// 20 attempted * 5000 spans / 1500s = 66.67 spans/sec.
	// (The success-only SpansPerSecond would be much higher because
	// AvgMs is over the 10 success samples only — that's the
	// asymmetry this field exists to expose.)
	want := 66.7
	if abs(r.SpansPerSecondWall-want) > 0.5 {
		t.Errorf("merged SpansPerSecondWall = %v, want ~%v (= 20 attempted * 5000 / 1500s)", r.SpansPerSecondWall, want)
	}
	// Sanity: success-only throughput should be dramatically
	// higher, proving the asymmetry is observable in the merged JSON.
	if r.SpansPerSecond <= r.SpansPerSecondWall*5 {
		t.Errorf("SpansPerSecond (%v) should be much higher than SpansPerSecondWall (%v) when failures dominate; this test pins that the diagnostic gap survives the merge", r.SpansPerSecond, r.SpansPerSecondWall)
	}
}

// TestMergeChunks_OldChunksWithoutWallClock covers the
// backward-compat path: chunks written by an older bench build
// with no wall_clock_seconds field decode as zero, and the
// merger must NOT emit a divide-by-zero, infinity, or
// nonsense throughput. The merged output simply has zero
// wall-clock fields, signalling "not measurable from these
// chunks", which the operator's downstream tooling can detect
// by checking the field for zero.
func TestMergeChunks_OldChunksWithoutWallClock(t *testing.T) {
	chunks := []chunkFile{
		{
			SpansPerCall: 5000, Iterations: 500, Tenant: "Trading-dev", BenchMode: "standard-baseline",
			Results: []benchstats.PathResult{{
				Path: "AgenticUDP", N: 500, Status: "PASS", SpansPerCall: 5000,
				Samples: []float64{100, 200, 300},
				// No WallClockSeconds set — old chunk shape.
			}},
		},
		{
			SpansPerCall: 5000, Iterations: 500, Tenant: "Trading-dev", BenchMode: "standard-baseline",
			Results: []benchstats.PathResult{{
				Path: "AgenticUDP", N: 500, Status: "PASS", SpansPerCall: 5000,
				Samples: []float64{400, 500, 600},
			}},
		},
	}
	merged, _, fatal := mergeChunks([]string{"a.json", "b.json"}, chunks, []string{"a", "b"})
	if fatal != nil {
		t.Fatalf("merge unexpectedly fatal: %v", fatal)
	}
	r := merged.Results[0]
	if r.WallClockSeconds != 0 {
		t.Errorf("merged WallClockSeconds = %v, want 0 (no contributing chunk had a wall clock)", r.WallClockSeconds)
	}
	if r.SpansPerSecondWall != 0 {
		t.Errorf("merged SpansPerSecondWall = %v, want 0 (cannot compute without wall clock)", r.SpansPerSecondWall)
	}
}

// abs is a tiny helper because the merge package does not pull
// math just for an absolute-value check.
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
