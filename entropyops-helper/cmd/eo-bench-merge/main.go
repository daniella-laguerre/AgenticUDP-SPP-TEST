// eo-bench-merge concatenates per-iteration samples from multiple
// eo-ingest-bench chunk runs and emits a single merged JSON of the same
// shape, with statistics recomputed using the exact same algorithm as
// a live run (internal/benchstats.Summarize).
//
// Why this exists:
//
// The 5000-span n=10,000 follow-up run takes ~1.5–2.5 days of
// wall-clock per replica on a real Windows VM. Many operators cannot
// dedicate a host that long without their RDP session, screen-saver,
// antivirus, or cloud-VM auto-stop policy interrupting the run. The
// supported workaround is to do many small chunk runs back-to-back —
// e.g. 50 × n=200 or 10 × n=1000 — each of which fits inside a
// single uninterrupted session, and then merge their per-iteration
// samples into a single artifact.
//
// For the merged artifact to be cited interchangeably with a single
// live n=10,000 run, two properties must hold:
//
//  1. The merged JSON has the exact same shape (so downstream tooling
//     and the writeup tables don't need a second code path).
//  2. The percentile / mean / rounding math is bit-identical to a
//     single live run with the concatenated samples — otherwise the
//     merged result is not the same statistical statement and cannot
//     be cited as such.
//
// Both are guaranteed by importing the shared internal/benchstats
// package the bench tool itself uses; see that package's doc-comment
// for the rationale.
//
// Compatibility requirements between chunks (enforced or warned):
//
//   - spans_per_call MUST match across chunks (different batch sizes
//     are different workloads; merging them is a methodology error).
//   - bench_mode MUST match across chunks (standard-baseline and
//     tuned are different stacks; merging is a methodology error).
//   - tenant SHOULD match (the same tenant exercises the same
//     downstream rules); we warn rather than abort because there are
//     legitimate cases for cross-tenant pooling in non-prod.
//   - Each chunk MUST have been written with -keep-samples (otherwise
//     samples_ms is empty and the merge is impossible). The bench
//     tool is silent about the missing flag; this tool flags it
//     explicitly so the operator doesn't discover the problem after
//     a multi-day run.
//   - Paths present in some chunks but not others are merged on the
//     paths that appear; we warn so the operator can spot
//     accidental -paths flags.
//
// The output records merged_from = [{file, n, sha256?}, ...] so any
// reader of the merged JSON can reproduce the merge from the chunk
// files.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/entropyops/entropyops-helper/internal/benchstats"
)

// chunkFile is the JSON shape eo-ingest-bench writes (top level).
// We keep it as a separate type so a future bench-tool field
// addition (e.g. a new metadata field) does not silently drop on
// the merge — adding a field here forces us to decide whether to
// require equality, take the union, or take the first.
type chunkFile struct {
	GeneratedAt  string                  `json:"generated_at"`
	SpansPerCall int                     `json:"spans_per_call"`
	Iterations   int                     `json:"iterations"`
	Warmup       int                     `json:"warmup"`
	Tenant       string                  `json:"tenant"`
	BenchMode    string                  `json:"bench_mode"`
	Results      []benchstats.PathResult `json:"results"`
}

// mergedFile is the merged output. Fields above the line mirror the
// chunk-file schema 1:1 so downstream tooling can read it as if it
// were a single live run. Fields below the line are merge metadata
// that lets a reader reproduce the merge.
type mergedFile struct {
	GeneratedAt  string                  `json:"generated_at"`
	SpansPerCall int                     `json:"spans_per_call"`
	Iterations   int                     `json:"iterations"`
	Warmup       int                     `json:"warmup"`
	Tenant       string                  `json:"tenant"`
	BenchMode    string                  `json:"bench_mode"`
	Results      []benchstats.PathResult `json:"results"`

	MergedFrom []mergedSource `json:"merged_from"`
	MergeNotes []string       `json:"merge_notes,omitempty"`
}

// mergedSource records one chunk file by name, its n per path, and a
// sha256 so the merged output is reproducible from the same set of
// chunk files. Hashing is cheap relative to the multi-day cost of
// generating the chunks themselves.
type mergedSource struct {
	File   string         `json:"file"`
	SHA256 string         `json:"sha256"`
	NByPath map[string]int `json:"n_by_path"`
}

func main() {
	var (
		inGlob     = flag.String("in", "", "Glob pattern for chunk files (e.g. 'bench-5000-udp-chunk*.json'). Combined with positional args.")
		out        = flag.String("out", "", "Output path for the merged JSON (required)")
		keep       = flag.Bool("keep-samples", false, "Include the merged samples_ms array in the output (large; default off — same as eo-ingest-bench).")
		strict     = flag.Bool("strict", true, "Abort on any compatibility warning (mismatched tenant, missing path in some chunks, etc.). Set -strict=false to merge anyway.")
		summaryOnly = flag.Bool("summary", false, "Print the merged summary table to stdout in addition to writing the JSON.")
	)
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: eo-bench-merge -out merged.json [-in 'glob'] [chunk1.json chunk2.json ...]")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Concatenates per-iteration samples from multiple eo-ingest-bench chunk runs")
		fmt.Fprintln(os.Stderr, "and emits a single merged JSON of identical shape, with all statistics")
		fmt.Fprintln(os.Stderr, "(p50/p90/p99/avg/min/max/spans_per_second) recomputed using the exact same")
		fmt.Fprintln(os.Stderr, "algorithm as a live run. Each chunk MUST have been written with -keep-samples.")
		fmt.Fprintln(os.Stderr, "")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *out == "" {
		fmt.Fprintln(os.Stderr, "ERROR: -out is required")
		flag.Usage()
		os.Exit(2)
	}

	files, err := resolveChunkFiles(*inGlob, flag.Args())
	if err != nil {
		log.Fatalf("resolve chunk files: %v", err)
	}
	if len(files) < 2 {
		log.Fatalf("need at least 2 chunk files to merge; got %d (use -in 'glob' or positional args)", len(files))
	}

	chunks := make([]chunkFile, 0, len(files))
	hashes := make([]string, 0, len(files))
	for _, f := range files {
		c, h, err := readChunk(f)
		if err != nil {
			log.Fatalf("read %s: %v", f, err)
		}
		chunks = append(chunks, c)
		hashes = append(hashes, h)
	}

	merged, notes, fatal := mergeChunks(files, chunks, hashes)
	for _, w := range notes {
		fmt.Fprintln(os.Stderr, "WARN:", w)
	}
	if fatal != nil {
		log.Fatalf("merge failed: %v", fatal)
	}
	if *strict && len(notes) > 0 {
		log.Fatalf("strict mode: %d compatibility warning(s); rerun with -strict=false to merge anyway", len(notes))
	}

	// Drop the (potentially very large) samples_ms array from the
	// output unless the caller explicitly asked for it. This matches
	// the bench-tool default and keeps the JSON small enough to
	// commit alongside the chunk files.
	if !*keep {
		for i := range merged.Results {
			merged.Results[i].Samples = nil
		}
	}

	f, err := os.Create(*out)
	if err != nil {
		log.Fatalf("create output: %v", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(merged); err != nil {
		log.Fatalf("encode output: %v", err)
	}

	fmt.Printf("Merged %d chunks → %s\n", len(files), *out)
	fmt.Printf("Effective n per path:\n")
	for _, r := range merged.Results {
		// spans/sec_avg = per-iter throughput from success-only AvgMs
		//                 (matches the existing SpansPerSecond field).
		// spans/sec_wall = (attempted * spans) / summed wall clock
		//                  (matches the operator-experienced stopwatch;
		//                   includes failed iters in the numerator).
		// Both are printed so a discrepancy between them — the symptom
		// the n=1000 thread surfaced — is visible at a glance.
		extra := ""
		if r.WallClockSeconds > 0 {
			extra = fmt.Sprintf("  wall=%.1fs  spans/sec_wall=%.1f", r.WallClockSeconds, r.SpansPerSecondWall)
		}
		fmt.Printf("  %-12s n=%-6d  p50=%.2f ms  p99=%.2f ms  avg=%.2f ms  spans/sec_avg=%.1f%s\n",
			r.Path, r.N, r.P50Ms, r.P99Ms, r.AvgMs, r.SpansPerSecond, extra)
	}
	if *summaryOnly {
		fmt.Println()
		fmt.Println("Merged from:")
		for _, s := range merged.MergedFrom {
			fmt.Printf("  %s  sha256=%s  %v\n", s.File, s.SHA256[:12]+"…", s.NByPath)
		}
	}
}

// resolveChunkFiles expands the optional -in glob and combines with
// positional args, deduping and sorting for stable output. We sort
// because PowerShell does not glob-expand on the caller's behalf, so
// users mix '-in pattern' with explicit positional args; we want the
// merge to be deterministic regardless of input order.
func resolveChunkFiles(glob string, positional []string) ([]string, error) {
	seen := make(map[string]struct{})
	var files []string
	add := func(p string) {
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		files = append(files, p)
	}
	if glob != "" {
		matches, err := filepath.Glob(glob)
		if err != nil {
			return nil, fmt.Errorf("invalid glob %q: %w", glob, err)
		}
		for _, m := range matches {
			add(m)
		}
	}
	for _, p := range positional {
		add(p)
	}
	sort.Strings(files)
	return files, nil
}

// readChunk loads one chunk JSON, computes its sha256 (so the merged
// output records exactly which inputs went in), and validates that
// samples_ms is present (otherwise the merge produces nonsense).
func readChunk(path string) (chunkFile, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return chunkFile{}, "", err
	}
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])
	var c chunkFile
	if err := json.Unmarshal(data, &c); err != nil {
		return chunkFile{}, "", fmt.Errorf("decode: %w (is this an eo-ingest-bench JSON?)", err)
	}
	if len(c.Results) == 0 {
		return chunkFile{}, "", fmt.Errorf("no results in chunk")
	}
	for _, r := range c.Results {
		// We accept FAIL_* statuses with empty samples — they get
		// surfaced as a merge-time warning so the operator can
		// decide whether to drop the chunk. But a PASS or
		// PASS_WITH_FAILURES with no samples means the chunk was
		// generated without -keep-samples, which makes a merge
		// mathematically impossible, so we hard-fail.
		if (r.Status == "PASS" || r.Status == "PASS_WITH_FAILURES") && len(r.Samples) == 0 {
			return chunkFile{}, "", fmt.Errorf(
				"path %q reported %s with no samples_ms — chunk was generated without -keep-samples; cannot merge",
				r.Path, r.Status,
			)
		}
	}
	return c, hash, nil
}

// mergeChunks does the actual concatenation and re-summary. It
// returns the merged file, a list of non-fatal warnings (which
// -strict=true escalates to errors), and a fatal error for the
// methodology-error cases (different spans_per_call, different
// bench_mode).
//
// The merged output's Iterations is the sum of chunk Iterations,
// Warmup is the sum of chunk Warmups (the warmup samples were
// already excluded from each chunk's samples_ms before we got here,
// so this is purely informational), and GeneratedAt is set to the
// merge time.
func mergeChunks(files []string, chunks []chunkFile, hashes []string) (mergedFile, []string, error) {
	if len(chunks) == 0 {
		return mergedFile{}, nil, fmt.Errorf("no chunks to merge")
	}

	first := chunks[0]
	var notes []string

	// Methodology-fatal checks: spans_per_call and bench_mode must
	// match. Mixing batch sizes or modes makes the merged statistic
	// meaningless.
	for i := 1; i < len(chunks); i++ {
		if chunks[i].SpansPerCall != first.SpansPerCall {
			return mergedFile{}, nil, fmt.Errorf(
				"spans_per_call mismatch: %s=%d vs %s=%d (cannot merge different batch sizes)",
				files[0], first.SpansPerCall, files[i], chunks[i].SpansPerCall,
			)
		}
		if chunks[i].BenchMode != "" && first.BenchMode != "" && chunks[i].BenchMode != first.BenchMode {
			return mergedFile{}, nil, fmt.Errorf(
				"bench_mode mismatch: %s=%q vs %s=%q (cannot merge standard-baseline and tuned)",
				files[0], first.BenchMode, files[i], chunks[i].BenchMode,
			)
		}
	}

	// Soft warnings: tenant and warmup. Tenants can legitimately
	// differ in non-prod tests; warmup can vary because the
	// operator may have used different warmup counts per chunk.
	tenants := uniqueStrings(chunks, func(c chunkFile) string { return c.Tenant })
	if len(tenants) > 1 {
		notes = append(notes, fmt.Sprintf("tenant mismatch across chunks: %v (proceeding; review whether cross-tenant pooling is intended)", tenants))
	}

	totalIters := 0
	totalWarmup := 0
	for _, c := range chunks {
		totalIters += c.Iterations
		totalWarmup += c.Warmup
	}

	// Per-path concat: paths can legitimately differ across chunks
	// (e.g. an HTTP-only chunk and a UDP-only chunk), but if a path
	// is missing from some chunks we warn so the operator notices.
	pathOrder := []string{}
	pathSamples := map[string][]float64{}
	pathChunks := map[string]int{}
	pathSpans := map[string]int{}
	pathStatus := map[string]string{}
	for _, c := range chunks {
		for _, r := range c.Results {
			if _, seen := pathSamples[r.Path]; !seen {
				pathOrder = append(pathOrder, r.Path)
				pathSpans[r.Path] = r.SpansPerCall
			}
			if pathSpans[r.Path] != r.SpansPerCall {
				return mergedFile{}, nil, fmt.Errorf(
					"path %q has inconsistent spans_per_call across chunks (%d vs %d)",
					r.Path, pathSpans[r.Path], r.SpansPerCall,
				)
			}
			pathSamples[r.Path] = append(pathSamples[r.Path], r.Samples...)
			pathChunks[r.Path]++
			// Record the worst (most-failed) status seen across
			// chunks so a single bad chunk can't be silently masked.
			if pathStatus[r.Path] == "" || isWorseStatus(r.Status, pathStatus[r.Path]) {
				pathStatus[r.Path] = r.Status
			}
		}
	}
	// Cross-path isolated mode: every chunk contributes a disjoint
	// set of paths (e.g. chunk-A=HTTP-only, chunk-B=gRPC-only,
	// chunk-C=UDP-only). This is the recommended run protocol for
	// publishable cross-path numbers — each path measures against a
	// fresh server/DB so the comparison isn't tainted by one path
	// warming the DB before the next path runs. In this mode the
	// "path X present in some-but-not-all chunks" warning would
	// fire on every path and (under -strict=true) escalate to a
	// fatal error, even though the merge is exactly what the
	// operator intended. Suppress per-path missing-from-others
	// warnings in that case; emit a single info line so the merge
	// log is unambiguous about which mode produced the output.
	isolated := isCrossPathIsolated(chunks)
	if isolated {
		fmt.Fprintf(os.Stderr,
			"INFO: cross-path isolated merge detected — %d chunks each contribute a disjoint set of paths (the recommended protocol for headline cross-path numbers; per-path missing-from-others warnings suppressed).\n",
			len(chunks),
		)
	}
	for _, p := range pathOrder {
		if pathChunks[p] != len(chunks) && !isolated {
			notes = append(notes, fmt.Sprintf("path %q present in %d/%d chunks (some chunks lacked it; merging on the chunks that have it)", p, pathChunks[p], len(chunks)))
		}
	}

	results := make([]benchstats.PathResult, 0, len(pathOrder))
	for _, p := range pathOrder {
		// Aggregate the failure breakdown across all chunks for
		// this path. Each chunk recorded its own per-category
		// counts and (when -keep-samples was used) the per-iter
		// wall-clock of its failed iterations; the merged result
		// must carry the totals so a chunked run is cited the
		// same way as a single live run with the same total
		// attempt count.
		fb := benchstats.FailureBreakdown{}
		for _, c := range chunks {
			for _, cr := range c.Results {
				if cr.Path != p {
					continue
				}
				fb.Timeouts += cr.Timeouts
				fb.NonOK += cr.NonOK
				fb.OtherErrors += cr.OtherErrors
				if len(cr.FailedSamples) > 0 {
					fb.FailedSamples = append(fb.FailedSamples, cr.FailedSamples...)
				}
				if cr.LastFailureMsg != "" {
					fb.Last = cr.LastFailureMsg
				}
			}
		}
		r := benchstats.SummarizeWithFailures(p, pathSamples[p], fb, pathSpans[p])
		// Promote AgenticUDP protocol counters by summing across
		// chunks so the merged JSON has the totals a single live
		// run would have produced. Sum wall-clock seconds across
		// chunks too: an N-chunk run took the SUM of the chunks'
		// wall clocks (chunks are run sequentially), so the merged
		// throughput is (total attempted spans) / (total wall
		// clock). The merger does NOT carry started_at /
		// finished_at — those are meaningful only for a single
		// contiguous live run.
		var wallSecondsSum float64
		for _, c := range chunks {
			for _, cr := range c.Results {
				if cr.Path != p {
					continue
				}
				r.UDPSent += cr.UDPSent
				r.UDPAcked += cr.UDPAcked
				r.UDPRetransmits += cr.UDPRetransmits
				r.UDPDropped += cr.UDPDropped
				wallSecondsSum += cr.WallClockSeconds
			}
		}
		if wallSecondsSum > 0 {
			r.SetWallClockSeconds(wallSecondsSum)
		}
		// If any contributing chunk reported a non-PASS / non-
		// PASS_WITH_FAILURES for this path (e.g. FAIL_HANDSHAKE
		// from a chunk where the AgenticUDP receiver was down),
		// propagate that into the merged result rather than
		// silently overwriting with the recomputed status.
		propagate := pathStatus[p] != "" &&
			pathStatus[p] != "PASS" &&
			pathStatus[p] != "PASS_WITH_FAILURES"
		if propagate {
			r.Status = pathStatus[p]
			r.Note = fmt.Sprintf("merged status reflects worst per-chunk status across %d chunks", pathChunks[p])
		}
		results = append(results, r)
	}

	sources := make([]mergedSource, 0, len(files))
	for i, f := range files {
		ns := map[string]int{}
		for _, r := range chunks[i].Results {
			ns[r.Path] = r.N
		}
		sources = append(sources, mergedSource{
			File:    f,
			SHA256:  hashes[i],
			NByPath: ns,
		})
	}

	out := mergedFile{
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		SpansPerCall: first.SpansPerCall,
		Iterations:   totalIters,
		Warmup:       totalWarmup,
		Tenant:       first.Tenant,
		BenchMode:    first.BenchMode,
		Results:      results,
		MergedFrom:   sources,
		MergeNotes:   notes,
	}
	return out, notes, nil
}

// uniqueStrings collects the distinct values of fn(c) across chunks.
func uniqueStrings(chunks []chunkFile, fn func(chunkFile) string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, c := range chunks {
		v := fn(c)
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			out = append(out, v)
		}
	}
	return out
}

// isCrossPathIsolated returns true when every chunk contributes a
// set of paths that is disjoint from every other chunk's paths.
// This is the canonical "isolated per-path" run protocol the
// writeup recommends for publishable cross-path numbers: bring
// the server up with a fresh ./data, run -paths http, tear down,
// bring server up with fresh ./data, run -paths grpc, tear down,
// and so on. Each chunk file then has results for exactly one
// path, no chunk shares paths with any other chunk, and the
// "missing from N/M chunks" warning that's appropriate for the
// chunked-same-path use case is misleading here — the operator
// did the right thing.
//
// Returns false for any of: <2 chunks (nothing to be isolated
// across); a chunk with zero results (no paths to contribute); a
// path that appears in two or more chunks (which means at least
// one path's measurements were not isolated, so the cross-path
// comparison story is back on the table).
func isCrossPathIsolated(chunks []chunkFile) bool {
	if len(chunks) < 2 {
		return false
	}
	seenIn := map[string]int{}
	for i, c := range chunks {
		if len(c.Results) == 0 {
			return false
		}
		for _, r := range c.Results {
			if _, dup := seenIn[r.Path]; dup {
				return false
			}
			seenIn[r.Path] = i
		}
	}
	return true
}

// isWorseStatus is a tiny ordering on the bench tool's status values.
// PASS < PASS_WITH_FAILURES < FAIL_TIMEOUT < FAIL_DIAL < FAIL_HANDSHAKE < FAIL_NO_DATA so
// the merged status reflects the worst observation. PASS_WITH_FAILURES
// sits between PASS and the FAIL_* statuses because the path delivered
// some successful samples but also lost some attempts; merging two
// PASS_WITH_FAILURES chunks should yield a PASS_WITH_FAILURES merged
// result, not silently downgrade to PASS.
func isWorseStatus(a, b string) bool {
	rank := func(s string) int {
		switch s {
		case "PASS":
			return 0
		case "PASS_WITH_FAILURES":
			return 1
		case "FAIL_TIMEOUT":
			return 2
		case "FAIL_DIAL":
			return 3
		case "FAIL_HANDSHAKE":
			return 4
		case "FAIL_NO_DATA":
			return 5
		default:
			if strings.HasPrefix(s, "FAIL") {
				return 6
			}
			return 0
		}
	}
	return rank(a) > rank(b)
}

// Compile-time guard: keep the io import in case future merger
// features (e.g. streaming a very large samples_ms field) need it.
var _ = io.Discard
