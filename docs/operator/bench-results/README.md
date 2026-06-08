# Archived `eo-ingest-bench` results

Raw JSON output from `eo-ingest-bench` runs that the `PROGRAM_SUMMARY.md`
"Current measured numbers" table is built from. Keep these so anyone
re-reading the doc can spot-check the multipliers cited in the prose.

## 2026-04-29 — Windows loopback, v2.14.x, all four bulk-send fixes applied

| File | Batch | Iters | Server | Notes |
|------|-------|-------|--------|-------|
| `bench-200.json`  | 200 spans  | 50 | localhost (Windows) | Smoke. AgenticUDP p50 = 3.40 ms, retx=0, drops=0 |
| `bench-1000.json` | 1000 spans | 30 | localhost (Windows) | Mid-load. AgenticUDP p50 = 37.50 ms, retx=0, drops=0 |
| `bench-5000.json` | 5000 spans | 20 | localhost (Windows) | Stress, **pre-WAL-fix**. AgenticUDP **bimodal**: p50 = 179.67 ms, p90 = 5,226 ms, p99 = 6,226 ms, retx=440, drops=0. |

## 2026-04-29 — Same hardware, **post-WAL-checkpointer fix**, `wal_autocheckpoint=0`

| File | Batch | Iters | Server | Notes |
|------|-------|-------|--------|-------|
| `bench-5000-postwal.json` | 5000 spans | 20 | localhost (Windows) | AgenticUDP p50 = 179.24 ms (flat), p90 = 3,080 ms (-41% vs pre), p99 = 3,740 ms (-40%), **retx=0** (was 440), drops=0. **HTTP** improved disproportionately too: p50 1,519 → 583 ms (-62%), 8,502 spans/sec. **gRPC**: p50 2,376 → 1,160 ms (-51%). |

## 2026-04-29 — Post-instrumentation rerun (no behavioural change, just SLOW FLUSH log lines)

| File | Batch | Iters | Server | Notes |
|------|-------|-------|--------|-------|
| `bench-5000-postinstr.json` | 5000 spans | 20 | localhost (Windows) | AgenticUDP p50 = 191 ms, p90 = 2,476 ms, p99 = 2,845 ms, retx=42, drops=0. Within run-to-run variance of `bench-5000-postwal.json`. **The value of this run is the server log**, not the JSON — the per-flush latency tracker captured the bimodal-tail mechanism. |

What the SLOW FLUSH log lines from this run proved:

- **Big flushes (`items≈2000`) consistently took 660–960 ms.** That is a
  single `WriteTraces` call on a 2,000-trace batch and identifies the
  per-row INSERT loop as the dominant cost (`~0.35 ms/row`).
- **Small flushes (`items=15–105`) ALSO took 200–830 ms.** A 15-item
  write should complete in <10 ms in absolute terms; observing 700 ms
  is direct evidence that small flushes were **queued behind big ones**
  in the coalescer's single-flusher goroutine.
- **Final stats: `flushes=139, items=110000, slow=92, max=946 ms,
  avg=428 ms`.** Sustained storage write rate ≈ 1,300 items/sec, vs
  inbound rate ≈ 5,500 items/sec → ~26 s of post-bench drain.
- **HTTP was 3× faster per-row** (`558 ms / 5,000 = 0.11 ms/row`) than
  the AgenticUDP coalescer's big flushes (`0.35 ms/row`). The cause is
  partly that HTTP issues one transaction per request (one fsync) while
  the coalescer cap of 2,000 forced 3 transactions per 5,000-span
  batch, but most of the gap is in the per-row Prepare+Exec loop
  itself.

This run is the empirical justification for the next change: replace
the per-row INSERT loop in `WriteTraces`/`WriteMetrics`/`WriteLogs`
with multi-row bulk INSERT (`INSERT INTO traces VALUES (...), (...),
...` chunked at 100 rows / statement). Local unit-test latency on the
same `WriteTraces(2000)` workload dropped from a per-row 600–900 ms
profile to **11 ms**.

## 2026-04-29 — Bulk-INSERT shipped, **production unchanged** — diagnostic added

| File | Batch | Iters | Server | Notes |
|------|-------|-------|--------|-------|
| `bench-5000-postbulk.json` | 5000 spans | 20 | localhost (Windows) | AgenticUDP p50 = 201 ms, p90 = 2,135 ms, p99 = 3,613 ms, retx=0, drops=0. **No meaningful change vs `bench-5000-postinstr.json`**. SLOW FLUSH log lines still show 600–900 ms for `items≈2000` flushes (max 1,104 ms). Final stats: `flushes=154 items=110000 slow=92 max_ms=1104 avg_ms=384`. |

**This is a useful negative result.** Local unit tests confirm the
bulk-INSERT path completes `WriteTraces(2000)` in 11–14 ms on macOS;
the production Windows box stayed at 600–900 ms per flush.
That is a 50–80× gap that cannot be explained by Windows fsync
overhead alone. Three remaining hypotheses:

1. **The deployed binary is stale.** The user re-extracted the zip but
   Windows file locking on the running `.exe` may have left the old
   binary in place and silently no-op'd the Expand-Archive. The next
   build prints a boot-line `sqlite: bulk-insert chunk size = 100
   rows/statement (per-row INSERT loop replaced); slow-write log
   threshold = 100ms`. **If that line is missing from the boot log,
   the bulk-INSERT code is not running.**
2. **modernc/sqlite multi-row INSERT is slow on Windows for this
   workload.** Surprising (pure-Go driver, OS shouldn't matter for
   parsing/planning) but possible. Distinguishable from (1) by reading
   the new `sqlite: SLOW WRITE WriteTraces rows=N chunks=M
   duration=...` log line that fires whenever a single Write* call
   exceeds 100 ms.
3. **Another writer is contending for the SQLite connection** (pipeline
   analytics writer, self-obs scraper, host scraper, change-event
   writer, ...) and the AgenticUDP coalescer's flush is queueing
   behind it inside the database/sql connection pool. Distinguishable
   from (2) by comparing `agenticudp: SLOW FLUSH ... duration=Xms` to
   `sqlite: SLOW WRITE ... duration=Yms` for the same call: if X ≫ Y,
   the wait is in the wrapper / connection pool; if X ≈ Y, the SQL
   write itself is slow.

The next build adds both diagnostics. The next 5000-span run will
disambiguate.

What this run established:

- **Inline `wal_autocheckpoint` was hurting every path, not just AgenticUDP.**
  Disabling inline checkpointing and moving it to a passive out-of-band
  goroutine roughly doubled HTTP and gRPC throughput too. That is why the
  AgenticUDP-vs-HTTP/gRPC delta narrowed even though AgenticUDP itself got
  faster.
- **Retransmits dropped from 440 to 0.** Direct evidence the WAL hypothesis
  was at least partly correct — the receiver is now ACKing fast enough that
  the agent never thinks a chunk was lost.
- **The bimodal tail at 5000 spans is reduced but not eliminated.** p50/p90
  spread is still ~17×. Something other than `wal_autocheckpoint` is
  occasionally stalling the storage write. Next step: per-flush latency
  instrumentation in the coalescer (`agenticudp: SLOW FLUSH ...` log lines,
  `agenticudp: flush_stats ...` every 10 s) shipped in the next build to
  identify whether the culprit is `WriteTraces` itself, the pipeline
  analytics writer, the self-obs scraper, or another writer competing for
  the single SQLite write connection.

Server build for `bench-5000-postwal.json` had:

- All four bulk-send fixes from the prior run.
- SQLite `wal_autocheckpoint=0` (default flipped from 1000).
- Out-of-band `walCheckpointer` goroutine (`PRAGMA wal_checkpoint(PASSIVE)`
  every 1 s, configurable via `ENTROPYOPS_SQLITE_CHECKPOINT_INTERVAL_MS`).
- Final `PRAGMA wal_checkpoint(TRUNCATE)` at shutdown.

**Read these alongside the three caveats in `PROGRAM_SUMMARY.md`** —
handler asymmetry, the 5000-span bimodal tail, and single-host loopback.
The numbers are not a clean cross-protocol ranking, they are a snapshot
of each path's current latency profile under fixed work.

## 2026-04-29 — Bench mode flag introduced (v2.15) — these older numbers are now "tuned mode"

Every result above was captured against a server with all SQLite
optimizations active for **all three** ingest paths (bulk INSERT,
NORMAL fsync, out-of-band WAL checkpointer, large mmap). That made
the OTLP HTTP / gRPC paths look unrealistically competitive vs
AgenticUDP — which was the user's central complaint: a customer's
existing OTLP backend has none of those tunings, so the comparison
masked AgenticUDP's actual product advantage.

As of v2.15 the server defaults to **standard-baseline mode**:

- `ENTROPYOPS_STANDARD_BASELINE_MODE` defaults to `true`.
- The OTLP HTTP / gRPC handlers route through `WriteTracesPerRow` /
  `WriteMetricsPerRow` / `WriteLogsPerRow` — a per-row INSERT loop in
  one transaction, i.e. what an off-the-shelf SQL-backed OTLP
  collector would do.
- The SQLite tuning PRAGMAs revert to defaults: `synchronous=FULL`,
  `wal_autocheckpoint=1000`, no large cache, no mmap.
- The out-of-band WAL checkpointer goroutine does NOT start.
- AgenticUDP keeps the bulk INSERT path because receiver-level
  batching is part of the AgenticUDP product, not a backend tuning.

Every bench JSON written by `eo-ingest-bench` now records the active
`bench_mode` next to the results. Future runs in this folder MUST be
labelled accordingly:

- `bench-<size>-baseline.json` — `bench_mode=standard-baseline` (the
  honest comparison vs an off-the-shelf OTLP backend).
- `bench-<size>-tuned.json` — `bench_mode=tuned` (platform-self-test
  only; useful for "does the WAL checkpointer still work?" but NOT
  for AgenticUDP-vs-industry framing).

The next benchmark run on Windows production should:

1. Refresh the binary as per `INGEST_BENCH_QUICKSTART.md`.
2. Confirm the boot log contains
   `BENCH MODE = standard-baseline (DEFAULT). OTLP HTTP/gRPC use per-row INSERT...`
3. Run `.\eo-ingest-bench-windows-amd64.exe ... -spans 5000 -iters 20 -json bench-5000-baseline.json`.
4. Capture the `server bench_mode: standard-baseline` line that the
   bench prints in its header.

For the gold-standard apples-to-apples comparison (HTTP/gRPC pointed
at Jaeger / OTel Collector / Tempo / customer's actual stack instead
of at the in-tree SQLite), see
[`THIRD_PARTY_BACKEND_BENCH_PLAN.md`](../THIRD_PARTY_BACKEND_BENCH_PLAN.md).

## 2026-04-29 — First standard-baseline run (v2.15+, headline numbers)

Captured against `bench_mode=standard-baseline` (the default in v2.15+).
OTLP HTTP/gRPC handlers used the per-row INSERT path against an
SQLite backend running default PRAGMAs (`synchronous=FULL`,
`wal_autocheckpoint=1000`, no large mmap, no out-of-band checkpointer).
AgenticUDP used its receiver-level coalescer + bulk INSERT path.

| File | Batch | Iters | OTLP HTTP p50 | OTLP gRPC p50 | AgenticUDP p50 | AgenticUDP p99 | AgenticUDP packets |
|------|-------|-------|---------------|---------------|-----------------|-----------------|--------------------|
| `bench-200-baseline.json`  | 200  | 50 | 169.43 ms   | 228.03 ms   | **3.96 ms**   | 10.04 ms   | sent=55  acked=55  retx=0  drops=0  |
| `bench-1000-baseline.json` | 1000 | 30 | 511.03 ms   | 714.15 ms   | **38.56 ms**  | 51.32 ms   | sent=2211  acked=2211  retx=0  drops=0 |
| `bench-5000-baseline.json` | 5000 | 20 | 1,669.44 ms | 2,259.68 ms | **185.98 ms** | not reported — n=20 is below the sample size for a defensible p99 estimator; see "5000-span" below | sent=7348  acked=7556  retx=208  drops=0 |

### What this run actually establishes

**At small-to-mid load (≤1000 spans), AgenticUDP is dramatically
faster than the off-the-shelf-equivalent OTLP backend on EVERY
percentile.** This is the realistic enterprise comparison and the
honest version of the AgenticUDP value proposition:

- **200-span batches**: AgenticUDP p50 is **42.8× lower** than OTLP
  HTTP p50 (3.96 ms vs 169.43 ms) and **57.6× lower** than OTLP gRPC.
  Throughput: AgenticUDP 46,548 spans/sec vs HTTP 1,199 spans/sec
  (38.8× higher).
- **1000-span batches**: AgenticUDP p50 is **13.2× lower** than HTTP
  (38.56 ms vs 511.03 ms) and 18.5× lower than gRPC. Zero
  retransmits, zero drops, p99 still under 52 ms — i.e. the win
  holds at the tail too.

**At stress load (5000 spans) the published claim is the median
only. Tail and average numbers are held until a higher-n run
lands.**

What the n=20 sample supports:

- **p50 = 185.98 ms is 9.0× lower than OTLP HTTP p50 (1,669 ms).**
  The architectural claim — AgenticUDP's median is roughly an order
  of magnitude lower than OTLP/HTTP at every batch size we tested —
  holds at 5000 spans the same way it holds at 200 and 1000.
- 208 retransmits, 0 drops. The protocol recovered every chunk.

What the n=20 sample does NOT support, and is therefore NOT
reported as a finding:

- **p90, p99, avg, max at 5000 spans.** p99 of n=20 is the single
  worst iteration; p90 is the second worst; average inherits the
  same outlier sensitivity. These numbers are in the JSON for
  completeness; we are not interpreting them. Reporting them as
  "AgenticUDP p99 vs HTTP p99" would be reporting noise as
  measurement.
- **spans/sec at 5000 spans.** Spans/sec is computed from total
  elapsed time per request, so it inherits the same outlier
  sensitivity as the average. The spans/sec claim — roughly an
  order of magnitude higher throughput — stands at 200 spans
  (38.8×, n=50) and 1000 spans (12.7×, n=30), where the tail is
  clean enough for the mean to be meaningful.

A **10,000-iteration follow-up at 5000 spans is in progress.** The
sample-size ladder we use:

- n ≥ 10,000 → publishable p99 with stable neighbours; p99.9
  (the percentile that actually matters for SLO budgets at
  production volume) starts to be meaningful.
- n ≥ 1,000  → defensible p99 (the original target; bumped to
  10,000 because the bench is cheap and the marginal precision
  matters at this batch size).
- n ≈ 100   → bare minimum to say anything about p99.
- n ≤ 20    → medians only.

The follow-up will resolve to one of three outcomes: tail is real
and characterized; tail is environmental noise that the original
n=20 result happened to surface; or tail collapses to near-p50
levels and the bimodal-tail story dissolves entirely. All three
are publishable; none of the three changes the median claim.

The prior tuned-mode 5000-span runs showed a similar shape, so
storage-side contention is the leading hypothesis if outcome (1)
lands — but we are not citing those tuned-mode results as
evidence here, since they were taken under different backend
configuration and have the same n=20 limitation.

### What this run does NOT establish

Same caveats as before. The OTLP HTTP handler still does the head/tail
sampler + k8s-attribute join inline; gRPC and AgenticUDP do not. So
some of AgenticUDP's lead is real protocol/storage advantage and some
is enrichment-work asymmetry. See
[`HANDLER_PARITY_PLAN.md`](../HANDLER_PARITY_PLAN.md).

The comparison is also still vs the in-tree SQLite, not vs an actual
third-party OTLP backend (Jaeger, Tempo, OTel Collector). That is
[`THIRD_PARTY_BACKEND_BENCH_PLAN.md`](../THIRD_PARTY_BACKEND_BENCH_PLAN.md),
not yet run.

---

## ⚠ v2.15.0 (2026-05-01) — bench binary corrections before running n=10,000

The `eo-ingest-bench` binary shipped with the `bench-5000-baseline.json`
run above had **two bugs that inflate the reported UDP numbers**. Both are
fixed in the v2.15.0 binary:

### Bug 1: `inflight` map key wraparound

Sessions exceeding 65,535 sends silently overwrote inflight entries using
a composite `(streamID << 16) | seq` key that wraps at 16-bit seq rollover.
The n=20 bench (total sends well below 65,535) was unaffected. The n=10,000
bench at 5000 spans/iter sends millions of datagrams and **will** hit this.
The fix uses a monotonic `uint32 seqFull` as the map key; ACK lookup does a
linear scan by wire `seq` + `streamID`.

### Bug 2: `acked` counter overcounting

When the server sent a duplicate ACK (retransmit-triggered), the `processACK`
helper incremented `c.acked.Add(1)` even if the inflight entry had already
been removed (first ACK). This caused the UDP drain condition to fire early —
the client thought all datagrams were ACKed before they actually were.

Effect on the published numbers: `WallClockSeconds` was shorter than actual
delivery time, inflating `spans/sec_wall` for the UDP path. The n=20 runs
show `acked=7556` against `sent=7348` — a 208-packet overcount that exactly
matches the `retx=208` count in `bench-5000-baseline.json`. This confirms
the overcounting was active in the baseline run.

### New primary UDP metric: `spans/sec_wall`

As of v2.15.0, `p50_ms` for UDP reflects **client send latency** (how long
`SendCycle` blocks waiting for the inflight window to free up), not the
end-to-end ACK round-trip. The end-to-end throughput metric is now
`spans/sec_wall = (iters × spans) / WallClockSeconds`, where
`WallClockSeconds` covers the full window from first send to last ACK
confirmed. Use `spans/sec_wall` for all cross-path comparisons going forward.

**Accept criteria for the n=10,000 v2.15.0 run:**

| Metric | Required |
|--------|---------|
| `fail%` | `0.0` — all datagrams ACKed within drain timeout |
| `retransmits` | `0` or near-zero |
| `spans/sec_wall` UDP vs HTTP | UDP wall-clock throughput > HTTP |
| `spans/sec_wall` UDP vs gRPC | UDP wall-clock throughput > gRPC |

**SHA-256 of the v2.15.0 bench binaries for the Windows VM:**

```
343c865658641ececdc18d2fb995763e99af200f51f509010881929da0436cdf  eo-ingest-bench-windows-amd64.exe
5f931cfca3a36b3d073b09aef351bc76d705c825f9a5c9be745340d55b44fd38  eo-bench-merge-windows-amd64.exe
```

**Do not** interpret n=10,000 results from a pre-v2.15.0 binary alongside
these baselines — the `acked` overcounting makes the UDP drain appear ~3%
faster than it actually is, which at n=10,000 total sends compounds into a
noticeable wall-clock difference.

When the n=10,000 run completes, archive the merged JSON here as
`bench-5000-n10000-baseline-run{1,2,3}.json` and update this README with
the confirmed `spans/sec_wall` numbers and tail distribution.
