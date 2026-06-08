# Building an Honest OTLP-vs-AgenticUDP Benchmark

*A standard-baseline mode, an open coalescer-stall investigation, and
the reproduction harness so you can break our numbers.*

---

## TL;DR

We built an alternative OpenTelemetry transport ("AgenticUDP" — UDP
datagrams + tier-aware ACKs + receiver-level coalescer) and ran it
against OTLP/HTTP and OTLP/gRPC into the same backend. Initial bench
results looked great. They were also dishonest, in a specific and
recoverable way: every path was hitting the same heavily-tuned SQLite
backend, which is not what a real customer's existing OTLP collector
looks like.

We added a `bench_mode` flag (`standard-baseline` is the new default;
`tuned` is the opt-in self-test) that flips the OTLP HTTP/gRPC paths
to behave like an off-the-shelf SQL-backed OTLP receiver: per-row
INSERT, FULL fsync, default cache, no out-of-band WAL checkpointer.
AgenticUDP keeps its bulk-write coalescer because that's part of the
AgenticUDP product, not a backend tuning. Re-ran the bench. The
numbers got better, not worse, because the framing finally matched
what we were actually claiming.

| Batch size | n | OTLP HTTP p50 | OTLP gRPC p50 | AgenticUDP p50 | AgenticUDP p99 |
|---|---|---|---|---|---|
| 200 spans | 50 | 169 ms | 228 ms | **3.96 ms** (42.8× lower than HTTP) | 10.04 ms |
| 1000 spans | 30 | 511 ms | 714 ms | **38.56 ms** (13.2× lower) | 51.32 ms |
| 5000 spans | 20 | 1,669 ms | 2,260 ms | **186 ms** (9.0× lower at p50) | tail behaviour under-sampled at n=20 — characterization in progress |

The architectural claim — **AgenticUDP's p50 is roughly an order of
magnitude lower than OTLP/HTTP at every batch size we tested** —
holds at every batch size. The throughput claim — roughly an order
of magnitude higher spans/sec — holds at 200 spans (38.8×, n=50)
and 1000 spans (12.7×, n=30).

We are deliberately *not* publishing tail or average numbers at
5000 spans yet. n=20 is below the iteration count needed to make a
defensible p99 claim (rule of thumb: n≥10,000 for a publishable
p99 + an emerging p99.9 estimator; n≥1,000 for a defensible p99;
n≈100 bare minimum to say anything about p99 with a straight face;
n≤20 medians only). A **10,000-iteration follow-up at 5000 spans
is in progress** — that's enough samples for the 9,900th-ranked
observation to act as a p99 estimator with neighbours 9,899 and
9,901 to sanity-check stability, and the 9,990th to give an
honest first read on p99.9. It will resolve the tail story to
"characterized," "environmental noise," or "no bimodal tail at
all" — see the 5000-span section below for the design and the
three branching outcomes. The methodology section is the actual
interesting part.

Reproduction harness, raw JSON, and binaries are linked at the
bottom. Reading-time-to-bench-running: ~10 minutes if you have
PowerShell or bash and 250 MB of disk.

---

## What AgenticUDP is, in 200 words

OTLP/HTTP and OTLP/gRPC both run over TCP. TCP gives you in-order,
per-byte-ACKed delivery whether you want it or not. For
high-frequency telemetry that's two structural costs:

1. **Per-request handshake + framing overhead.** A POST `/v1/traces`
   carrying 200 spans pays HTTP/1.1 framing, TLS, TCP ACKs, and
   server-side parse on every request. At enterprise volume — every
   SDK in every team firing every cycle — that overhead is the
   bottleneck, not the storage.
2. **Head-of-line blocking on shared streams.** One slow consumer on
   a multiplexed gRPC connection stalls every other span on that
   connection.

AgenticUDP is the alternative we built to attack both. The wire is
UDP datagrams (no per-byte ACK, no head-of-line). On top we layered:

- **Cycle-batched datagrams** — one collection cycle (metrics +
  traces + logs) becomes a small number of datagrams instead of
  dozens of HTTP requests.
- **Tier-aware delivery** — Guaranteed / Reliable / Best-effort.
  Best-effort metrics never block guaranteed traces.
- **Server-side `content_id` dedup** — duplicate datagrams (e.g. a
  reliable-tier retransmit that crossed a recovered ACK) are silently
  dropped before they hit storage.
- **Receiver-level batch coalescer** — incoming datagrams accumulate
  in a per-signal buffer (~50 ms window, 2000-item cap) before being
  written as a single bulk INSERT. The UDP read loop never blocks on
  storage I/O.
- **Optional DTLS** for the wire (Path C); cleartext UDP (Path B) is
  also supported for trusted networks.

That's the design. Whether it actually wins at scale is what the
bench is supposed to answer. Which brings us to the methodology
problem.

---

## UDP vs TCP (defaults) and why AgenticUDP layers reliability on UDP

Bare UDP and TCP make different trade-offs out of the box:

| Attribute | UDP (default) | TCP (default) |
|-----------|---------------|---------------|
| Delivery guarantee | No | Yes |
| Ordering | No | Yes |
| Retransmission | No | Yes |
| Flow control | No | Yes |
| Congestion control | No | Yes |
| Overhead / latency | Low | Higher |
| When to use | Real-time, multicast, simple queries | Reliable streams, file transfer, web traffic |

**Short answer.** You can make UDP behave like TCP by implementing the missing reliability features at the application or protocol layer: handshake, sequence numbers, ACKs/NACKs, retransmission, ordering, flow control, and congestion control. Many modern protocols do exactly this on top of UDP (for example QUIC).

### How to implement TCP-like reliability over UDP

**Connection setup (handshake).** Exchange an initial handshake to agree on parameters (initial sequence numbers, window sizes, options). Both sides then share session state before bulk data.

**Sequencing and framing.** Add sequence numbers to every datagram so the receiver can detect loss, reorder packets, and discard duplicates.

**Acknowledgements and retransmission.** Use ACKs (or selective ACKs) to confirm receipt. Retransmit unacknowledged packets after a timeout. Implement RTT estimation and adaptive retransmission timers to limit premature or excessive retransmits.

**Flow control.** Let the receiver advertise how much buffer it has (window size) so the sender does not overwhelm it.

**Congestion control.** Implement congestion-avoidance behaviour (slow start, congestion window, loss detection) so the sender backs off under network congestion instead of flooding the path. Without this, UDP traffic can be unfair to other flows and can increase loss for everyone.

**Duplicate suppression and reassembly.** Detect and drop duplicates using sequence numbers; reassemble larger messages split across multiple datagrams.

**Integrity and optional stronger checks.** UDP carries a checksum; you can add stronger integrity checks and application-level verification (hashes, CRCs) where needed.

**Security.** Protect the channel with DTLS or an equivalent (or use QUIC, which integrates TLS) for confidentiality and integrity. AgenticUDP Path C (DTLS) exists for exactly this class of requirement.

AgenticUDP was in part born from this pattern: keep UDP’s low per-datagram overhead and absence of TCP head-of-line blocking, then selectively add the reliability, pacing, and security pieces the workload actually needs—rather than inheriting all of TCP’s semantics by default.

---

## Why this comparison is hard to do honestly

The version of the bench we shipped first ran all three paths into
the same SQLite backend. That backend, over multiple rounds of
performance work, had accumulated the following tunings:

- `synchronous=NORMAL` instead of the SQLite default `FULL`.
- `wal_autocheckpoint=0` (inline checkpoint disabled) plus an
  out-of-band `walCheckpointer` goroutine running passive checkpoints
  every 1 s.
- 64 MB page cache, 256 MB mmap.
- Multi-row bulk INSERT (`INSERT INTO traces VALUES (...), (...)`
  chunked at 100 rows/statement) replacing the per-row INSERT loop in
  `WriteTraces` / `WriteMetrics` / `WriteLogs`.

Every one of those tunings is real engineering work. Every one of
them benefits AgenticUDP, OTLP/HTTP, and OTLP/gRPC equally because
they live below the dispatch layer. Which means: in the original
bench, the OTLP HTTP path was hitting a backend that performs
*nothing like* the off-the-shelf SQL-backed OTLP receiver a customer
is actually running.

This is the trap every transport vendor falls into. You optimize
your backend, you measure end-to-end against your competitor's
transport into your backend, and your transport "wins" — but you
haven't actually shown your transport is faster, you've shown that
*your stack* is faster than your competitor's transport against
*your tuned stack*. Which is a different and far less interesting
claim.

The fix is conceptually simple, mechanically annoying. We added an
env flag, `ENTROPYOPS_STANDARD_BASELINE_MODE`, defaulting to `true`,
and bifurcated the behavior:

- **`standard-baseline` (default):** the OTLP HTTP and gRPC handlers
  route through new `WriteTracesPerRow` / `WriteMetricsPerRow` /
  `WriteLogsPerRow` methods on the storage backend. These are a
  single-transaction per-row INSERT loop — i.e. exactly what an
  untuned SQL-backed OTLP receiver would do. The SQLite tuning
  PRAGMAs revert to defaults (`synchronous=FULL`,
  `wal_autocheckpoint=1000`, no large mmap). The out-of-band WAL
  checkpointer goroutine doesn't start. Everything that made the
  in-tree backend "fast" is gated off.
- **`tuned` (opt-in via `ENTROPYOPS_STANDARD_BASELINE_MODE=false`):**
  every path uses the bulk + tuned backend. Useful for "is the WAL
  checkpointer actually doing its job?" self-tests; not useful for
  any external comparison.
- **AgenticUDP always uses the bulk path**, because receiver-level
  batching is part of the AgenticUDP product. It ships in any
  AgenticUDP deployment regardless of the storage tier.

The bench tool (`eo-ingest-bench`) probes
`/api/ingest/transports`, reads the active `bench_mode`, and writes
it into the JSON output. Every result file is now self-describing.
The server logs a loud `BENCH MODE = standard-baseline (DEFAULT)...`
banner at startup so any captured `server.log` is unambiguous.

The relevant unit tests:

- `internal/storage/baseline_mode_test.go` —
  `TestIsStandardBaselineMode_DefaultsToTrue` is load-bearing. If a
  future refactor flips the default, the cross-protocol bench
  silently goes back to lying.
- `internal/ingest/receiver/otlp_baseline_dispatch_test.go` —
  asserts OTLP HTTP/gRPC route to the per-row path in baseline mode
  and to the bulk path in tuned mode.
- `internal/storage/sqlite/checkpointer_test.go` —
  `TestCheckpointer_NotStartedInStandardBaselineMode` — explicitly
  prevents a "let's just always run the goroutine, it's harmless"
  refactor.

### What the methodology has had to absorb mid-flight

Both of the constraints in this section came from external review,
not from the original design. We're flagging them here because the
review process is itself part of the methodology, and any future
reader who skips it will mis-interpret the numbers.

1. **The standard-baseline / tuned bifurcation above** was added
   after a domain-expert reviewer pointed out that running OTLP
   HTTP/gRPC against the in-tree heavily-tuned SQLite backend is
   not the comparison anyone in the industry actually faces. The
   original numbers — also archived — looked even better for
   AgenticUDP, and were misleading for that reason.
2. **The 5000-span tail and average numbers were initially
   reported.** A reviewer with statistics fluency pointed out that
   p99 of n=20 is the single worst observation, p90 is the second
   worst, and average inherits the same outlier sensitivity — i.e.
   we were treating noise as a finding. The honest disclosure at
   n=20 is the sample size limitation, not the percentile values.
   Tail and average claims at 5000 spans are now held until the
   **n=10,000 follow-up** lands; see the open-investigation
   section. (The follow-up was originally scoped to n=1,000 — the
   commonly cited "stable p99" bar — and was bumped to n=10,000
   because the bench is cheap to run, the marginal cost of the
   bigger sample is hours not days, and at production volume the
   percentile that actually matters for SLO budgets is p99.9, which
   needs n≥10,000 to estimate honestly at all.)
3. **The bench tool's failure-accounting was asymmetric across
   paths.** Before the symmetric-accounting patch, the OTLP HTTP
   and gRPC implementations of `eo-ingest-bench` silently dropped
   failed iterations from the per-path sample distribution and
   stored only the *last* error string in a free-form `note`
   field; the AgenticUDP implementation counted timeouts and
   exposed protocol-level retransmit / drop counters separately.
   That asymmetry was an artifact of the original n=20 cold-burst
   design space, where HTTP/gRPC effectively never failed and only
   AgenticUDP had a meaningful failure mode worth characterizing.
   Under sustained load against the standard-baseline backend the
   failure-prone paths invert (HTTP/gRPC routinely exceed the 30s
   exporter timeout while AgenticUDP retx remains zero), so the
   asymmetric accounting was systematically *understating*
   HTTP/gRPC failures relative to AgenticUDP failures in any
   side-by-side comparison this tool produced — exactly the class
   of methodology error the standard-baseline-mode work was
   intended to avoid. The bench tool was patched to count and
   expose failures the same way for all three paths, with the
   structured fields `failures` / `timeouts` / `non_ok` /
   `other_errors` / `failure_rate_pct`, and the per-iteration
   wall-clock of failed iterations is retained in
   `failed_samples_ms` so the censored tail is recoverable for
   post-hoc analysis. A new status `PASS_WITH_FAILURES` covers
   the standard-baseline-mode-under-load case where some
   iterations succeeded and others timed out. The n=1,000
   follow-up at 5000 spans is being captured under the patched
   tool; the headline operator-facing number it produces will be
   the *failure rate* an exporter at its default timeout would
   see, which is a stronger and more actionable claim than the
   per-success-iteration percentile alone. Results to follow in
   the open-investigation section.

4. **The single-invocation run protocol embedded a cross-path
   order effect.** Before the isolated-per-path patch, the
   recommended way to run the bench was a single
   `eo-ingest-bench` invocation that exercised all three paths
   sequentially in fixed order: HTTP → gRPC → AgenticUDP. That
   is fine for the cold-burst n=20 design space (the DB barely
   grows in 60 seconds of bench traffic) but breaks down at any
   sustained-load N. Concretely: at n=1,000 with 5,000 spans per
   call, HTTP runs against a fresh `./data` (≈ 0 GB), gRPC runs
   against `./data` already grown by HTTP's 5 M trace rows
   (≈ 10–15 GB), and AgenticUDP runs against `./data` grown by
   HTTP's *and* gRPC's writes (≈ 20–30 GB). Standard-baseline-
   mode SQLite contention is itself a function of DB size — the
   B-tree depth grows, the page cache hit rate falls, and the
   inline auto-checkpoint stalls get bigger — so the three paths
   were not measuring against the same backend. The bias in this
   particular order was conservative (worst DB state for the
   path the writeup wanted to make the strongest claim about,
   AgenticUDP), but a conservative bias is still a bias and
   "these numbers were measured against differently-warmed DBs"
   is a footnote that doesn't belong in a publishable
   comparison. The fix: a `-paths` flag in `eo-ingest-bench`
   (`-paths http`, `-paths grpc`, `-paths udp`, or any
   combination; default `http,grpc,udp` preserves the old
   single-invocation behaviour for non-publishable runs) and a
   formalized run protocol that brings the server up with a
   fresh `./data`, runs one path, tears the server down, and
   repeats for each path. The per-path JSONs are then merged
   with `eo-bench-merge`; the merger detects this "cross-path
   isolated" pattern and suppresses the per-path
   present-in-N/M-chunks warnings that would otherwise fire and
   (under `-strict=true`) escalate to fatal. Headline n=1,000
   and n=10,000 numbers in this writeup are produced under that
   protocol; older single-invocation JSONs remain readable but
   are flagged with a methodology note rather than re-quoted as
   the headline numbers.

5. **The AgenticUDP client transport had a self-inflicted
   retransmit storm under sustained backpressure.** The first
   isolated-per-path n=1,000 run captured under constraint (4)
   produced an unexpectedly slow AgenticUDP path (~7h 34m
   wall-clock vs ~4h 24m for HTTP and ~5h 05m for gRPC), and the
   server-side log analysis showed why: for 5 M ingested spans
   the coalescer emitted 9,215 flushes (vs the ~2,500 expected
   from 2 K-row batching) with average flush time degrading from
   264 ms at the start to 5,887 ms at the end and a single worst
   flush of 70,011 ms; 99.4 % of flushes exceeded the 200 ms
   slow-flush threshold. The root cause was *not* in the storage
   layer — server-side `WriteTraces` durations on a clean DB
   were ~118 ms for 2 K rows, which is faster per-row than the
   per-row INSERT path used by HTTP/gRPC — it was in the client
   transport. The pre-fix `retransmitLoop` used a fixed 2-second
   timeout for every unACKed datagram and had no per-tick budget,
   so any time the server's coalescer flush blocked the UDP read
   loop long enough to delay an ACK by >2 s, every inflight
   datagram retransmitted on the next tick, the read loop fell
   further behind, and the storm recursively deepened. The fix
   (now landed in `entropyops-helper/internal/transport/agenticudp.go`):
   capped exponential backoff on the per-packet RTO
   (`baseRTO * 2^retries`, capped at 30 s) plus a per-tick
   retransmit budget (default 64 datagrams) so a brief stall
   produces a brief retransmit pulse, not an O(N) burst.
   Configurable via `ENTROPYOPS_AGENT_UDP_RETRANSMIT_MS`,
   `ENTROPYOPS_AGENT_UDP_RETRANSMIT_MAX_MS`, and
   `ENTROPYOPS_AGENT_UDP_RETRANSMIT_BUDGET` env vars; the bench
   tool exposes the base RTO directly as
   `-udp-retransmit-ms`. The n=1,000 AgenticUDP number is being
   re-captured under the patched transport; the prior client-side
   wall-clock value is retained as a "pre-fix transport" footnote
   for traceability but is not the headline figure.

6. **The published `spans_per_second` was per-iteration, not
   wall-clock; under sustained-load runs with timeouts the two
   diverge by the success/total ratio.** Under the prior schema
   each `PathResult` carried a single throughput field,
   `SpansPerSecond`, computed from the success-only `AvgMs` as
   `(spans_per_call * 1000) / avg_ms`. For an all-success run
   that is identical to the wall-clock throughput an operator
   would see on a stopwatch — the iter loop is sequential and
   has no idle gaps. For a run with timeouts it is not: timed-
   out iters consume wall clock (each spends up to `-udp-ack-
   timeout-ms` or the HTTP/gRPC client timeout) but contribute
   nothing to `AvgMs`, so the published throughput overstates
   the rate at which spans actually landed at the receiver.
   The asymmetry was discovered when an operator's stopwatch on
   an n=1,000 AgenticUDP run reported ~3 h of wall clock while
   the `spans_per_second` field implied ~7 h, and it is the
   same family of bias the failure-accounting fix in (4) was
   meant to remove. The fix: every `PathResult` now carries
   four additional first-class fields — `started_at`,
   `finished_at`, `wall_clock_seconds`, and
   `spans_per_second_wall_clock`, the last computed as
   `(attempted * spans_per_call) / wall_clock_seconds` so the
   numerator includes failed iters and matches what an operator
   actually experienced. The bench tool records the start
   timestamp after warmup and the end timestamp after the iter
   loop returns, so warmup is excluded (matching the existing
   `AvgMs` convention). The merge tool sums per-chunk
   `wall_clock_seconds` across chunks and recomputes
   `spans_per_second_wall_clock` from the merged `attempted`
   total — a chunked n=10,000 run carries a single honest
   stopwatch number, not a ratio derived from an artificially
   small denominator. The pre-existing `spans_per_second` field
   is preserved unchanged for "how fast is one healthy request"
   comparisons (it is the right number for per-iter
   comparisons across paths); the new
   `spans_per_second_wall_clock` is the publishable number for
   "how long did the run take." The bench's printed table now
   shows both side-by-side so a divergence between them — the
   diagnostic signal of a failure-dominated run — is visible at
   a glance instead of buried in JSON.

If a seventh constraint surfaces in subsequent review, this section
will absorb it the same way. The point is not that the numbers are
final; the point is that the methodology is supposed to make
"these numbers were wrong because of X" a *one-edit* fix instead
of a credibility-loss event.

---

## The numbers

Captured on Windows loopback, `bench_mode=standard-baseline`, three
sizes. Raw JSON archived under
[`docs/operator/bench-results/bench-{200,1000,5000}-baseline.json`](../operator/bench-results/)
(`generated_at` 2026-04-29 UTC in each file). Tables in this section were
reconciled against those artifacts on 2026-05-06; there is no newer
standard-baseline JSON in-tree yet — when the isolated n=10,000 follow-up
lands, archive it under `bench-results/` and refresh this section in the same
pass.

### 200-span batches (50 iterations, 5 warmup)

| Path | p50 | p90 | p99 | spans/sec | retx | drops |
|---|---|---|---|---|---|---|
| OTLP HTTP | 169.43 ms | 217.40 ms | 292.03 ms | 1,199 | n/a | n/a |
| OTLP gRPC | 228.03 ms | 271.27 ms | 295.68 ms | 860 | n/a | n/a |
| **AgenticUDP** | **3.96 ms** | **6.29 ms** | **10.04 ms** | **46,548** | 0 | 0 |

AgenticUDP wins on every percentile. p50 is 42.8× lower than HTTP.
Throughput is 38.8× higher.

### 1000-span batches (30 iterations, 3 warmup)

| Path | p50 | p90 | p99 | spans/sec | retx | drops |
|---|---|---|---|---|---|---|
| OTLP HTTP | 511.03 ms | 597.78 ms | 635.20 ms | 1,976 | n/a | n/a |
| OTLP gRPC | 714.15 ms | 824.16 ms | 909.70 ms | 1,407 | n/a | n/a |
| **AgenticUDP** | **38.56 ms** | **45.64 ms** | **51.32 ms** | **25,122** | 0 | 0 |

Same story at scale. p50 is 13.2× lower. The chunking path activated
(2,211 datagrams over 30 iterations), recovered everything, dropped
nothing.

### 5000-span batches (20 iterations, 2 warmup)

We are intentionally reporting only the median at this batch size.
The raw bench output contains p90, p99, avg, max etc., and is
archived so anyone can recompute, but with **n=20 those statistics
are not estimators we will publish as findings.** See "What we are
NOT reporting at 5000 spans" below.

| Path | p50 | retx | drops |
|---|---|---|---|
| OTLP HTTP | 1,669 ms | n/a | n/a |
| OTLP gRPC | 2,260 ms | n/a | n/a |
| **AgenticUDP** | **186 ms** (9.0× lower than HTTP) | 208 | 0 |

The architectural claim — **AgenticUDP's median is roughly an order
of magnitude lower than OTLP/HTTP at every batch size we tested** —
holds at 5000 spans the same way it does at 200 and 1000.

Retransmits = 208, drops = 0. The protocol recovered every chunk;
no datagram was lost, the receiver eventually ACKed everything.

#### What we are NOT reporting at 5000 spans

p99 of n=20 is the single worst iteration. p90 of n=20 is the
second worst. Average over n=20 with any heavy-tail behaviour
inherits the same outlier sensitivity. Reporting those numbers as
"AgenticUDP p99 vs HTTP p99" would be reporting noise as
measurement. They are in the raw JSON; they are not in this table
and we are not interpreting them in this section.

A **10,000-iteration run at 5000 spans is in progress.** That
sample size lets the 9,900th-ranked observation act as a p99
estimator with neighbours 9,899 and 9,901 to sanity-check
stability, and the 9,990th observation to give an honest first
read on p99.9 — the percentile that actually matters for SLO
budgets at production volume. The full ladder we use:

- n ≥ 10,000 → publishable p99 with stable neighbours, p99.9
  starts to be meaningful.
- n ≥ 1,000  → defensible p99 (the original "follow-up" target;
  bumped up to 10,000 because the run is cheap and the marginal
  precision matters at this batch size).
- n ≈ 100   → bare minimum to say anything about p99.
- n ≤ 20    → medians only. Not in the tail conversation.

That follow-up will resolve to one of three outcomes — and any of
them is publishable, so the 5000-span tail story is delayed, not
shelved:

1. **p99 across n=10,000 clusters tightly around some value
   (and replicates across reruns).** The tail is real and
   characterized. The storage-side stall hypothesis (next
   section) becomes the thing to fix.
2. **p99 jumps around across multiple n=10,000 runs.** The tail
   is environmental noise (Windows fsync, GC pauses, antivirus,
   host contention) and the original n=20 result was misleading.
3. **p99 collapses to near-p50 levels.** The n=20 outliers were
   unrepresentative; there is no bimodal tail and the
   coalescer-stall hypothesis dissolves.

We will publish the n=10,000 result and the analysis behind
whichever outcome lands. The architectural p50 claim does not
depend on which one it is.

#### What about throughput at 5000 spans?

We are also not reporting spans/sec at 5000 spans as a transport
finding. spans/sec is computed from total elapsed time per request;
it inherits exactly the same outlier sensitivity as the average,
so under-sampled tail iterations distort it the same way they
distort p99. The throughput claim — **AgenticUDP delivers roughly
an order of magnitude more spans/sec to storage than OTLP/HTTP
under the same workload** — stands at 200 spans (38.8×, n=50) and
1000 spans (12.7×, n=30), where the tail is clean enough for the
mean to be meaningful. At 5000 spans the spans/sec number waits on
the same n=10,000 follow-up.

---

## Beyond throughput: what cheaper per-event delivery does to data quality

The latency numbers above are what the bench directly measured.
The more interesting question — and the reason this work is worth
publishing rather than just shipping — is what the numbers *imply*
about instrumentation choices upstream of the wire.

Modern observability has a load-bearing compromise that almost
nobody flags as a compromise: **transport cost forces SDKs to
degrade data on the way out**, and the degradation is what produces
most of the cardinality and data-quality pain operators spend their
time fighting. Specifically, OTLP/HTTP and OTLP/gRPC's per-request
overhead pushes instrumentation authors toward four defensive
patterns:

1. **Coarse SDK-side batching.** Every OTel SDK ships with a
   `BatchSpanProcessor` that buffers spans for 5–30 s before sending,
   because per-request HTTP cost makes 100ms-batch flushes
   uneconomical. Net effect: alerting and debugging see stale data
   by exactly that interval.
2. **Aggressive head/tail sampling.** 1% trace sampling is a typical
   default at scale. Every dropped trace is debugging context the
   operator will need exactly when they don't have it — usually
   during the incident.
3. **Attribute truncation.** OTel's spec has explicit max-attribute-
   value-length limits (default 128 chars) and max-attribute-count
   limits (default 128 per span) so payloads stay manageable on the
   wire. Most useful debug context — full SQL queries, full HTTP
   bodies, full stack traces — exceeds those limits and gets
   truncated at the SDK before the wire even sees it.
4. **Cardinality pressure on the metrics path.** Because each unique
   label combination is a new time series at the storage tier, AND
   because every additional label increases the per-request payload
   size, instrumentation authors and storage admins both push back on
   "rich labels per metric." The result is the metrics-vs-events
   split observability stacks have today: low-cardinality metrics
   with thin labels, separate event/log/trace pillars carrying the
   actual context. That split is partly a real query-engine
   constraint, but partly an artifact of the transport making rich
   per-event delivery expensive.

What changes structurally with a UDP-datagram transport that has a
receiver-level coalescer + tier-aware delivery + content-id dedup:

- **Per-cycle amortization happens at the transport, not the SDK.**
  One collection cycle (every 1 s, say) becomes a small number of
  datagrams whether the SDK buffered them or not. So the SDK's
  `BatchSpanProcessor` interval can drop from 10 s to ~1 s without
  multiplying the per-request cost. Alerting sees fresher data.
- **Resource attributes can be sent once per cycle and referenced.**
  Today, every span repeats the same 30-50 resource attribute fields
  (k8s pod, namespace, container, image, region, az, deployment,
  version, ...) on the wire. With cycle-batched datagrams + content-
  id dedup, the resource block is paid once per cycle. That budget
  freed up at the wire is budget the SDK can spend on richer
  per-span attributes — full SQL, full request bodies, full
  exemplars — without truncation.
- **Sampling can move from the SDK toward the storage tier.** When
  per-request transport cost stops being the bottleneck, the
  pressure to sample at 1% relaxes. Best-effort tier (e.g. detailed
  span attributes) can ship at 100% and the storage tier can
  decide what to retain after-the-fact based on actual query
  patterns, instead of throwing data away at the source.
- **The metrics-vs-events split can soften.** When rich-attribute
  per-event delivery is cheap, you can run wide-event observability
  ([Charity Majors-style](https://www.honeycomb.io/blog/observability-a-manifesto)
  high-cardinality structured events) at scale without the
  transport tax that historically forced everyone back into thin
  metrics.

**Important caveat.** None of those four points were directly
measured by the bench in the previous section. They are *implied*
by the throughput numbers (38× higher spans/sec at 200 spans means
38× headroom for richer payloads at the same wire cost) but they
are not *proved* by them. The benches that would prove them
directly are:

1. **A "rich-attribute" bench**: same span count, attribute payload
   scaled from 1 KB to 100 KB per span, same three transports.
   Measures whether AgenticUDP's headroom translates into
   tolerating bigger per-event payloads.
2. **A "fresh-data" bench**: vary the SDK-side
   `BatchSpanProcessor` flush interval from 1 s to 30 s, hold span
   rate constant, measure end-to-end latency from `span.End()` on
   the SDK to row-visible-in-storage on the server. Shows whether
   AgenticUDP lets you economically run shorter intervals.
3. **A "100% sampling at constant cost" bench**: same total span
   count, run once at 100% via AgenticUDP and once at 1% sampling
   via OTLP/HTTP, compare both backend cost and operator-visible
   coverage during a synthetic incident.

These are on our list. We have not run them. If they interest your
shop more than ours, we'd love to read your results.

The headline framing is: **the throughput gap isn't the actual
product story. The product story is what stops being a forced
choice once the throughput gap exists.** AgenticUDP is, in that
sense, less interesting as a faster transport and more interesting
as a transport that doesn't make instrumentation authors choose
between rich data and affordable delivery.

---

## Reproduction harness

You need: a release zip and 5 minutes. No build toolchain required.

### 1. Get the binaries

Either build from source —

```bash
git clone https://github.com/entropyops/entropyops
cd entropyops
make build-release
# release/ now contains entropyops-{darwin,linux,windows}-{amd64,arm64}.zip
# and eo-ingest-bench-{...}.zip
```

— or grab them from the release page on the same repo. You need
exactly two binaries: `entropyops-<platform>` (the server) and
`eo-ingest-bench-<platform>` (the bench client).

### 2. Run the server

```bash
export ENTROPYOPS_DEPLOYMENT_MODE=appliance
export ENTROPYOPS_HTTP_PORT=8000
export ENTROPYOPS_GRPC_PORT=4317
export ENTROPYOPS_ENABLE_AGENTIC_UDP=true
export ENTROPYOPS_AGENTIC_UDP_PORT=4320
export ENTROPYOPS_AGENTIC_UDP_RCVBUF_BYTES=67108864
# IMPORTANT: leave ENTROPYOPS_STANDARD_BASELINE_MODE unset for the
# realistic comparison. Setting it to "false" gives you the
# self-test (tuned) configuration instead.

./entropyops-linux-amd64
```

The first line of stdout MUST read:

```
BENCH MODE = standard-baseline (DEFAULT). OTLP HTTP/gRPC use per-row INSERT;
SQLite uses default PRAGMAs (FULL fsync, autocheckpoint=1000); out-of-band
WAL checkpointer NOT started. AgenticUDP keeps its bulk-write fast path. ...
```

If it doesn't, the binary is too old or you accidentally set the env
var. Stop and fix.

### 3. Run the bench

The bench tool defaults to running all three paths in a single
invocation, sequentially in the order HTTP → gRPC → AgenticUDP.
That is the right ergonomic for a quick smoke or a low-N
exploratory run, and it is what `-paths` defaults to:

```bash
./eo-ingest-bench-linux-amd64 \
  -server http://localhost:8000 \
  -grpc localhost:4317 \
  -udp 127.0.0.1:4320 \
  -tenant Trading-dev \
  -spans 1000 -iters 30 -warmup 3 \
  -keep-samples \
  -json bench-1000-baseline.json
```

`-keep-samples` retains both the successful sample distribution AND
the per-iteration wall-clock of any failed iterations
(`failed_samples_ms`) in the JSON so the censored tail is
recoverable. Strongly recommended at any non-toy `-iters` count;
the JSON gets larger but the symmetric-failure-accounting story in
methodology constraint #3 depends on these arrays being present.

The bench tool's stdout will print `server bench_mode:
standard-baseline (...)` in the header so the active mode travels
with the result. The JSON output also embeds it as `bench_mode`.

**For publishable cross-path numbers at any sustained-load N**
(the n=1,000 follow-up at 5,000 spans and the n=10,000
follow-up referenced in the open-investigation section below),
use the *isolated-per-path* protocol instead. The single-
invocation form shown above runs all three paths against the same
backing `./data` in fixed order, so the second and third paths
measure against a DB that the previous path has already grown
(see methodology constraint #4 for the full explanation).
Isolated-per-path eliminates that order effect by restarting the
server with a fresh `./data` between each path:

```bash
# 1. Server up, fresh ./data, then HTTP only.
rm -rf ./data && ./entropyops-${SUFFIX} &  SERVER=$!
sleep 5
./eo-ingest-bench-${SUFFIX} \
  -server http://localhost:8000 -grpc localhost:4317 -udp 127.0.0.1:4320 \
  -tenant Trading-dev -spans 5000 -iters 1000 -warmup 10 \
  -keep-samples -udp-ack-timeout-ms 60000 \
  -paths http -json bench-5000-symmetric-http.json
kill $SERVER && wait $SERVER 2>/dev/null

# 2. Server up, fresh ./data, then gRPC only.
rm -rf ./data && ./entropyops-${SUFFIX} &  SERVER=$!
sleep 5
./eo-ingest-bench-${SUFFIX} \
  -server http://localhost:8000 -grpc localhost:4317 -udp 127.0.0.1:4320 \
  -tenant Trading-dev -spans 5000 -iters 1000 -warmup 10 \
  -keep-samples -udp-ack-timeout-ms 60000 \
  -paths grpc -json bench-5000-symmetric-grpc.json
kill $SERVER && wait $SERVER 2>/dev/null

# 3. Server up, fresh ./data, then AgenticUDP only.
rm -rf ./data && ./entropyops-${SUFFIX} &  SERVER=$!
sleep 5
./eo-ingest-bench-${SUFFIX} \
  -server http://localhost:8000 -grpc localhost:4317 -udp 127.0.0.1:4320 \
  -tenant Trading-dev -spans 5000 -iters 1000 -warmup 10 \
  -keep-samples -udp-ack-timeout-ms 60000 \
  -paths udp -json bench-5000-symmetric-udp.json
kill $SERVER && wait $SERVER 2>/dev/null

# 4. Merge the three single-path JSONs into one multi-path JSON.
./eo-bench-merge-${SUFFIX} \
  bench-5000-symmetric-http.json \
  bench-5000-symmetric-grpc.json \
  bench-5000-symmetric-udp.json \
  -out bench-5000-symmetric-merged.json -summary
```

The merger automatically detects the cross-path-isolated pattern
(each chunk contributes a disjoint set of paths) and suppresses
the per-path "present in 1/3 chunks" warnings that would
otherwise fire and (under the `-strict=true` default) escalate to
a fatal error. The merged JSON has the exact same shape as a
single-invocation run plus a `merged_from` array recording the
per-chunk SHA-256 so the result is reproducible from the same
chunks.

The Windows / PowerShell equivalent of this protocol is in
[`docs/operator/INGEST_BENCH_QUICKSTART.md`](../operator/INGEST_BENCH_QUICKSTART.md)
("Isolated-per-path protocol" section).

### 4. Read the JSON

The schema lives in
[`entropyops-helper/internal/benchstats/stats.go`](../../entropyops-helper/internal/benchstats/stats.go)
(the `PathResult` struct). Key fields:

- `bench_mode` — the active server-side mode at run time.
- `results[].n` — number of **successful** iterations the
  percentile fields below were computed from. NOT the same as the
  `iterations` parameter; see `attempted` and `failures`.
- `results[].p50_ms`, `p90_ms`, `p99_ms` — client wall-clock per
  successful request.
- `results[].ms_per_span_p50` — normalises across batch sizes; the
  cross-path comparable.
- `results[].attempted` — `n + failures`. Equals the `iterations`
  parameter minus warmup unless something prevented an iteration
  from being attempted (e.g. a UDP handshake failure aborts the
  path before any iteration runs).
- `results[].failures`, `timeouts`, `non_ok`, `other_errors` —
  symmetric across HTTP / gRPC / AgenticUDP. `failures` is the
  total; the other three are the categorical breakdown.
- `results[].failure_rate_pct` — `100 * failures / attempted`.
  This is the headline operator-facing number a customer's exporter
  with its default timeout would see as a drop rate.
- `results[].failed_samples_ms` — per-iteration wall-clock for the
  failed iterations (only present when the bench was invoked with
  `-keep-samples`). Recovers the censored tail for post-hoc
  analysis.
- `results[].udp_sent`, `udp_acked`, `udp_retransmits`,
  `udp_dropped` — AgenticUDP packet-level counters, first-class
  fields. (Older artifacts encoded these in the free-form `note`
  string; see methodology constraint #3.)
- `results[].status` — `PASS` (no failures), `PASS_WITH_FAILURES`
  (some successful samples + at least one failure),
  `FAIL_TIMEOUT` / `FAIL_DIAL` / `FAIL_HANDSHAKE` /
  `FAIL_NO_DATA` (every attempt failed).

#### Reading the JSON in PowerShell — normalize blank cells to zero

PowerShell's `ConvertFrom-Json` faithfully represents JSON-elided
fields (the `omitempty` ones in the schema above —
`failures` / `timeouts` / `non_ok` / `other_errors` /
`failure_rate_pct` / `udp_*` / `attempted`) as `$null`, and
`Format-Table` then renders them as **blank cells, not as `0`**.
Visually that's the same hazard symmetric-failure-accounting was
introduced to eliminate: a quick scan of a results table can
misread "this path's failures field is blank" as "we don't know
how many failures it had" rather than the correct "zero
failures." Cast at the read boundary so every cell renders as
either an explicit number or an explicit zero:

```powershell
$rows = (Get-Content .\bench-5000-baseline-symmetric-n1000.json -Raw |
         ConvertFrom-Json).results |
    ForEach-Object {
        [PSCustomObject]@{
            path             = $_.path
            n                = [int]$_.n
            attempted        = [int]$_.attempted
            failures         = [int]$_.failures
            timeouts         = [int]$_.timeouts
            non_ok           = [int]$_.non_ok
            other_errors     = [int]$_.other_errors
            failure_rate_pct = [double]$_.failure_rate_pct
            p50_ms           = [double]$_.p50_ms
            p99_ms           = [double]$_.p99_ms
            ms_per_span_p50  = [double]$_.ms_per_span_p50
            udp_sent         = [int]$_.udp_sent
            udp_acked        = [int]$_.udp_acked
            udp_retransmits  = [int]$_.udp_retransmits
            udp_dropped      = [int]$_.udp_dropped
            status           = $_.status
        }
    }
$rows | Format-Table -AutoSize
```

`[int]$null` evaluates to `0` and `[double]$null` to `0.0`, so the
cast is safe across both omitted and present fields. Every
PowerShell consumer of this JSON in a publishable comparison
should use this read pattern (or the obvious bash/jq / Python /
Go equivalent: treat absent failure fields as explicit zeros at
decode time). Mixing patched-tool and old-tool JSONs through this
reader is also safe — old chunks decode cleanly with all the new
fields at zero, since the old tool literally couldn't tell us
otherwise.

---

## What we did NOT prove (and what to break)

Things that would change these numbers — and that we'd love to see
someone else measure.

### Single-host loopback only

We ran on Windows localhost. No real network, no NAT, no firewall,
no L7 proxy. WAN behaviour is exactly where TCP head-of-line is most
expensive and UDP's lack of ordering matters most. **WAN numbers
should move the ranking, probably in AgenticUDP's favor at the tail.**

### In-tree SQLite, not a real third-party OTLP backend

The "off-the-shelf-equivalent" SQLite in standard-baseline mode is an
*approximation* of what a customer's actual OTLP backend (Jaeger,
Tempo, OTel Collector + Cassandra/Elasticsearch, Datadog Agent,
Honeycomb, anything else) looks like. Approximation is not the real
thing.

The right next step is the rig in
[`THIRD_PARTY_BACKEND_BENCH_PLAN.md`](../operator/THIRD_PARTY_BACKEND_BENCH_PLAN.md)
— point `eo-ingest-bench`'s `-server` and `-grpc` flags at an actual
Jaeger or OTel Collector debug exporter while leaving `-udp` against
EntropyOps. The bench tool already accepts split targets, so this is
a CLI swap. The plan doc lists work items to make it first-class
(target labelling, smoke-check fail-fast, pinned docker-compose
reference rig).

We have not run this yet. If you do, please publish the JSON. If
your numbers contradict ours, you'll have learned something
interesting; we'll have learned more.

### Handler asymmetry inside EntropyOps

The OTLP HTTP traces handler runs the head/tail sampler and the
k8s-attribute joiner inline. The OTLP gRPC handler skips the
sampler. The AgenticUDP receiver skips both. So some of AgenticUDP's
lead is real protocol/storage advantage and some is enrichment-work
asymmetry. The plan to close it is in
[`HANDLER_PARITY_PLAN.md`](../operator/HANDLER_PARITY_PLAN.md). Until
that lands, AgenticUDP's numbers are an upper bound and HTTP's are a
lower bound.

### Other things that might matter and we didn't test

- **OS variance.** Windows fsync, Linux fsync, and macOS fsync
  behave differently. Storage tail latency is a likely-culprit
  delta.
- **Larger batches.** We stopped at 5000 spans. 10k, 100k would
  exercise the chunking path harder.
- **Heavier attribute payloads.** Our spans have ~5 attributes. Real
  k8s-tagged spans can have 30+. JSON size matters for both the
  parse path and the storage path.
- **Mixed-tenant load.** Multiple sources contending for the same
  receiver socket and the same SQLite write connection.
- **Different OTel collectors.** Jaeger 1.62, Tempo, OTel
  Collector contrib, Datadog Agent, Splunk OTel Collector — they
  perform very differently from each other and from our SQLite
  approximation.

If you try any of these and they break our numbers, please open an
issue with the JSON.

---

## Open investigation: characterizing 5000-span tail behaviour

We do not yet have enough samples to say whether AgenticUDP has a
tail problem at 5000 spans. This section is the design of the run
that will answer that, and the hypotheses we'd investigate IF the
answer turns out to be "yes, there's a real tail."

### What the data does and does not tell us today

n=20 at 5000 spans gives us a defensible median (AgenticUDP p50 =
186 ms vs OTLP HTTP p50 = 1,669 ms, a 9.0× win). It does not give
us a defensible tail estimator: p99 of 20 is the single worst
iteration, p90 is the second worst, average over n=20 with any
heavy-tail behaviour is dominated by those same one or two
samples. We are therefore not publishing tail or average numbers
at 5000 spans, and we are not using the n=20 results as evidence
for or against a tail.

The smaller batch sizes are different. At 200 spans (n=50) and
1000 spans (n=30) AgenticUDP wins on every percentile reported in
the JSON, including p99, with no retransmits and no drops. The
tail problem, if it exists, is a 5000-span phenomenon.

### The follow-up run

A **10,000-iteration run at 5000 spans is in progress** (replicated
three times, separate JSON files, freshly-booted server each time).
That sample size lets the 9,900th-ranked observation act as a p99
estimator with neighbours 9,899 and 9,901 to sanity-check
stability, and the 9,990th to give an honest first read on p99.9
(the percentile that actually matters for SLO budgets at production
volume, and the percentile a smaller n cannot estimate at all).
n=1,000 was the original "defensible p99" target; we bumped to
10,000 because the bench is cheap to run and the marginal precision
matters at the batch size where we suspect a tail. n=100 is the
bare minimum to say anything about p99 with a straight face. n=20
is not in the conversation.

The run resolves to one of three outcomes. All three are
publishable.

1. **p99 across n=10,000 clusters tightly across replicas, and
   p99.9 lands at a stable multiple of p99.** The tail is real and
   characterized; the storage-side hypotheses below become the
   things to fix.
2. **p99 jumps around across the three n=10,000 replicas.** The
   tail is environmental noise (Windows fsync, GC pauses, antivirus,
   host contention) and the original n=20 result was misleading —
   we'd publish that as a methodological note.
3. **p99 collapses to near-p50 levels.** The n=20 outliers were
   unrepresentative; there is no bimodal tail and the
   coalescer-stall hypothesis dissolves. Publishable as "AgenticUDP
   stays sub-300 ms across 10,000 iterations at 5000 spans."

### Hypotheses to investigate IF outcome (1) lands

These are not load-bearing on the current published numbers; they
are the diagnostic checklist we'd run if the n=10,000 follow-up
confirms a tail. They're documented now so the investigation is
pre-registered, not retrofitted.

### The mechanism we'd interrogate first

The AgenticUDP receiver does this on every datagram:

```
read from socket -> validate -> push to coalescer -> ack
```

The coalescer flushes every 50 ms or every 2000 items, whichever
hits first. A flush calls `WriteTraces(2000 spans)` synchronously.
We have separately observed (in pre-baseline tuned-mode runs and
in the boot-time instrumentation logs) individual `WriteTraces`
calls taking 600–900 ms on Windows even with multi-row bulk
INSERT — a workload that completes in 11–14 ms in unit tests on
macOS. That 50–80× gap between local and production is real
storage-side behaviour we have measured directly; it is the
*candidate* mechanism for any 5000-span tail, but until the
n=10,000 follow-up shows that the tail itself is real, we are not
linking the two as cause and effect.

The boot-line `sqlite: bulk-insert chunk size = 100 rows/statement
(per-row INSERT loop replaced); slow-write log threshold = 100ms`
confirms which binary is running, and the runtime line
`sqlite: SLOW WRITE WriteTraces rows=N chunks=M duration=Xms
(threshold=100ms, chunk_size=100)` separates SQL write time from
connection-pool wait time. If outcome (1) lands, those two log
lines plus the per-flush coalescer histogram are how we'd
attribute the stall.

### Hypotheses, in priority order (pre-registered for outcome (1))

1. **Single-writer connection contention.** SQLite is configured
   with `db.SetMaxOpenConns(1)` (writer-locked anyway). The host
   scraper, self-obs collector, pipeline analytics writer, and
   change-event handler all write through the same connection. If
   one of them holds the writer for 800 ms, the AgenticUDP
   coalescer's flush queues behind it.
2. **Windows fsync overhead.** Even with `synchronous=NORMAL`, large
   commits on Windows NTFS are slower than on Linux ext4 by a
   non-trivial factor.
3. **Coalescer back-pressure.** The coalescer channel is sized to
   4096. If a slow flush keeps the goroutine busy, incoming
   datagrams fan into a backlog that the next flush has to
   absorb — a feedback loop that, IF the bimodal hypothesis holds
   at higher n, would produce exactly the shape we'd see.

The `agenticudp: SLOW FLUSH signal=traces ... items=N duration=Xms`
log line and the periodic `agenticudp: flush_stats ... slow=K
max_ms=Y avg_ms=Z` summary are how we'll attribute the stall on the
next run.

### Why this is interesting beyond AgenticUDP

The general pattern — "fast transport in front of a single-writer
storage tier eventually sees the storage tail bleed through into the
transport tail" — applies to anything that accumulates incoming
events and flushes them in batches against a serial backend. Kafka
producers, Postgres logical replication subscribers, OTel Collector
batch exporters, Loki ingesters. The mitigation toolbox is well
known (parallel writers, queue priority, write-ahead logs with
batched commits) but the right combination depends on the workload.
We'd be glad to read other people's flame graphs for the same
pattern.

---

## Protocol Changes in the New Binaries (2026-05-01)

The `eo-ingest-bench-windows-amd64.exe` binary shipping with this writeup includes two rounds of protocol changes since the last published numbers. Both are in the V2 transport (the one the bench measures).

### What changed in V2

**Inflight key wraparound fix** (silent correctness bug, not a performance fix):

The inflight map keyed on `uint32(streamID)<<16 | uint32(seq)` where `seq` is a 16-bit wire field. After 65,535 sends in a session, `seq` wraps to 0 and the new entry silently overwrites the oldest entry with the same wrapped key, losing its retransmit record. Long benchmark runs (n=10,000 at 5,000 spans/iter) are exactly the workload that triggers this. The fix uses a monotonic `uint32` session counter as the map key and performs a linear scan to match ACKs by wire `seq+streamID`. The inflight map is bounded by `maxInflight` (default 64) so the scan is O(1) in practice.

**`acked` counter overcounting fix** (was inflating `spans/sec_wall`):

Duplicate ACKs from the server (a legitimate response to retransmits) were incrementing `acked` even when the corresponding entry had already been removed from the inflight map. The drain condition in `runUDP` fires when `acked - ackedAtStart >= totalMainSent`. Duplicate ACKs could fire this early, making `WallClockSeconds` shorter than the actual delivery time and inflating `spans/sec_wall`. The fix only increments `acked` when the entry is actually found-and-removed.

**`pktConfig` ACK** (enables bidirectional reliability):

When the server pushes a `pktConfig` datagram (OTEL collector config or pipeline task), the client now ACKs it. Previously the server sent it fire-and-forget from its perspective because no ACK came back. The ACK enables a server-side retry loop for reliable server→client task delivery.

**`congestionTunerLoop`** (closes the AI feedback loop):

The `CongestionPredictor` was running, computing predictions, and logging them — but no code read the output to adjust any send parameter. `congestionTunerLoop` now reads `Predict().RecommendedAction` every 5 s and scales `maxInflight` accordingly: pace→50%, reduce→25%, defer→12.5%, normal→100% of the session ceiling. You will see `maxInflight` oscillate in the bench output under sustained load.

### What is V3 and why it is not in this bench

The V3 protocol adds an 8-byte framing prefix to every data datagram (message ID + fragment offset + fragment total), a new packet type (`pktSACK` = type 4) for selective ACK of multiple fragments in one packet, and transparent fragmentation for payloads larger than 64,978 bytes.

V3 is **not benchmarked here** because the server module has not yet been updated to parse the 22-byte V3 header. The V3 client (`ClientV3`) is complete and all unit tests pass; it is the client that will be benchmarked once the server ships V3 support. The expected perf delta vs V2 is small: the 8-byte header increase reduces data capacity per datagram by 8 bytes (64,986 → 64,978), an unmeasurable difference for any payload larger than a few bytes.

What V3 will materially improve:

| Scenario | V2 behaviour | V3 behaviour |
|---|---|---|
| Payload > 64 KB (e.g. LLM response) | Application must chunk at JSON level | Transport fragments transparently; application sends full payload |
| Bulk ACK (server confirms many fragments) | One ACK per datagram; N datagrams = N ACK packets | One `pktSACK` acknowledges N seqs; N datagrams = 1–few ACK packets |
| AI pipeline result return | Application manages streamID manually | `SendTaskResult(task, result)` routes on `task.StreamID` automatically |

The benchmarkable claim for V3 is: for payloads that require fragmentation (>64 KB), V3 `spans/sec_wall` should match V2 closely because fragment-level ACKs are individually tracked — the same retransmit machinery applies. The ACK count reduction from SACK is the only material difference, and it will show up as lower server-to-client network overhead, not higher client throughput.

---

## What we'd like the community to do with this

1. **Reproduce.** Pull the binaries (or build from source). Run the
   harness. Compare your JSON to ours. Publish the result, especially
   if it disagrees.
2. **Run Plan B.** Stand up Jaeger or OTel Collector debug exporter,
   point `-server` and `-grpc` at it, leave `-udp` against
   EntropyOps. The bench tool already supports this. We haven't done
   it; we'd love to read your numbers before we do.
3. **Help us solve the 5000-span tail.** If you've debugged a
   similar coalescer-vs-serial-backend stall before, the
   instrumentation is in place; the relevant log lines are
   `agenticudp: SLOW FLUSH` and `sqlite: SLOW WRITE`. Drop a comment
   on the issue tracker.
4. **Try to break the methodology.** The standard-baseline mode is
   our best attempt at making the comparison fair. If you can show
   it's still tilted — e.g. our per-row INSERT path is somehow doing
   less work than a real Jaeger backend would — we want to know.

The JSON files, the bench tool source, the bench-mode flag
implementation, and this writeup are all in the same git repo.
Everything is there to break.

---

## Pointers

**Bench and transport (V2)**
- Bench tool source: [`entropyops-helper/cmd/eo-ingest-bench/main.go`](../../entropyops-helper/cmd/eo-ingest-bench/main.go)
- AgenticUDP V2 transport: [`entropyops-helper/internal/transport/agenticudp.go`](../../entropyops-helper/internal/transport/agenticudp.go)
- Pipeline task helpers (V2): [`entropyops-helper/internal/transport/agenticudp_pipeline.go`](../../entropyops-helper/internal/transport/agenticudp_pipeline.go)
- V2 transport tests: [`entropyops-helper/internal/transport/agenticudp_test.go`](../../entropyops-helper/internal/transport/agenticudp_test.go)

**V3 (Agentic AI UDP)**
- ClientV3 + wire protocol: [`entropyops-helper/internal/transport/agenticudp_v3.go`](../../entropyops-helper/internal/transport/agenticudp_v3.go)
- V3 tests (fragmentation, SACK, pipeline dispatch): [`entropyops-helper/internal/transport/agenticudp_v3_test.go`](../../entropyops-helper/internal/transport/agenticudp_v3_test.go)

**AI modules**
- Tier classifier (LLM + Thompson Sampling + TTL cache): [`entropyops-helper/internal/transport/ai_classifier.go`](../../entropyops-helper/internal/transport/ai_classifier.go)
- Congestion predictor: [`entropyops-helper/internal/transport/ai_congestion.go`](../../entropyops-helper/internal/transport/ai_congestion.go)
- Feature extractor: [`entropyops-helper/internal/transport/ai_features.go`](../../entropyops-helper/internal/transport/ai_features.go)

**Server-side**
- Bench-mode flag implementation: [`entropyops-v2/internal/storage/baseline_mode.go`](../../entropyops-v2/internal/storage/baseline_mode.go)
- Per-row write path (standard-baseline storage): [`entropyops-v2/internal/storage/sqlite/client.go`](../../entropyops-v2/internal/storage/sqlite/client.go) (search `WriteTracesPerRow`)
- Receiver dispatch helper: [`entropyops-v2/internal/ingest/receiver/otlp_baseline_dispatch.go`](../../entropyops-v2/internal/ingest/receiver/otlp_baseline_dispatch.go)
- AgenticUDP coalescer + flush instrumentation: [`entropyops-v2/internal/ingest/receiver/agenticudp_coalescer.go`](../../entropyops-v2/internal/ingest/receiver/agenticudp_coalescer.go)

**Operator docs**
- Step-by-step runbook: [`docs/operator/INGEST_BENCH_QUICKSTART.md`](../operator/INGEST_BENCH_QUICKSTART.md)
- Plan B (third-party backend): [`docs/operator/THIRD_PARTY_BACKEND_BENCH_PLAN.md`](../operator/THIRD_PARTY_BACKEND_BENCH_PLAN.md)
- Handler parity work: [`docs/operator/HANDLER_PARITY_PLAN.md`](../operator/HANDLER_PARITY_PLAN.md)
- Raw JSON results: [`docs/operator/bench-results/bench-{200,1000,5000}-baseline.json`](../operator/bench-results/)

**Agentic AI docs**
- Integration plan (phases + implementation status): [`docs/plans/AGENTIC_AI_INTEGRATION_PLAN.md`](../plans/AGENTIC_AI_INTEGRATION_PLAN.md)
- Operator walkthrough (transport agent + collection agent): [`docs/operator/AGENTIC_AI_WALKTHROUGH.md`](../operator/AGENTIC_AI_WALKTHROUGH.md)
