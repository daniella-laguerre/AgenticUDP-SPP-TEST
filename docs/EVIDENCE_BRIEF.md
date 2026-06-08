# EntropyOps Evidence Brief

Last updated: 2026-05-06 — figures reconciled to archived `bench-*-baseline.json` (captured 2026-04-29Z). Bench toolchain reference: v2.15.0 (SHA below).

This document contains the benchmark results, their methodology, what they prove, what they do not prove, and a list of open questions. It is intentionally separate from the architecture and README so claims can be updated independently as new data arrives.

---

## What we are measuring

The benchmark compares three ingest transports — OTLP HTTP, OTLP gRPC, and AgenticUDP — on client-side wall-clock latency and throughput.

**Tool:** `eo-ingest-bench` (`entropyops-helper/cmd/eo-ingest-bench`)  
**Method:** one persistent connection per path (HTTP keepalive, gRPC `ClientConn`, AgenticUDP session), identical N-span payload per iteration, fresh trace/span IDs per call to prevent dedup short-circuiting.  
**Output:** p50 / p90 / p99 / avg / min / max per path + `spans/sec_wall` (total spans ÷ wall-clock seconds for the full send+ACK-drain window).

---

## Bench modes

The server has two modes, selected before startup:

| Mode | OTLP HTTP / gRPC | AgenticUDP | When to use |
|------|-----------------|------------|-------------|
| `standard-baseline` (default) | Per-row INSERT, default SQLite PRAGMAs (`synchronous=FULL`, `wal_autocheckpoint=1000`), no out-of-band WAL checkpointer | Bulk INSERT, receiver-level coalescer | Realistic comparison vs an off-the-shelf OTLP backend |
| `tuned` (`ENTROPYOPS_STANDARD_BASELINE_MODE=false`) | Bulk INSERT, `synchronous=NORMAL`, out-of-band WAL checkpointer, large mmap | Same as above | Platform self-test only — not honest for cross-path comparisons |

All headline numbers in this document are from `standard-baseline` mode. The older `bench-results/` files (`bench-200.json`, `bench-1000.json`, `bench-5000.json`, `*-postwal`, `*-postinstr`, `*-postbulk`) were captured in `tuned` mode before the flag existed. They are archived for reference, not cited as findings.

---

## Current measured numbers

**Environment:** Windows amd64, localhost loopback, v2.15+ server (`standard-baseline` default), 2026-04-29.  
**Bench binary:** v2.15.0, SHA-256 `343c865658641ececdc18d2fb995763e99af200f51f509010881929da0436cdf`.  
**Raw JSON:** [`docs/operator/bench-results/`](operator/bench-results/) — `generated_at` in each file matches the capture time above.

| Batch | Path | n | p50 ms | p90 ms | p99 ms | spans/sec | retx | drops |
|-------|------|---|-------:|-------:|-------:|----------:|-----:|------:|
| 200 spans | OTLP HTTP   | 50 | 169.43 | — | — | 1,199 | — | — |
| 200 spans | OTLP gRPC   | 50 | 228.03 | — | — | 860 | — | — |
| 200 spans | AgenticUDP  | 50 | **3.96** | — | — | **46,548** | 0 | 0 |
| 1000 spans | OTLP HTTP  | 30 | 511.03 | — | — | 1,976 | — | — |
| 1000 spans | OTLP gRPC  | 30 | 714.15 | — | — | 1,407 | — | — |
| 1000 spans | AgenticUDP | 30 | **38.56** | — | — | **25,122** | 0 | 0 |
| 5000 spans | OTLP HTTP  | 20 | 1,669.44 | 2,443 | 3,670 | 2,642 | — | — |
| 5000 spans | OTLP gRPC  | 20 | 2,259.68 | 2,635 | 2,996 | 2,174 | — | — |
| 5000 spans | AgenticUDP | 20 | **185.98** | — | — | — | 208 | 0 |

**Notes on the 200 and 1000-span rows:** p90 / p99 values from those runs are available in the JSON files but are omitted here because the table format is already wide. At n=50 and n=30, p99 is the single worst or second-worst iteration — the numbers are in the file if you want them, but they carry wide confidence intervals.

**Notes on the 5000-span row:** OTLP HTTP/gRPC p90, p99, and `spans_per_second` are copied from the JSON for context; at n=20, tail percentiles are still thinly sampled (see sample-size ladder in Caveats §2). p90, p99, and spans/sec for **AgenticUDP** at n=20 are not cited as findings. Explanation in Caveats §2 below.

`[ placeholder: standard-baseline n=10,000 result when the Windows VM run completes ]`

---

## What these numbers establish

**At 200-span batches:**  
AgenticUDP p50 is 42.8× lower than OTLP HTTP (3.96 ms vs 169 ms) and 57.6× lower than OTLP gRPC (3.96 ms vs 228 ms). Throughput is 38.8× higher than OTLP HTTP (46,548 vs 1,199 spans/sec). Retransmits and drops: zero.

**At 1000-span batches:**  
AgenticUDP p50 is 13.2× lower than OTLP HTTP (38.56 ms vs 511 ms) and 18.5× lower than OTLP gRPC. The spread tightened vs 200-span because larger payloads require more UDP chunks and more per-chunk ACK round-trips. Retransmits and drops: zero.

**At 5000-span batches (median only):**  
AgenticUDP p50 is 9.0× lower than OTLP HTTP (186 ms vs 1,669 ms). The architectural claim — AgenticUDP p50 is roughly an order of magnitude lower than OTLP HTTP at every batch size tested — holds at 5000 spans. The 208 retransmits (at 20 iterations) show the protocol recovered all data. Drops: zero.

**What changed between the tuned-mode runs and these:**  
The WAL checkpointer fix (moving inline `wal_autocheckpoint` to an out-of-band goroutine) improved every path. OTLP HTTP 5000-span p50 went from ~3,100 ms to 1,669 ms. AgenticUDP 5000-span retransmits dropped from 440 to 208 (tuned mode) to 0 (standard-baseline, before the v2.15.0 `acked` overcounting fix). The v2.15.0 bench binary fixes two additional accounting bugs — see [v2.15.0 binary corrections](#v2150-binary-corrections-that-affect-how-to-read-old-results) below.

---

## Caveats

### 1. Handler enrichment asymmetry (cross-path comparison is not pure transport)

The OTLP HTTP traces handler does work that the gRPC and AgenticUDP handlers skip:

| Handler | Extra work vs AgenticUDP |
|---------|--------------------------|
| OTLP HTTP | head/tail sampler, k8s-attribute joiner, 3 background fan-outs |
| OTLP gRPC | sampler and k8s join skipped; 2 of 3 fan-outs still present |
| AgenticUDP | none of the above |

The measured latency difference is partly real transport advantage and partly enrichment-work saved. Until all three handlers run the same pipeline, the right interpretation is: AgenticUDP numbers are an upper bound on the transport-only advantage, and HTTP numbers are a lower bound. Plan to fix this: [`docs/operator/HANDLER_PARITY_PLAN.md`](operator/HANDLER_PARITY_PLAN.md).

### 2. 5000-span tail and average are not published (n=20 is too small)

At n=20:
- p99 = the single worst iteration.
- p90 = the second worst iteration.
- Average and spans/sec inherit the same outlier sensitivity.

Reporting these as findings would be reporting noise as measurement. The raw values are in the JSON for completeness.

The n=10,000 follow-up is in progress on a Windows VM. Sample-size ladder:

| n | What it supports |
|---|-----------------|
| ≥ 10,000 | Publishable p99 with stable neighbours (9,899th and 9,901st); first honest p99.9 estimator |
| ≥ 1,000 | Defensible p99 |
| ≈ 100 | Bare minimum to say anything about the tail |
| ≤ 20 | Median only |

The follow-up will resolve to one of three outcomes: (a) tail is real and characterized by storage-side stall, (b) tail is environmental noise from the Windows test environment, or (c) no bimodal tail at the higher sample count. All three are publishable; none changes the median claim.

### 3. Localhost loopback only

All benchmark runs are single-host, no network between client and server. Real WAN conditions (packet loss, reordering, NAT, firewall UDP blocking) affect UDP and TCP differently:
- TCP head-of-line blocking gets worse on lossy or high-latency links.
- UDP retransmit storms can form if the receiver's kernel buffer fills and the sender's backoff doesn't kick in fast enough.

No WAN data exists yet.

### 4. v2.15.0 binary corrections that affect how to read old results

Two bugs in the bench client before v2.15.0 affected UDP numbers:

**Bug 1 — `acked` counter overcounting.** When the server sent a duplicate ACK, the client incremented the `acked` counter even if the inflight entry had already been removed. This caused the drain condition to fire early, making `WallClockSeconds` shorter than actual and inflating `spans/sec_wall`. The bench-5000-baseline.json run shows `acked=7,556` vs `sent=7,348` — a 208-count overcount that exactly equals the 208 retransmits in that run.

**Bug 2 — inflight map key wraparound.** Sessions exceeding 65,535 sends used a composite key `(streamID<<16)|seq` that wraps silently. This didn't affect n=20 runs (well below 65,535 sends), but will affect any n=10,000 run at 5000-span batches.

Both are fixed in the v2.15.0 binary (SHA above). Old n=20 results are still valid for p50 comparisons but the reported `acked` count is inflated. The n=10,000 run must use the v2.15.0 binary.

---

## Pilots and external data

**None yet.** All numbers are from internal test environments (one Windows VM, one macOS dev machine). No customer or external deployments.

`[ placeholder: first external pilot — date, environment description, results ]`

---

## What is not proven

| Claim | Status |
|-------|--------|
| AgenticUDP p99 at 5000-span batches | Not published. n=10,000 follow-up in progress. |
| Performance over WAN | Not measured. |
| Performance with >10 concurrent agents | Not measured. |
| Performance when SQLite entity count exceeds ~100 | Not measured. |
| AgenticUDP advantage is transport (not just enrichment skipping) | Not proven until handler parity ships. |
| Regime detection fires before a real incident | Not validated against real failure data. `[ placeholder: pilot failure event comparison ]` |
| Regime detection accuracy (precision / recall) | Not measured against ground truth. `[ placeholder: labeled failure dataset ]` |
| LLM root cause accuracy | Not evaluated. `[ placeholder: evaluation run against known failure modes ]` |
| `tierd` memory savings on real CXL hardware | Not measured. All tierd results are on simulated workloads. |
| V3 protocol end-to-end | Client-only. Server-side fragment reassembly not implemented. |

---

## Open questions driving the next work

1. **n=10,000 at 5000 spans** — does the tail characterize as stable, dissolve, or shift?  
   → Run is staged: `docs/operator/INGEST_BENCH_QUICKSTART.md §4b`.

2. **Handler parity** — once gRPC and AgenticUDP handlers run the same enrichment, how much of the latency spread is transport vs enrichment?  
   → Plan: `docs/operator/HANDLER_PARITY_PLAN.md`.

3. **Third-party backend comparison** — what does AgenticUDP look like vs Jaeger, Tempo, or an OTel Collector receiving the same spans?  
   → Plan (not yet run): `docs/operator/THIRD_PARTY_BACKEND_BENCH_PLAN.md`.

4. **Regime detection validation** — does the physics model fire before a real incident on a real host?  
   → Needs an external pilot with a labeled failure event.

---

## Raw data location

All bench JSON files: [`docs/operator/bench-results/`](operator/bench-results/)  
Bench methodology and running instructions: [`docs/operator/INGEST_BENCH_QUICKSTART.md`](operator/INGEST_BENCH_QUICKSTART.md)  
Transport deep-dive and triage history: [`docs/writeups/AGENTICUDP_BENCH_WRITEUP.md`](writeups/AGENTICUDP_BENCH_WRITEUP.md)
