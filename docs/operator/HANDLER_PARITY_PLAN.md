# Handler Parity — Plan to Make the Three Ingest Paths Comparable

**Status:** open work item, owner unassigned.

**Why this matters:** the `eo-ingest-bench` results in `PROGRAM_SUMMARY.md`
"Current measured numbers" cannot be read as a clean cross-protocol
ranking until this lands. Today, OTLP HTTP, OTLP gRPC, and AgenticUDP
each run a **different** scope of work in the receiver, so any latency
spread between them is partly transport difference and partly handler
asymmetry. This document specifies exactly what each handler does today,
what they all should do, and the phased work to converge them.

---

## 1. The asymmetry, by file path

Verified by reading the three handlers head-to-head on 2026-04-29:

### OTLP HTTP traces handler

`entropyops-v2/internal/ingest/receiver/otlp.go`, around line 1384:

```go
traces, physicsStates := r.parseOTLPTracesPayload(payload, tenantID)
traces = r.applySampler(traces)                             // [SAMPLER]
if r.k8sJoiner != nil && len(traces) > 0 {                  // [K8S JOIN]
    for i := range traces {
        r.k8sJoiner.JoinTrace(tenantID, &traces[i])
    }
}
// ...
if len(traces) > 0 {
    if err := r.store.WriteTraces(ctx, traces); err != nil { ... }
    if r.pipelinePublisher != nil {
        r.pipelinePublisher.PublishTraces(tenantID, traces)
    }
    go r.persistEdgesFromTraces(traces, tenantID)           // [BG-EDGES]
    go r.indexBusinessIDsFromTraces(traces, tenantID)       // [BG-BUSIDS]
    go r.indexPeersFromTraces(traces, tenantID)             // [BG-PEERS]
}
```

### OTLP gRPC traces handler

`entropyops-v2/internal/ingest/receiver/otlp_grpc.go`, around line 122:

```go
traces, physicsStates := s.receiver.parseOTLPTracesPayload(payload, tenantID)
if len(traces) > 0 {
    if err := s.receiver.store.WriteTraces(ctx, traces); err != nil { ... }
    if s.receiver.pipelinePublisher != nil {
        s.receiver.pipelinePublisher.PublishTraces(tenantID, traces)
    }
    go s.receiver.persistEdgesFromTraces(traces, tenantID)  // [BG-EDGES]
    // NO sampler. NO k8s join. NO BG-BUSIDS. NO BG-PEERS.
}
```

### AgenticUDP traces handler

`entropyops-v2/internal/ingest/receiver/agenticudp.go`, `enqueueTraces`:

```go
func (r *AgenticUDPReceiver) enqueueTraces(tenantID string, traces []storage.Trace) {
    // (called from routeJSONEnvelope ~line 829 and routeProtoEnvelope ~line 877)
    // routes through the coalescer to r.store.WriteTraces eventually.
    // NO sampler. NO k8s join. NO BG-EDGES. NO BG-BUSIDS. NO BG-PEERS.
}
```

The AgenticUDP path also calls `r.pipeline.PublishTraces(tenantID, traces)`
from the route function (line 827 / 875), so PublishTraces parity is fine.

### Asymmetry table

| Step | HTTP | gRPC | AgenticUDP |
|------|:----:|:----:|:----------:|
| auth + parse | ✅ | ✅ | ✅ |
| `applySampler` (head/tail) | ✅ | ❌ | ❌ |
| `k8sJoiner.JoinTrace` | ✅ | ❌ | ❌ |
| `WriteTraces` | ✅ | ✅ | ✅ (via coalescer) |
| `PublishTraces` | ✅ | ✅ | ✅ |
| BG: `persistEdgesFromTraces` | ✅ | ✅ | ❌ |
| BG: `indexBusinessIDsFromTraces` | ✅ | ❌ | ❌ |
| BG: `indexPeersFromTraces` | ✅ | ❌ | ❌ |

The same audit needs to be done for **metrics** and **logs** handlers
once traces are converged. Spot-checks suggest the same pattern.

---

## 2. The target end state

A single shared package `entropyops-v2/internal/ingest/handler/` with one
function per signal type that every transport's handler calls:

```go
// Package handler centralises the per-signal enrichment + write +
// fan-out work so that no transport (HTTP / gRPC / AgenticUDP / Kafka /
// any future addition) can accidentally diverge from the others.
//
// Every transport-level handler is a thin shell:
//   1. authorise + parse payload bytes into []storage.Trace
//   2. call handler.IngestTraces(ctx, tenantID, traces)
//
// IngestTraces is the SINGLE place that runs sampling, k8s join,
// storage write, pipeline publish, and the three background fan-outs.
// New enrichment work only ever lands here, never in a transport file.

package handler

func (h *TraceIngestor) IngestTraces(ctx context.Context, tenantID string, traces []storage.Trace) error {
    if len(traces) == 0 {
        return nil
    }
    // 1. Synchronous enrichment
    traces = h.sampler.Apply(traces)
    h.k8sJoiner.JoinAll(tenantID, traces)
    // 2. Synchronous storage (or coalesced storage if h.coalescer != nil)
    if err := h.writer.WriteTraces(ctx, tenantID, traces); err != nil {
        return err
    }
    // 3. Synchronous pipeline publish (telemetry, NOT critical-path)
    h.pipeline.PublishTraces(tenantID, traces)
    // 4. Background fan-outs — all three share a bounded worker pool so
    //    a slow downstream cannot pin the handler goroutine
    h.bg.Submit(func() { h.persistEdges(tenantID, traces) })
    h.bg.Submit(func() { h.indexBusinessIDs(tenantID, traces) })
    h.bg.Submit(func() { h.indexPeers(tenantID, traces) })
    return nil
}
```

Same pattern for `IngestMetrics` and `IngestLogs`. The transports become
boring:

```go
// otlp.go
traces, _ := r.parseOTLPTracesPayload(payload, tenantID)
return r.tracesIngestor.IngestTraces(req.Context(), tenantID, traces)

// otlp_grpc.go
traces, _ := s.receiver.parseOTLPTracesPayload(payload, tenantID)
return s.receiver.tracesIngestor.IngestTraces(ctx, tenantID, traces)

// agenticudp.go
traces := r.parseFromEnvelope(payload, tenantID)
return r.tracesIngestor.IngestTraces(ctx, tenantID, traces)
```

---

## 3. Phased plan (5 milestones, each independently shippable)

### M1 — Extract the trace ingestor (no behaviour change)

**Code:** create `entropyops-v2/internal/ingest/handler/traces.go`
holding the new `TraceIngestor` struct with the API above. Move the
six steps (sampler, k8s join, write, publish, edges, busids, peers)
out of the HTTP handler into `IngestTraces`. The HTTP handler becomes
a one-liner that calls it. The gRPC and AgenticUDP handlers do **not**
change yet — they continue to call their own (smaller) sequences.

**Tests:** lift the existing OTLP HTTP traces test cases
(`internal/api/server_phase5_test.go`, `internal/ingest/receiver/`
test files) so they exercise `IngestTraces` directly. Add a new test
that proves: given the same `[]storage.Trace`, the fan-out goroutines
all fire and the synchronous return path matches the previous output
byte-for-byte.

**Bench impact:** none. HTTP behaves identically. gRPC and UDP still
skip work. Bench numbers should not move.

**Acceptance:** all existing tests green; new direct-call tests green;
`PROGRAM_SUMMARY.md`-cited HTTP latency at 200/1000/5000 spans within
5% of the previous run.

### M2 — Wire OTLP gRPC to the shared ingestor

**Code:** `otlp_grpc.go` `Export` calls
`s.receiver.tracesIngestor.IngestTraces(...)` instead of its own
inline `WriteTraces` + `persistEdgesFromTraces`. Delete the inline
copy. Same for `Metrics` and `Logs` Export methods once those signals
have ingestors.

**Tests:** add a test that issues the same payload over HTTP and gRPC
and asserts the storage row counts, the persisted edges, the indexed
business IDs, and the indexed peers are identical for both transports.
This is the "no asymmetry" regression test that protects the codebase
forever.

**Bench impact:** gRPC will get **slower** at all three batch sizes
because it now does sampler + k8s join + 2 extra background fan-outs.
The current `bench-1000.json` numbers (gRPC p50 = 648 ms vs HTTP p50 =
410 ms, ~1.6× slower than HTTP) will likely tighten because gRPC's
"win" was partly the missing work. **This is the desired outcome** —
it removes the asymmetry from the bench. The new gRPC p50 should land
within ~30% of HTTP at the same batch size.

**Acceptance:** parity test green; bench shows gRPC and HTTP within
30% of each other at p50 for 200 / 1000 / 5000 spans.

### M3 — Wire AgenticUDP to the shared ingestor

**Code:** `agenticudp.go` `enqueueTraces` calls
`r.tracesIngestor.IngestTraces(...)` instead of going straight to the
coalescer. The coalescer becomes a property of `TraceIngestor.writer`
(an `interface { WriteTraces(...) error }`) so AgenticUDP keeps its
batched-write behaviour transparently.

**Tests:** extend the M2 parity test to a third leg: HTTP, gRPC, and
AgenticUDP all produce identical storage / edges / business-IDs /
peers output for the same payload.

**Bench impact:** AgenticUDP will get **slower** at all three batch
sizes for the same reason gRPC did. The current ~37 ms p50 at 1000
spans will rise. The honest expectation is: AgenticUDP's transport
advantage (no per-byte ACK, cycle-batched datagrams) survives the
parity work; the *raw* multipliers will drop from 11× HTTP to maybe
2–4× HTTP, which is the real protocol win.

**Acceptance:** three-way parity test green; AgenticUDP p50 at 1000
spans is ≥ 2× HTTP (so the protocol win is preserved) and ≤ 6× HTTP
(so the bench is plausibly fair).

### M4 — Repeat for metrics and logs

Same M1/M2/M3 cycle, applied to `MetricIngestor` and `LogIngestor`. The
existing AgenticUDP enricher (`r.enricher.EnrichAndStoreMetrics`)
becomes the body of `MetricIngestor.IngestMetrics` so HTTP and gRPC
also get DQI/exposure/topology/anomaly enrichment.

**Bench impact:** HTTP and gRPC metric paths get slower (they now run
the enricher); AgenticUDP metric path stays the same. This is the
**inverse** of the trace direction — for metrics, AgenticUDP was
already doing the work HTTP and gRPC skipped.

### M5 — Delete the per-handler inline code

After M1–M4, delete:
- `applySampler` calls in `otlp.go` (now in `TraceIngestor`)
- the inline `JoinTrace` loop in `otlp.go` (now in `TraceIngestor`)
- the three `go ...` fan-out lines in `otlp.go` (now in `TraceIngestor`)
- the inline `WriteTraces` + `persistEdgesFromTraces` in `otlp_grpc.go`
- the direct `enqueueTraces` call sites in `agenticudp.go`

Add a `// PARITY GUARD` linter rule (or a one-off `grep` in `make
test`) that fails CI if any of the above patterns reappear in the
transport files. That prevents the asymmetry from drifting back in.

---

## 4. Risks and explicit choices

1. **gRPC and AgenticUDP will get slower.** That is the point. The
   bench numbers will tighten and the "AgenticUDP is 11× faster" claim
   will become "AgenticUDP is N× faster" with a smaller, defensible N.
   Anyone reading the bench output today is implicitly told this in
   the "WHAT THESE NUMBERS DO NOT TELL YOU" footer.
2. **The sampler, the k8s joiner, and the three background fan-outs
   are not all wanted on every transport.** Some are arguments to be
   debated, not assumed:
   - `BG-EDGES` (`persistEdgesFromTraces`): topology graph data. Wanted
     on every transport. Already on HTTP and gRPC.
   - `BG-BUSIDS` (`indexBusinessIDsFromTraces`): claim/policy/quote
     number indexing for business search. **Wanted on every transport**
     — currently only HTTP indexes them, which means business search
     silently misses gRPC/UDP traffic. This is a latent correctness bug
     M2/M3 fixes.
   - `BG-PEERS` (`indexPeersFromTraces`): uninstrumented peer detection.
     **Wanted on every transport** — currently HTTP-only; gRPC/UDP
     traffic does not contribute to the "uninstrumented peers" view.
     Same correctness bug.
   - `applySampler`: head/tail sampling. **Should this run on AgenticUDP?**
     Open question. AgenticUDP traffic is typically already curated
     (it's coming from EntropyOps agents, not arbitrary OTel SDKs), so
     applying a head/tail sampler may discard signal we explicitly
     wanted. Default proposed: **on for HTTP/gRPC, off for AgenticUDP**,
     made explicit via `TraceIngestor` config rather than per-handler
     code drift.
   - `k8sJoiner`: enrich span attributes from a k8s metadata cache.
     **Wanted on every transport** in k8s deployments; no-op when no
     joiner is configured (current behaviour).
3. **The bounded worker pool for background fan-outs is new** and
   needs its own size knob (`ENTROPYOPS_INGEST_BG_WORKERS`, default
   16) plus a backpressure decision: **drop or block?** Recommended
   default: drop with a counter (`ingest_bg_dropped_total`) so a slow
   downstream cannot starve the ingest path. Operators who want the
   guarantee can set the queue depth higher.

---

## 5. Sequencing and effort estimate

| Milestone | Files touched | LOC est. | Risk | Bench rerun |
|-----------|---------------|---------:|------|:-----------:|
| M1: extract `TraceIngestor` | `internal/ingest/handler/traces.go` (new), `internal/ingest/receiver/otlp.go` | ~250 | low (move + thin call) | optional |
| M2: gRPC adopts ingestor | `internal/ingest/receiver/otlp_grpc.go` | ~80 | medium (gRPC slows down) | required |
| M3: AgenticUDP adopts ingestor | `internal/ingest/receiver/agenticudp.go`, `agenticudp_coalescer.go` | ~120 | medium-high (UDP slows down; coalescer adapter) | required |
| M4: metrics + logs | same files, `MetricIngestor`, `LogIngestor` | ~400 | medium | required |
| M5: cleanup + lint guard | all transport files, `Makefile` (lint) | ~50 | low | optional |

Estimated calendar time: **2–3 days of focused work** for one engineer,
including bench reruns.

---

## 6. Acceptance criteria for "handler parity is done"

1. The three-way parity test (HTTP / gRPC / AgenticUDP issuing the same
   payload land identical rows in `traces`, `edges`, `business_id_index`,
   `peer_index`) is in CI and passing.
2. The `eo-ingest-bench` footer is updated: caveat (1) — the handler
   asymmetry — is **removed**, replaced with one sentence stating that
   the three handlers now run identical synchronous and background work.
3. `PROGRAM_SUMMARY.md` "Current measured numbers" is rerun on the
   parity build. The HTTP/gRPC/AgenticUDP spread shown there is now an
   honest cross-protocol comparison; whatever it says is the real
   answer to "which transport wins."
4. The `// PARITY GUARD` lint check exists in `make test` and fails CI
   if any transport file calls `WriteTraces` directly instead of going
   through an ingestor.
