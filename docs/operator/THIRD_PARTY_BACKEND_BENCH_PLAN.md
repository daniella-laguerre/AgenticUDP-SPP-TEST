# Third-Party Backend Bench Plan (Option B)

## Why this document exists

The cross-protocol bench in this repo (`eo-ingest-bench`) currently
points all three paths — OTLP HTTP, OTLP gRPC, AgenticUDP — at the
same in-tree EntropyOps server. As of v2.15 the OTLP HTTP/gRPC paths
default to **standard-baseline mode** (per-row INSERT against an
untuned SQLite), which approximates an off-the-shelf OTLP backend
better than the previous "everything tuned" configuration did. That
flag is documented in [`PROGRAM_SUMMARY.md`](../../PROGRAM_SUMMARY.md)
and [`docs/operator/INGEST_BENCH_QUICKSTART.md`](INGEST_BENCH_QUICKSTART.md).

But "untuned SQLite" is still not what a customer is actually running.
Their existing OTel collector talks to **Jaeger / Tempo / Datadog
Agent / Splunk OTel Collector / Honeycomb / a homegrown Postgres
backend** — none of which are SQLite. To produce numbers that survive
"prove it against my actual stack" scrutiny, we need to point the
HTTP/gRPC portion of the bench at one of those real backends and
leave only the AgenticUDP portion against the EntropyOps server.

This document is the implementation plan for that bench. It is
intentionally **not implemented yet** — the work is non-trivial and
the current standard-baseline mode is the right next ship. This is the
follow-up that closes the "but is it really an apples-to-apples
comparison?" objection for good.

## The asymmetry we are explicitly accepting

We are NOT comparing the same logical workload through two different
transports against the same backend. We are comparing **two
fundamentally different ingest stacks**:

| Bench leg | Transport | Backend | What this approximates |
| --- | --- | --- | --- |
| HTTP / gRPC (industry baseline) | OTLP/HTTP, OTLP/gRPC | Third-party (Jaeger / Tempo / OTel Collector debug exporter) | The customer's existing observability stack |
| AgenticUDP (EntropyOps offering) | AgenticUDP / DTLS | EntropyOps SQLite (with all tunings ON) | The EntropyOps appliance |

That asymmetry is the point. AgenticUDP is a transport-PLUS-receiver
product; comparing it against an OTel collector + storage backend is
the comparison a customer would actually make when deciding which to
adopt. The previous "everything against SQLite, both per-row and
bulk" framing pretended the comparison was about transports in
isolation, which it never is in production.

## Reference targets, in priority order

Pick the FIRST one that is realistic for the deployment story you
are pitching to.

### 1. OpenTelemetry Collector + debug exporter — RECOMMENDED for first pass

**Why first**: it's the canonical "OTLP standard" reference. Any
result against it is unambiguous. It receives OTLP/HTTP and OTLP/gRPC
on the standard ports, parses the payload, and writes to stdout (or
`/dev/null` via the file exporter). No actual storage layer in the
loop, so the number is a clean "what does the OTel pipeline cost?"
floor.

What it tests: protocol overhead + collector parse + collector
batching. NOT storage. This is the **best case** for the OTLP/HTTP
side; if AgenticUDP doesn't beat this, it doesn't have a story.

```yaml
# otelcol-config.yaml (paste this into a fresh file)
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
      grpc:
        endpoint: 0.0.0.0:4317

exporters:
  debug:
    verbosity: basic        # set to "detailed" to see every span; basic = just counters
  file:
    path: /dev/null         # honest "we accepted it and threw it away" sink

service:
  pipelines:
    traces:
      receivers:  [otlp]
      exporters:  [debug, file]
    metrics:
      receivers:  [otlp]
      exporters:  [debug, file]
    logs:
      receivers:  [otlp]
      exporters:  [debug, file]
```

Run it (Docker):

```bash
docker run --rm \
  -p 4317:4317 -p 4318:4318 \
  -v "$(pwd)/otelcol-config.yaml:/etc/otelcol-contrib/config.yaml" \
  otel/opentelemetry-collector-contrib:latest
```

### 2. Jaeger all-in-one — RECOMMENDED for "what does a real trace store cost?"

**Why second**: Jaeger is the most widely deployed open-source
trace store. It accepts OTLP/HTTP on `:4318` and OTLP/gRPC on `:4317`
in the all-in-one image, parses, indexes, and writes to its own
storage backend (memory by default; cassandra / elasticsearch in
prod).

What it tests: protocol overhead + Jaeger's parse + Jaeger's
in-memory indexing. The in-memory backend is the **best case** for
Jaeger; switch to Cassandra or Elasticsearch when you need realistic
production numbers. The version of Jaeger matters: pre-1.50 the OTLP
receiver was a separate component, post-1.50 it is built in.

```bash
docker run --rm -d --name jaeger \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 16686:16686 \
  jaegertracing/all-in-one:1.62
```

### 3. Tempo (Grafana) — for the "prometheus-shop already has Tempo" story

**Why third**: Tempo is the OTLP-native trace store from Grafana. If
the prospect is already running Grafana / Loki / Mimir, Tempo is the
natural comparison point. Single-binary mode listens for OTLP on
`:4318` (HTTP) and `:4317` (gRPC). Storage backend is local disk by
default; S3/GCS/Azure in prod.

```bash
# tempo.yaml in cwd
docker run --rm -d --name tempo \
  -p 4317:4317 -p 4318:4318 -p 3200:3200 \
  -v "$(pwd)/tempo.yaml:/etc/tempo.yaml" \
  grafana/tempo:latest \
  -config.file=/etc/tempo.yaml
```

### 4. Customer's actual stack — the gold standard

If a specific customer is the audience, ask for their OTLP endpoint
and bench against THAT. No simulation beats the real thing. The bench
already takes `-server` and `-grpc` flags so this is a CLI swap, not
a code change.

## The bench tool already supports this

`eo-ingest-bench` accepts independent target flags for each path:

```bash
eo-ingest-bench \
  -server  http://otelcol:4318       \     # OTLP HTTP -> OTel Collector / Jaeger / Tempo
  -grpc    otelcol:4317              \     # OTLP gRPC -> same
  -udp     127.0.0.1:4320            \     # AgenticUDP -> EntropyOps server
  -tenant  Trading-dev               \
  -spans   1000 -iters 50 -warmup 5  \
  -json    bench-vs-jaeger.json
```

Two important caveats the tool does NOT currently enforce — operators
must remember them:

1. **AgenticUDP traffic still requires an EntropyOps server**, because
   AgenticUDP is not a public protocol with off-the-shelf receivers.
   So you will have BOTH a third-party OTLP collector AND the
   EntropyOps server running for the duration of a Plan-B bench.
2. **The `bench_mode` probe (`/api/ingest/transports`) only reports
   the EntropyOps server's mode**, not the third-party backend's.
   For a Plan-B run the bench should label the OTLP rows as
   "vs. <backend>" instead of as standard-baseline. See the work
   items below.

## Work items to land Plan B as a real product feature

These are roughly ordered by leverage. Items 1–3 unlock the bench
without code changes; items 4–7 make it first-class.

### Required to run a Plan-B bench today

1. **Spin up Jaeger or OTel Collector debug exporter on the bench
   host.** Use one of the recipes in §"Reference targets" above. Pick
   ports that don't conflict with the EntropyOps server (the recipes
   above already use the OTLP standard ports `:4317`/`:4318`, so make
   sure the EntropyOps server is on different ports for HTTP — its
   default is `:8000` and `:4317` respectively, which means you'll
   need to run the EntropyOps server with `ENTROPYOPS_GRPC_PORT=14317`
   to free `:4317` for Jaeger/OTel-Collector).
2. **Confirm the third-party backend accepts your payload.** Send a
   single test trace via `curl`/`grpcurl` and check it shows up in
   Jaeger UI / OTel Collector logs. This catches "wrong content-type",
   "missing /v1/traces path", and "auth required" gotchas before the
   bench masks them as latency outliers.
3. **Run the bench with split targets.** Use the command in the
   previous section. Capture the JSON output AND the OTel
   Collector / Jaeger logs for the same time window — those logs
   are evidence that the spans actually landed.

### To make Plan B first-class (real code work)

4. **Add a `--otlp-target-name` flag to `eo-ingest-bench`** so the
   bench output labels the HTTP/gRPC rows as e.g. `vs Jaeger 1.62
   (in-memory)` instead of just `OTLP HTTP`. Without this the JSON is
   ambiguous a week later. Suggest making it a free-text string the
   operator passes; the tool persists it into the JSON under
   `otlp_target_name`.
5. **Make `bench_mode` reporting target-aware.** When `--otlp-target`
   is anything other than the EntropyOps server, the bench should
   skip the `/api/ingest/transports` probe and label the run as
   `bench_mode=external-<otlp-target-name>`. This avoids the
   misleading "standard-baseline" label in JSON for runs that
   are actually against Jaeger.
6. **Add a smoke-check phase before iters.** In Plan-B mode, send 1
   span to each target during warmup and FAIL FAST if the third-party
   backend returns non-2xx, instead of letting all 50 iterations
   accumulate the same error. The current bench tool already collects
   `lastErr` but doesn't abort — change that for Plan-B mode only.
7. **Document a fixed reference rig.** A docker-compose file under
   `docs/operator/bench-results/plan-b/` that brings up Jaeger, the
   EntropyOps server, and the bench client in one command, with
   pinned versions and pinned ports. So results submitted to
   `bench-results/` are reproducible by anyone reviewing them.

### Result-quality work items

8. **Run the same bench size with all three target classes** (debug
   exporter, Jaeger in-memory, Jaeger+Cassandra) and publish the
   triple. The "AgenticUDP vs OTLP" gap is going to look very
   different against the debug exporter (~1ms ceiling) than against
   Jaeger+Cassandra (storage stalls dominate). Both numbers are
   honest; the distinction matters when a prospect asks.
9. **Run from a separate host across a real network**, not on
   loopback. UDP's lack of head-of-line blocking is hardest to see
   on loopback because there's no congestion.

## What this plan does NOT solve

- **Handler asymmetry inside EntropyOps.** The HTTP / gRPC / AgenticUDP
  paths in the EntropyOps codebase still don't run identical
  enrichment chains (HTTP runs sampler + k8s-joiner; the others don't).
  That's a separate cleanup tracked in
  [`HANDLER_PARITY_PLAN.md`](HANDLER_PARITY_PLAN.md).
- **Whether the EntropyOps appliance is faster than Jaeger as a
  storage product.** Plan B compares ingest paths, not storage / query
  capability. A separate query-side bench would be needed for a fair
  storage comparison, and would have its own design.

## Acceptance criteria for "we landed Plan B"

We can claim Plan B is complete when **all** of the following are
true for the headline numbers in `PROGRAM_SUMMARY.md`:

1. The HTTP / gRPC rows were measured against a third-party OTLP
   backend (Jaeger or OTel Collector), not against the in-tree
   server in either bench mode.
2. The bench JSON in `docs/operator/bench-results/` records the
   `otlp_target_name` for each row.
3. The reference rig (docker-compose + pinned versions) exists and
   reproduces the headline numbers within ±20%.
4. The numbers were captured both on loopback AND across a real
   network, and both are published.

Until then, the headline numbers are honest about being
**standard-baseline mode against the in-tree SQLite**, with this
document linked as the next-step.
