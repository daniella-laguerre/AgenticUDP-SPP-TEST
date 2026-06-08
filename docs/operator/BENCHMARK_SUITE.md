# EntropyOps Benchmark Suite

This benchmark suite provides a reproducible way to measure Architecture Physics
performance at `1K`, `10K`, and `100K` entities and document the current
performance envelope.

## Scope

- Topology analysis runtime (architecture snapshot)
- Memory usage during analysis
- API response latency for architecture endpoints
- Repo architecture scan throughput (files/sec)

## Benchmark profiles

| Profile | Entities | Edges (target) | Purpose |
|---------|----------|----------------|---------|
| `small` | 1,000    | 5,000          | Developer and SMB baseline |
| `medium` | 10,000  | 60,000         | Team/department production scale |
| `large` | 100,000  | 900,000        | Stress envelope and sampling behavior |

## Repro workflow

1. Start EntropyOps locally (single binary mode).
2. Seed topology graph with synthetic profile.
3. Run repeated score/history/hotspot calls.
4. Export latency and RSS metrics into a result artifact.

Example harness entrypoint:

```bash
./deploy/e2e/benchmark-architecture.sh --profile medium --runs 20
```

## Result format

Each benchmark run should emit:

- profile metadata (`entities`, `edges`, run timestamp)
- p50/p95 latency for:
  - `/api/architecture/score`
  - `/api/architecture/history`
  - `/api/architecture/hotspots`
- process RSS peak
- notes on sampling mode (exact vs adaptive)

## Publication policy

- Publish latest benchmark snapshot in release notes for each minor release.
- Keep prior snapshots to show trend (regression/improvement).
- Include hardware class and Go version used for test.
