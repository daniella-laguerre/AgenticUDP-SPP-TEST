# AgenticUDP, SPP, and ingest benchmark bundle

This folder is a **snapshot copy** of AgenticUDP transport code, System Physics Protocol (SPP) protobufs and generated Go, OTLP shim / bench tooling, OpenTelemetry AgenticUDP receiver, and benchmark documentation **from the EntropyOps monorepo**.

- **Source of truth for development** remains in `../EntropyOps` (or your clone). Paths mirror that repo so you can diff or `git init` here and push a **small public repo** without publishing all of EntropyOps.
- **SPP** = `proto/spp/v1/*.proto` and `entropyops-v2/pkg/sppv1/` (generated message types).
- **AgenticUDP** = `entropyops-helper/internal/transport/` and `entropyops-v2/internal/ingest/receiver/agenticudp*.go` (transport + core receiver).
- **Benchmarks / guides** = `docs/`, `release/`, `deploy/e2e/agenticudp-nat-matrix.sh`, bench commands under `entropyops-helper/cmd/`.

This bundle is **not** a guaranteed standalone build; module boundaries and imports still assume the full EntropyOps tree. Use it for **review, sharing, or seeding a dedicated public repository**.
