// Standalone Go module for the agenticudpreceiver. Kept as its own
// module so the heavy OTel Collector SDK dependency tree never
// reaches the EntropyOps server binary; only `ocb` (OTel Collector
// Builder) sees these deps.
//
// Phase 9.3 — Observability Hardening.

module github.com/entropyops/entropyops/otelcol/receiver/agenticudpreceiver

go 1.22

// Deliberate: we do NOT vendor go.opentelemetry.io/collector/* here.
// The upstream contrib repo will resolve it at its own version. This
// module ships the wire decoder and component skeleton; the OCB build
// wires it to the collector SDK at the operator's chosen version.
