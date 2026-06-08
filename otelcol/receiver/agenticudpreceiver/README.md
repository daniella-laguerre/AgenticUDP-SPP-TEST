# agenticudpreceiver

> OpenTelemetry Collector receiver for the AgenticUDP V2 protocol.
>
> Phase 9.3 of the EntropyOps Observability Hardening plan
> (`docs/plans/OBSERVABILITY_HARDENING_PHASES_3_10.md`).

This Go module is a **standalone OpenTelemetry Collector receiver**.
It can be vendored into any `otelcol-builder` configuration and
allows an OTel Collector to decode AgenticUDP V2 datagrams (the
EntropyOps lossless UDP transport — see `docs/AGENTICUDP_V2.md`)
into native `pdata` and forward them through any standard collector
pipeline (`exporters: [otlp/tempo, otlphttp/mimir, ...]`).

## Status: Phase 9.3 alpha

The receiver decodes the **JSON tier** of AgenticUDP V2 today
(`flagProtobuf=0` payloads). Full SPP-protobuf payload support and
DTLS-encrypted streams are **planned** and tracked under
`docs/plans/OBSERVABILITY_HARDENING_PHASES_3_10.md` as Phase 11.x.

This is intentional. The minimum viable upstream Collector receiver
proves the round-trip (`AgenticUDP V2 frame → pdata → OTLP exporter →
Tempo/Mimir`) end-to-end without taking on the full SPP protobuf
schema as a transitive dependency on every collector build. Operators
who need the protobuf tier today should keep using EntropyOps as the
AgenticUDP terminator and forward via OTLP from there.

## Quickstart

```yaml
# otelcol-config.yaml
receivers:
  agenticudp:
    # Default in v2.11+ is 0.0.0.0:4320. Earlier releases used :4318,
    # which collides in port number with the IANA-reserved OTLP HTTP
    # listener; pin to :4318 explicitly only for legacy fleets.
    endpoint: 0.0.0.0:4320
    default_tenant: default

exporters:
  otlp/tempo:
    endpoint: tempo:4317
    tls: { insecure: true }

service:
  pipelines:
    traces:
      receivers: [agenticudp]
      exporters: [otlp/tempo]
    metrics:
      receivers: [agenticudp]
      exporters: [otlp/tempo]
    logs:
      receivers: [agenticudp]
      exporters: [otlp/tempo]
```

Build with `ocb` (OTel Collector Builder):

```yaml
# builder.yaml
receivers:
  - gomod: github.com/entropyops/entropyops/otelcol/receiver/agenticudpreceiver v0.1.0
```

```bash
ocb --config builder.yaml
./otelcol --config otelcol-config.yaml
```

## Goals (acceptance criteria for Phase 9.3)

- An OTel-Collector with `receivers: [agenticudp]` and
  `exporters: [otlp/tempo]` can route AgenticUDP traces into Tempo
  without touching the EntropyOps server. (Met for JSON tier.)
- Round-trip integration test ships in
  `entropyops-v2/internal/ingest/receiver/agenticudp_test.go` and
  proves wire compatibility against the existing
  `AgenticUDPReceiver` decoder.

## Upstream contribution path

This module targets eventual contribution to
`open-telemetry/opentelemetry-collector-contrib` under
`receiver/agenticudpreceiver`. The module path and import surface are
deliberately compatible with the contrib repo layout so the move is
a copy-paste plus README rewrite.
