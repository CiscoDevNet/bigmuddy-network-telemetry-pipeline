# bigmuddy network telemetry pipeline

A streaming-telemetry collector for network devices. It subscribes to
devices over **gNMI** (any vendor) or accepts **Cisco MDT dial-out**, turns
everything into one vendor-neutral metric model, and delivers it to
**Prometheus**, **OpenTelemetry (OTLP)**, **InfluxDB**, **Kafka** or files.

> Originally released by Cisco in 2016 as an IOS XR-specific collector.
> Rebuilt in 2026 around gNMI, OpenTelemetry and modern Go — see
> [docs/MODERNIZATION.md](docs/MODERNIZATION.md) for what changed and why,
> and for a migration guide from the old `pipeline.conf`.

```
 gNMI dial-in ─┐                                   ┌─► Prometheus (scrape)
 MDT gRPC  ────┤                                   ├─► OTLP → OTel Collector / Prometheus 3 / Mimir / Datadog…
 MDT TCP   ────┼─► normalise ─► filter/tag/rename ─┼─► InfluxDB 1.x / 2.x / 3.x
 MDT UDP   ────┘                                   ├─► Kafka / Redpanda (JSON)
                                                   └─► file / stdout (JSON lines)
```

## Features

* **gNMI Subscribe** — `sample`, `on_change`, `target_defined`; TLS/mTLS;
  username/password; `proto`, `json_ietf` or `json` encoding; jittered
  exponential redial; per-target `up` and sync metrics.
* **Cisco MDT dial-out** — gRPC, TCP and UDP; self-describing KV-GPB and
  JSON; TLS/mTLS on gRPC and TCP.
* **Schema-free decoding** — no compiled-in YANG models; new models and
  software releases just work.
* **Isolation and backpressure** — bounded queues per output; a slow or
  failing destination never stalls the others; inputs apply backpressure to
  devices instead of dropping silently.
* **Batching and retries** with permanent-error detection; graceful drain on
  shutdown.
* **Processors** — regex filtering on names/tags/fields, static and
  per-device inventory tags, metric and field renames.
* **Operable** — strict YAML config with `${ENV}` secrets and `*_file`
  options, `-check` mode, `/healthz` `/readyz` `/metrics`, structured JSON
  logs, distroless non-root image.

## Quick start

```bash
make build                                   # → bin/pipeline
cat > pipeline.yaml <<'EOF'
admin: {listen: ":9180"}
inputs:
  - name: lab
    type: gnmi
    targets: ["192.0.2.1:57400"]
    username: admin
    password: ${GNMI_PASSWORD}
    tls: {enabled: true, insecure_skip_verify: true}   # lab only
    subscriptions:
      - {name: if, path: /interfaces/interface/state/counters, sample_interval: 10s}
      - {name: oper, path: /interfaces/interface/state/oper-status, mode: on_change}
outputs:
  - {name: prom, type: prometheus, listen: ":9273", string_fields: info}
  - {name: debug, type: file, path: "-"}
EOF
GNMI_PASSWORD=secret ./bin/pipeline -config pipeline.yaml -check
GNMI_PASSWORD=secret ./bin/pipeline -config pipeline.yaml
curl -s localhost:9273/metrics | grep interfaces_interface_state_counters_in_octets
```

Everything else:

* [examples/pipeline.yaml](examples/pipeline.yaml) — annotated reference of every option
* [deploy/compose](deploy/compose) — pipeline + Prometheus + Grafana lab (`docker compose up --build`)
* [deploy/kubernetes](deploy/kubernetes) — Deployment with probes, secrets, hardened security context

### Device configuration

IOS XR, gNMI (dial-in):

```
grpc
 port 57400
 tls
```

IOS XR, MDT dial-out to the pipeline over gRPC:

```
telemetry model-driven
 destination-group PIPELINE
  address-family ipv4 192.0.2.10 port 57500
   encoding self-describing-gpb
   protocol grpc no-tls
 sensor-group IF
  sensor-path Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters
 subscription S1
  sensor-group-id IF sample-interval 10000
  destination-id PIPELINE
```

Junos, EOS, SR OS/SR Linux, NX-OS and SONiC: enable their gNMI server and
use the `gnmi` input.

## How data is shaped

gNMI notifications are grouped per list instance:

```
/interfaces/interface[name=Ethernet1]/state/counters/in-octets = 1234
            ↓
name:   openconfig:/interfaces/interface
tags:   source=core1 subscription=if interface_name=Ethernet1
fields: state/counters/in-octets=1234
```

| Output | Representation |
|---|---|
| Prometheus / OTLP | `interfaces_interface_state_counters_in_octets{source="core1",interface_name="Ethernet1"} 1234` (OTLP: one Resource per device) |
| InfluxDB | `openconfig:/interfaces/interface,interface_name=Ethernet1,source=core1 state/counters/in-octets=1234u <ns>` |
| Kafka / file | `{"name":"openconfig:/interfaces/interface","tags":{…},"fields":{…},"timestamp":"…"}` |

## Development

```bash
make lint test race     # gofmt, vet, tests (with race detector)
make generate           # regenerate protos (buf, protoc-gen-go, protoc-gen-go-grpc)
make image              # container image
```

Layout:

```
cmd/pipeline/            entry point, end-to-end test
internal/config/         YAML loading, env expansion, secrets
internal/pipeline/       plugin registry, router, per-output workers, self-metrics
internal/telemetry/      the Metric model, naming, JSON flattening
internal/input/gnmi/     gNMI subscribe client + notification conversion
internal/input/ciscomdt/ MDT dial-out (gRPC/TCP/UDP), KV-GPB/JSON decoding
internal/processor/      filter, tags, rename
internal/output/…        prometheus, otlp, influxdb, kafka, file
proto/                   Cisco MDT .proto sources (generated code in internal/gen)
```

Adding a plugin: implement `pipeline.Input`, `Processor` or `Output`,
register it in `init()` with `pipeline.Register*`, and import the package in
`cmd/pipeline/main.go`.

## License

Apache 2.0 — see [LICENSE](LICENSE).
