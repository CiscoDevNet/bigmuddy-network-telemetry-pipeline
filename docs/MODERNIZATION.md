# Modernization: 2016 → 2026

This document records why the pipeline was rebuilt, what changed in network
telemetry over the intervening nine years, the architecture that replaced
the original, and how to migrate an existing `pipeline.conf`.

## 1. What changed in network telemetry

| Area | 2016 (original pipeline) | 2026 (state of the art) | What we did |
|---|---|---|---|
| **Collection protocol** | Cisco-proprietary MDT: gRPC dial-out, gRPC dial-in (`gRPCConfigOper.CreateSubs`), TCP/UDP with a custom "ST" header | **gNMI** (OpenConfig) `Subscribe` is the multi-vendor standard: Cisco XR/XE/NX-OS, Juniper, Arista, Nokia, SONiC… | New first-class `gnmi` input. Kept MDT dial-out (gRPC/TCP/UDP) for existing IOS XR fleets; dropped the proprietary dial-in, which gNMI replaces. |
| **Data models** | Cisco native YANG only | OpenConfig models across vendors, native models where needed | Vendor-neutral internal model; gNMI paths and Cisco encoding paths both map to it. |
| **Encoding** | Compact GPB needing ~3,600 compiled-in per-model `.proto` packages (and a rebuild for every new model or software release) | Self-describing encodings: gNMI typed values, `JSON_IETF`, KV-GPB | Dropped compact GPB and the 108 MB generated tree. Decoding is now schema-free: any model works without a rebuild. |
| **Subscription semantics** | Periodic (cadence) only | `SAMPLE`, `ON_CHANGE`, `TARGET_DEFINED`, heartbeats, `suppress_redundant`, `sync_response` | All supported per subscription path. |
| **Security** | TLS optional; passwords RSA-encrypted into the config file via an interactive prompt | TLS 1.2+ by default, mTLS, secrets from a vault/K8s/Docker secrets | `tls` block on every network plugin (client and server, mTLS). Secrets via `${ENV}` or `*_file`. The RSA/prompt mechanism and test keys checked into the repo were removed. |
| **Export protocol** | Bespoke per-TSDB clients; Prometheus via Pushgateway | **OpenTelemetry (OTLP)** is the industry wire format; Prometheus 3 ingests OTLP natively and discourages Pushgateway for this use | New `otlp` output (gRPC and HTTP). The Prometheus output is now a pull exporter with series expiry (no Pushgateway). |
| **InfluxDB** | InfluxDB 1.x Go client (archived) | InfluxDB 2.x/3.x with token auth; line protocol is also spoken by VictoriaMetrics, QuestDB, Greptime… | Dependency-free line-protocol writer for `/api/v2/write` (token) and legacy `/write`. |
| **Kafka** | `Shopify/sarama` + `bsm/sarama-cluster` (deprecated) | `franz-go`; idempotent producers; zstd; SASL/SCRAM; managed Kafka/Redpanda | New `kafka` output on franz-go with all of the above. |
| **Go ecosystem** | Go 1.7, glide, vendored deps, `golang/protobuf` v1, `x/net/context`, `Sirupsen/logrus` | Go modules, `google.golang.org/protobuf`, `context` everywhere, `log/slog`, generics | Rewritten on Go 1.26 with modules; protos regenerated with `buf` + `protoc-gen-go`/`-go-grpc`. |
| **Operations** | INI config silently ignoring typos; checked-in binary; Debian image as root | Strict declarative config, health probes, self-metrics, distroless non-root images, CI with vuln scanning | Strict YAML (unknown keys are errors), `-check`, `/healthz` `/readyz` `/metrics`, graceful drain on SIGTERM, distroless image, GitHub Actions with `govulncheck`. |

## 2. Architecture

```
                  ┌──────────────── inputs ────────────────┐
 devices ─gNMI───►│ gnmi (dial-in, per-target redial)      │
 devices ─MDT────►│ cisco_mdt (grpc | tcp | udp dial-out)  │
                  └───────────────┬────────────────────────┘
                                  │ telemetry.Metric (vendor-neutral)
                                  ▼
                     router queue (bounded; blocks → backpressure to devices)
                                  │
                     processors in order: filter → tags → rename
                                  │
            ┌──────────────┬──────┴───────┬──────────────┬──────────┐
            ▼              ▼              ▼              ▼          ▼
     [queue+batch+retry per output — overflow drops & counts for that output only]
       prometheus        otlp          influxdb        kafka       file
```

### The data model

Everything is normalised into one type (`internal/telemetry.Metric`):

| Field | gNMI example | Cisco MDT example |
|---|---|---|
| `Name` | `openconfig:/interfaces/interface` | `Cisco-IOS-XR-infra-statsd-oper:infra-statistics/interfaces/interface/latest/generic-counters` |
| `Tags` | `source=core1, subscription=if-counters, interface_name=Ethernet1` | `source=dr02, subscription=…, interface-name=HundredGigE0/0/0/0` |
| `Fields` | `state/counters/in-octets=1234` | `bytes-received=1234` |
| `Timestamp` | notification timestamp (ns) | row timestamp (ms) |

gNMI updates are grouped by **instance path** — the path up to the last list
element with keys — so all leaves of one interface become one metric (one
InfluxDB point) however the device splits its notifications. List keys become
tags named `<list>_<key>` (`interface_name`, `subinterface_index`), unique
across nested lists. Nested containers and JSON values flatten into
`/`-separated field names; RFC 7951 quoted 64-bit integers are restored to
numbers.

### Delivery guarantees

* **Inputs → router:** blocking. A full router queue stops reading from the
  network, so gRPC/TCP flow control pushes back to the device rather than
  silently losing data inside the collector.
* **Router → outputs:** per-output bounded queue, non-blocking. One slow or
  down destination cannot stall the others; its overflow is dropped and
  counted in `pipeline_output_dropped_total{reason="queue_full"}`.
* **Writes:** batched by size/time, retried with exponential backoff on
  transient errors (5xx, 429, gRPC unavailable), not retried on permanent
  ones (400, auth, schema) — `pipeline_output_dropped_total{reason="write_failed"}`.
* **Shutdown:** SIGTERM stops inputs, drains every queue, flushes partial
  batches (bounded by a 30 s drain timeout), then closes outputs.

### Self-monitoring (`admin.listen`, default `:9180`)

| Metric | Meaning |
|---|---|
| `pipeline_input_metrics_total{input}` | metrics received |
| `pipeline_router_queue_batches` | router backlog (sustained >0 ⇒ processors/outputs are the bottleneck) |
| `pipeline_output_enqueued_total`, `_written_total`, `_dropped_total{reason}` | per-output flow |
| `pipeline_output_write_duration_seconds`, `_batch_size` | write latency / batch histograms |
| `gnmi_target_up{input,target}` | 1 while a subscription is established |
| `gnmi_session_failures_total`, `gnmi_sync_responses_total` | redial and initial-sync visibility |
| `mdt_connections`, `mdt_decode_errors_total{reason}` | dial-out sessions and bad payloads (`compact_gpb`, `malformed`, `framing`) |

## 3. Plan (as executed) and roadmap

**Done in this change**

1. Remove the 2016 implementation, vendored tree, glide files, checked-in binary and test keys (all remain in git history).
2. Go modules; regenerate the Cisco MDT/telemetry protos with modern generators (`buf generate`).
3. Vendor-neutral metric model, strict YAML config with safe env expansion and secret files, shared TLS/mTLS.
4. Engine with bounded queues, per-output isolation, batching, retry/permanent-error classification, graceful drain, self-metrics.
5. Inputs: gNMI dial-in; Cisco MDT dial-out over gRPC/TCP/UDP with KV-GPB and JSON.
6. Processors: filter, tags (incl. per-device inventory enrichment), rename.
7. Outputs: Prometheus exporter, OTLP (gRPC/HTTP), InfluxDB (v1/v2/v3), Kafka, file.
8. Tests for every package, including real IOS XR captures from the original repo, an in-process gNMI target, an in-memory Kafka cluster and an end-to-end binary test. Docker (distroless), Compose lab, Kubernetes manifest, CI.

**Recommended next steps**

* **gNMI dial-out** (`gnmi.proto` server side / `grpctunnel`) for devices behind NAT.
* **Target discovery** from a file/Consul/NetBox with hot reload, and consistent-hash sharding of targets across replicas.
* **Counter semantics:** a processor marking monotonic counters (by path regex) so OTLP can emit `Sum{monotonic}` and Prometheus `counter` types, plus optional rate computation.
* **Kafka input** to chain pipelines (edge collectors → central bus → TSDB writers).
* **Disk-backed queue** option for outputs that must survive long outages.
* **gNMI `Get`/`Capabilities`** at session start to log model/version support per device.

## 4. Migrating from `pipeline.conf`

| 2016 section | 2026 equivalent |
|---|---|
| `type = tcp`, `encap = st`, `listen = :5432` | `{type: cisco_mdt, transport: tcp, listen: ":5432"}` |
| `type = udp` | `{type: cisco_mdt, transport: udp, listen: ...}` |
| `type = grpc`, `encap = gpb`, `listen = :57500` (dial-out server) | `{type: cisco_mdt, transport: grpc, listen: ":57500"}` |
| `type = grpc`, `server = router:57344`, `subscriptions = ...` (dial-in) | `type: gnmi` with `targets` and `subscriptions` (configure gNMI on the router: `grpc port 57400`) |
| `tls = true`, `tls_pem`, `tls_servername` | `tls: {enabled: true, ca_file, cert_file, key_file, server_name}` |
| `username` + RSA-encrypted `password` / `-pem` | `username` + `password: ${ENV}` or `password_file` |
| `type = kafka`, `stage = xport_output`, `brokers`, `topic`, `encoding = json` | `{type: kafka, brokers: [...], topic: ...}` (JSON records keyed by source) |
| `type = kafka`, `stage = xport_input` | not yet ported (see roadmap); consume with any Kafka client |
| `type = metrics`, `output = influx`, `influx = http://…`, `database = …` | `{type: influxdb, url: …, database: …}` (v1) or `org`/`bucket`/`token` (v2/v3) |
| `type = metrics`, `output = prometheus`, `pushgw = …` | `{type: prometheus, listen: ":9273"}` (scrape) or `{type: otlp, protocol: http, endpoint: …/api/v1/otlp/v1/metrics}` |
| `metrics.json` field selection / tags | Not needed: every leaf is exported and list keys become tags automatically. Use the `filter` processor (`include_names`, `keep_fields`, `drop_fields`) to narrow. |
| `type = tap`, `file = dump.txt` | `{type: file, path: dump.jsonl}` |
| `type = grpc` output (re-stream) | `kafka` or `otlp` (to an OpenTelemetry Collector, which can fan out further) |
| `datachanneldepth`, `workers` | `router.queue_size`; per-output `queue_size`, `batch_size`, `flush_interval` |
| `-log`, `-debug` | `log: {level: debug, format: json\|text}` (stderr; collect with your container runtime) |

**Device-side changes**

* Compact GPB is no longer decoded. On IOS XR change the destination group
  to `encoding self-describing-gpb`, or move to gNMI.
* Metric names change shape. With KV-GPB the measurement is still the
  encoding path; field names keep their YANG names (`bytes-received`, not
  `bytes_received`). Use the `rename` processor if dashboards depend on the
  old names.
