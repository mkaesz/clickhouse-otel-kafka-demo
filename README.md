# ClickHouse OSS -> OTel Collector -> Kafka -> ClickHouse -> Grafana (local POC)

Everything runs on plain HTTP, no auth beyond a hardcoded demo password, no
TLS. Local testing only — do not reuse these credentials or this compose
file anywhere real.

## What's in here

```
docker-compose.yml
otel-collector-config.yaml
clickhouse/
  config.d/observability.xml     # Prometheus endpoint, text_log, span log, trace sampling=100%
  init-scripts/
    01-reader-user.sql           # read-only "otel_reader" user for the collector
    02-sink-schema.sql           # Kafka-engine tables + MVs that re-ingest the 3 topics back into ClickHouse
grafana/
  provisioning/datasources/clickhouse.yaml
  provisioning/dashboards/dashboards.yaml
  dashboards/flow-overview.json  # pre-built dashboard, auto-loaded
```

## How the data flows

```
ClickHouse (system.metrics / text_log / opentelemetry_span_log)
        |
        v
  OTel Collector  (prometheus + sqlquery receivers)
        |
        v
  Kafka  (clickhouse-metrics / clickhouse-traces / clickhouse-logs topics, OTLP-JSON)
        |
        v
  ClickHouse "sink" database  (Kafka-engine tables -> MVs -> MergeTree, raw JSON captured as-is)
        |
        v
  Grafana  (queries sink.* tables directly)
```

A `loadgen` container runs trivial queries against ClickHouse every couple
seconds, purely so there's always something to see moving through the
pipeline — you don't need to do anything for data to start flowing.

## Run it

```
docker compose up -d
```

First startup takes a minute or two — ClickHouse has to come up and run the
init scripts, Kafka needs to elect itself as its own controller (KRaft,
single node), and the collector waits on both via healthchecks.

## Watch it work

**Grafana** — http://localhost:3000, login `admin` / `admin`. The
"ClickHouse -> Kafka -> ClickHouse flow (POC)" dashboard loads automatically
on the home page (or under Dashboards). Give it ~30-60 seconds after
`docker compose up` before rows appear — that's the round trip: ClickHouse
write -> collector poll (10s interval) -> Kafka -> sink consumer -> table.

**Raw Kafka topics**, if you want to see the OTLP-JSON on the wire before it
gets back into ClickHouse:

```
docker exec -it ch-poc-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic clickhouse-metrics --max-messages 3

docker exec -it ch-poc-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic clickhouse-traces --max-messages 3

docker exec -it ch-poc-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic clickhouse-logs --max-messages 3
```

**Directly in ClickHouse**, source and sink side by side:

```
docker exec -it ch-poc-clickhouse clickhouse-client --query \
  "SELECT count() FROM system.opentelemetry_span_log"

docker exec -it ch-poc-clickhouse clickhouse-client --query \
  "SELECT count() FROM sink.traces_raw"
```

If the sink count is climbing (even a few seconds behind the source count),
the whole chain is working.

**Collector logs**, if something's stuck:

```
docker compose logs -f otel-collector
```

## Tear down

```
docker compose down -v   # -v also drops the clickhouse-data and grafana-data volumes
```

## Known rough edges (fine for a POC, not for anything beyond it)

- `otel_reader`'s password is plaintext in two files (`01-reader-user.sql`
  and `otel-collector-config.yaml`) and matches by hand — if you change one,
  change the other.
- The sink tables store each Kafka message as one raw JSON string
  (`JSONAsString` format) rather than flattening it into typed columns. This
  sidesteps needing to hand-write OTLP-JSON parsing SQL for the POC, but it's
  not how you'd want to query this long-term — for real use you'd parse
  `raw` with `JSONExtract*` functions or land it in a properly typed schema.
- `opentelemetry_start_trace_probability = 1` (100% sampling) is set
  globally via the default profile. Fine for a low-traffic POC; would be
  extremely expensive on a real workload.
- No topic partitioning/replication considerations — single Kafka node,
  single partition per topic (auto-created).
- If you restart just the `clickhouse` container without `docker compose
  down -v`, the init scripts won't re-run (they only run once against an
  empty data directory) — that's expected, not a bug.
