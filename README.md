# ClickHouse OTel Kafka Demo

Multi-tenant observability pipeline: two customer ClickHouse clusters emit
metrics, logs, and traces via OTel Collector → Kafka → a central ClickHouse
cluster → Grafana.

Everything runs on plain HTTP, no TLS. Local testing only — do not reuse
these credentials anywhere real.

## Architecture

```
Customer Cluster 1                          Central Platform
──────────────────                          ────────────────
ch-keeper-customer-1 ←──── coordination
ch-customer-1        ──┐
                       │ Prometheus scrape        ch-keeper-central ←─ coordination
otel-collector-        │ + sqlquery receiver      ch-central (otel.* schema)
  customer-1    ───────┤                               ▲
                       │ publish OTLP-JSON             │ write OTel schema
                       ▼                          otel-collector-central ◄─── Kafka
                     Kafka                             │
                       ▲                               │ Prometheus scrape
                       │ publish OTLP-JSON             │ (ch-keeper-central)
otel-collector-        │
  customer-2    ───────┤                          Grafana
ch-customer-2        ──┘    (queries otel.* on ch-central)
ch-keeper-customer-2 ←──── coordination

Customer Cluster 2
```

## Repository layout

```
docker-compose.yml

ch-customer-1/
  config.d/
    observability.xml   # Prometheus endpoint, text_log, span log, 100% trace sampling
    keeper.xml          # ClickHouse Keeper connection
  init-scripts/
    01-reader-user.sql  # otel_reader user for the OTel Collector
    02-sink-schema.sql  # events table used by the load generator

ch-customer-2/          # identical structure to ch-customer-1
  config.d/
    observability.xml
    keeper.xml
  init-scripts/
    01-reader-user.sql
    02-sink-schema.sql

ch-keeper-customer-1/
  config.xml

ch-keeper-customer-2/
  config.xml

ch-central/
  config.d/
    network.xml
    settings.xml
    keeper.xml
  init-scripts/
    01-otel-schema.sql  # full OTel schema (otel.otel_metrics_*, otel.otel_logs)

ch-keeper-central/
  config.xml

otel-collector-customer-1/
  config.yaml           # scrapes ch-customer-1 + ch-keeper-customer-1 → Kafka

otel-collector-customer-2/
  config.yaml           # scrapes ch-customer-2 + ch-keeper-customer-2 → Kafka

otel-collector-central/
  config.yaml           # Kafka → ch-central; scrapes ch-keeper-central → ch-central

grafana/
  provisioning/
    datasources/clickhouse.yaml
    dashboards/dashboards.yaml
  dashboards/
    ch-cluster-v2.json
```

## Ports (host)

| Service              | HTTP   | Native | Prometheus | Keeper |
|----------------------|--------|--------|------------|--------|
| ch-customer-1        | 8123   | 9000   | 9363       |        |
| ch-customer-2        | 8126   | 9003   | 9364       |        |
| ch-central           | 8124   | 9001   |            |        |
| ch-keeper-customer-1 |        |        | 9365       | 2181   |
| ch-keeper-customer-2 |        |        | 9366       | 2182   |
| ch-keeper-central    |        |        | 9367       | 2183   |
| Kafka                |        | 29092  |            |        |
| Grafana              | 3000   |        |            |        |

## Run it

```bash
docker compose up -d
```

First startup takes a couple of minutes — ClickHouse runs init scripts,
Kafka elects itself as its own KRaft controller, and the collectors wait
on both via healthchecks.

## Watch it work

**Grafana** — http://localhost:3000, login `admin` / `admin`. The
`ch-cluster-v2` dashboard loads automatically. Allow 30–60 seconds after
`docker compose up` for the first rows to appear (collector poll interval +
Kafka round trip).

**Collector logs:**

```bash
docker compose logs -f otel-collector-customer-1
docker compose logs -f otel-collector-customer-2
docker compose logs -f otel-collector-central
```

**Query the sink directly:**

```bash
# metrics landing in ch-central
docker exec -it ch-poc-ch-central clickhouse-client --password clickhouse \
  --query "SELECT ServiceName, MetricName, count() FROM otel.otel_metrics_gauge GROUP BY ServiceName, MetricName ORDER BY ServiceName, MetricName LIMIT 20"

# logs
docker exec -it ch-poc-ch-central clickhouse-client --password clickhouse \
  --query "SELECT ServiceName, count() FROM otel.otel_logs GROUP BY ServiceName"
```

**Raw Kafka topics:**

```bash
docker exec -it ch-poc-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic clickhouse-metrics --max-messages 3

docker exec -it ch-poc-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic clickhouse-logs --max-messages 3
```

## Tear down

```bash
docker compose down -v   # -v also removes all named volumes
```

## Known rough edges (POC only)

- Passwords are hardcoded in plain text across config files — change one,
  change them all.
- `opentelemetry_start_trace_probability = 1` (100% sampling) is set in the
  default profile — fine for a low-traffic demo, very expensive on real
  workloads.
- Single Kafka node, single partition, no replication.
- Init scripts only run against an empty data directory. If you restart a
  ClickHouse container without `-v`, they will not re-run — this is expected.
