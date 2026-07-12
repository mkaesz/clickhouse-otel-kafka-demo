-- Runs once, automatically, on first container start (docker-entrypoint-initdb.d).
--
-- Closes the loop: OTel Collector writes OTLP-JSON messages to three Kafka
-- topics; these Kafka-engine tables consume them back into ClickHouse as raw
-- JSON strings (via JSONAsString), and materialized views persist them into
-- plain MergeTree tables that Grafana queries. This lets you watch the whole
-- chain (ClickHouse -> Collector -> Kafka -> ClickHouse -> Grafana) with SQL
-- instead of a Kafka console consumer.

CREATE DATABASE IF NOT EXISTS sink;

-- ===== metrics =====
CREATE TABLE sink.metrics_kafka (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'clickhouse-metrics',
         kafka_group_name = 'ch-sink-metrics',
         kafka_format = 'JSONAsString';

CREATE TABLE sink.metrics_raw (
    raw String,
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY ingested_at
TTL ingested_at + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW sink.metrics_mv TO sink.metrics_raw AS
SELECT raw FROM sink.metrics_kafka;

-- ===== traces =====
CREATE TABLE sink.traces_kafka (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'clickhouse-traces',
         kafka_group_name = 'ch-sink-traces',
         kafka_format = 'JSONAsString';

CREATE TABLE sink.traces_raw (
    raw String,
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY ingested_at
TTL ingested_at + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW sink.traces_mv TO sink.traces_raw AS
SELECT raw FROM sink.traces_kafka;

-- ===== logs =====
CREATE TABLE sink.logs_kafka (raw String)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'clickhouse-logs',
         kafka_group_name = 'ch-sink-logs',
         kafka_format = 'JSONAsString';

CREATE TABLE sink.logs_raw (
    raw String,
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY ingested_at
TTL ingested_at + INTERVAL 1 DAY;

CREATE MATERIALIZED VIEW sink.logs_mv TO sink.logs_raw AS
SELECT raw FROM sink.logs_kafka;
