-- Target table for loadgen INSERT workload.
-- A simple event store that gives the cluster realistic write amplification
-- (parts creation, merges) visible in the OTel metrics.
CREATE TABLE IF NOT EXISTS default.events (
    ts       DateTime  DEFAULT now(),
    session_id UInt64,
    event_type LowCardinality(String),
    value    Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (event_type, ts)
SETTINGS index_granularity = 8192;
