-- Runs once, automatically, on first container start (docker-entrypoint-initdb.d).

CREATE USER IF NOT EXISTS otel_reader IDENTIFIED WITH plaintext_password BY 'otel_reader_pw';

GRANT SELECT ON system.text_log TO otel_reader;
GRANT SELECT ON system.opentelemetry_span_log TO otel_reader;
GRANT SELECT ON system.metrics TO otel_reader;
GRANT SELECT ON system.asynchronous_metrics TO otel_reader;
GRANT SELECT ON system.events TO otel_reader;
