CREATE DATABASE IF NOT EXISTS outbox_test;

-- Column names (trace_parent, trace_state) intentionally differ from their
-- Kafka header keys (traceparent, tracestate) to exercise the key->column
-- name mapping in [sink.outbox.header-columns].
CREATE TABLE outbox_test.traced_events (
    id           VARCHAR(36)  NOT NULL PRIMARY KEY,
    event_key    VARCHAR(255) NOT NULL,
    payload      JSON         NOT NULL,
    trace_parent VARCHAR(55)  NOT NULL,
    trace_state  VARCHAR(256) NOT NULL
);
