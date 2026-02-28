CREATE DATABASE IF NOT EXISTS outbox_test;

CREATE TABLE outbox_test.tenant_events (
    id        VARCHAR(36)  NOT NULL PRIMARY KEY,
    tenant_id VARCHAR(64)  NOT NULL,
    event_key VARCHAR(255) NOT NULL,
    payload   JSON         NOT NULL
);
