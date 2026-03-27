CREATE DATABASE IF NOT EXISTS outbox_test;

CREATE TABLE outbox_test.events (
    id        VARCHAR(36)  NOT NULL PRIMARY KEY,
    event_key VARCHAR(255) NOT NULL,
    payload   JSON         NOT NULL,
    status    VARCHAR(32)  NOT NULL DEFAULT 'pending'
);
