USE `test`;

CREATE TABLE t_iceberg (
    id BIGINT PRIMARY KEY,
    v VARCHAR(32),
    updated_at DATETIME(6)
);
