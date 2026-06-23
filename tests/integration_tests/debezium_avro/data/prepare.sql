DROP DATABASE IF EXISTS test;
CREATE DATABASE test;
USE test;

CREATE TABLE tp_account (
    id BIGINT UNSIGNED PRIMARY KEY,
    account_id INT NOT NULL,
    name VARCHAR(64) NULL,
    balance DECIMAL(20, 4) NULL,
    payload VARBINARY(16) NULL
);
