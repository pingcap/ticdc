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

CREATE TABLE tp_int
(
    id          INT AUTO_INCREMENT,
    c_tinyint   TINYINT   NULL,
    c_smallint  SMALLINT  NULL,
    c_mediumint MEDIUMINT NULL,
    c_int       INT       NULL,
    c_bigint    BIGINT    NULL,
    PRIMARY KEY (id)
);

CREATE TABLE tp_unsigned_int
(
    id                   INT AUTO_INCREMENT,
    c_unsigned_tinyint   TINYINT   UNSIGNED NULL,
    c_unsigned_smallint  SMALLINT  UNSIGNED NULL,
    c_unsigned_mediumint MEDIUMINT UNSIGNED NULL,
    c_unsigned_int       INT       UNSIGNED NULL,
    c_unsigned_bigint    BIGINT    UNSIGNED NULL,
    PRIMARY KEY (id)
);

CREATE TABLE tp_real
(
    id          INT AUTO_INCREMENT,
    c_float     FLOAT          NULL,
    c_double    DOUBLE         NULL,
    c_decimal   DECIMAL        NULL,
    c_decimal_2 DECIMAL(10, 4) NULL,
    PRIMARY KEY (id)
);

CREATE TABLE tp_unsigned_real
(
    id                   INT AUTO_INCREMENT,
    c_unsigned_float     FLOAT UNSIGNED          NULL,
    c_unsigned_double    DOUBLE UNSIGNED         NULL,
    c_unsigned_decimal   DECIMAL UNSIGNED        NULL,
    c_unsigned_decimal_2 DECIMAL(10, 4) UNSIGNED NULL,
    PRIMARY KEY (id)
);

CREATE TABLE tp_time
(
    id          INT AUTO_INCREMENT,
    c_date      DATE      NULL,
    c_datetime  DATETIME  NULL,
    c_timestamp TIMESTAMP NULL,
    c_time      TIME      NULL,
    c_year      YEAR      NULL,
    PRIMARY KEY (id)
);

CREATE TABLE tp_text
(
    id           INT AUTO_INCREMENT,
    c_tinytext   TINYTEXT   NULL,
    c_text       TEXT       NULL,
    c_mediumtext MEDIUMTEXT NULL,
    c_longtext   LONGTEXT   NULL,
    PRIMARY KEY (id)
);

CREATE TABLE tp_blob
(
    id           INT AUTO_INCREMENT,
    c_tinyblob   TINYBLOB   NULL,
    c_blob       BLOB       NULL,
    c_mediumblob MEDIUMBLOB NULL,
    c_longblob   LONGBLOB   NULL,
    PRIMARY KEY (id)
);

CREATE TABLE tp_char_binary
(
    id          INT AUTO_INCREMENT,
    c_char      CHAR(16)      NULL,
    c_varchar   VARCHAR(16)   NULL,
    c_binary    BINARY(16)    NULL,
    c_varbinary VARBINARY(16) NULL,
    PRIMARY KEY (id)
);

CREATE TABLE tp_other
(
    id     INT AUTO_INCREMENT,
    c_enum ENUM ('a','b','c') NULL,
    c_set  SET ('a','b','c')  NULL,
    c_bit  BIT(64)            NULL,
    c_json JSON               NULL,
    PRIMARY KEY (id)
);

CREATE TABLE cs_gbk
(
    id          INT,
    name        VARCHAR(128) CHARACTER SET gbk,
    country     CHAR(32) CHARACTER SET gbk,
    city        VARCHAR(64),
    description TEXT CHARACTER SET gbk,
    image       TINYBLOB,
    PRIMARY KEY (id)
) ENGINE = InnoDB CHARSET = utf8mb4;

CREATE TABLE t1
(
    id         INT PRIMARY KEY,
    account_id INT NOT NULL,
    UNIQUE KEY account_id_idx (account_id)
);

CREATE TABLE vec
(
    id   INT PRIMARY KEY,
    data VECTOR(5)
);
