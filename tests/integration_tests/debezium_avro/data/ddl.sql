USE test;

ALTER TABLE tp_account ADD COLUMN note VARCHAR(32) NULL;

CREATE TABLE test_ddl1
(
    id INT AUTO_INCREMENT,
    c1 INT,
    PRIMARY KEY (id)
);

CREATE TABLE test_ddl2
(
    id INT AUTO_INCREMENT,
    c1 INT,
    PRIMARY KEY (id)
);

RENAME TABLE test_ddl1 TO test_ddl;

ALTER TABLE test_ddl
    ADD INDEX test_add_index (c1);

DROP INDEX test_add_index ON test_ddl;

ALTER TABLE test_ddl
    ADD COLUMN c2 INT NOT NULL;

TRUNCATE TABLE test_ddl;

DROP TABLE test_ddl2;

CREATE TABLE test_ddl2
(
    id INT AUTO_INCREMENT,
    c1 INT,
    PRIMARY KEY (id)
);

CREATE TABLE test_ddl3
(
    id INT,
    名称 VARCHAR(128),
    PRIMARY KEY (id)
) ENGINE = InnoDB;

ALTER TABLE test_ddl3
    ADD COLUMN 城市 CHAR(32);

ALTER TABLE test_ddl3
    MODIFY COLUMN 城市 VARCHAR(32);

ALTER TABLE test_ddl3
    DROP COLUMN 城市;

CREATE TABLE 表1
(
    id INT,
    name VARCHAR(128),
    PRIMARY KEY (id)
) ENGINE = InnoDB;

RENAME TABLE 表1 TO 表2;

DROP TABLE 表2;
