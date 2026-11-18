CREATE DATABASE mysql_conflict_keys;
USE mysql_conflict_keys;

-- Different primary keys can still conflict under the collation's weights.
CREATE TABLE accent (
    id INT PRIMARY KEY,
    u VARCHAR(32) NOT NULL,
    UNIQUE KEY uk(u)
) CHARSET utf8mb4 COLLATE utf8mb4_general_ci;

-- utf8mb4_bin uses PAD SPACE semantics even though it is case sensitive.
CREATE TABLE pad_space (
    id INT PRIMARY KEY,
    u VARCHAR(32) NOT NULL,
    UNIQUE KEY uk(u)
) CHARSET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE prefix (
    id INT PRIMARY KEY,
    u VARCHAR(32) NOT NULL,
    UNIQUE KEY uk(u(4))
) CHARSET utf8mb4 COLLATE utf8mb4_bin;

-- Distinct strings with the same length conflict on the generated value.
CREATE TABLE virtual_generated (
    id INT PRIMARY KEY,
    u VARCHAR(128) NOT NULL,
    g INT AS (LENGTH(u)) VIRTUAL,
    UNIQUE KEY uk(g)
);

CREATE TABLE stored_generated (
    id INT PRIMARY KEY,
    u VARCHAR(128) NOT NULL,
    g INT AS (LENGTH(u)) STORED,
    UNIQUE KEY uk(g)
);
