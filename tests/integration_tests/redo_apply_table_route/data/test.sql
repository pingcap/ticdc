-- Test mixed DDL and DML operations for redo apply with table route.
DROP DATABASE IF EXISTS source_db;
DROP DATABASE IF EXISTS source_extra_db;
CREATE DATABASE source_db;
CREATE DATABASE source_extra_db;
USE source_db;

-- ============================================
-- DDL: CREATE TABLE with initial DML
-- ============================================
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    user_id INT,
    amount DECIMAL(10, 2)
);

INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');

INSERT INTO orders VALUES (1, 1, 100.00);
INSERT INTO orders VALUES (2, 2, 200.00);

-- ============================================
-- DML: INSERT, UPDATE and DELETE
-- ============================================
INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');
INSERT INTO orders VALUES (3, 3, 300.00);
UPDATE users SET email = 'alice_updated@example.com' WHERE id = 1;
UPDATE orders SET amount = 150.00 WHERE id = 1;
DELETE FROM orders WHERE id = 2;

-- ============================================
-- DDL: ALTER TABLE and CREATE TABLE LIKE
-- ============================================
ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10, 2)
);

INSERT INTO products VALUES (1, 'Widget', 29.99);
INSERT INTO products VALUES (2, 'Gadget', 19.99);

CREATE TABLE products_backup LIKE products;
INSERT INTO products_backup VALUES (1, 'Widget', 29.99);

ALTER TABLE users DROP COLUMN created_at;
ALTER TABLE orders ADD INDEX idx_user_id (user_id);

-- ============================================
-- DDL: CROSS DATABASE
-- ============================================
CREATE TABLE `source_extra_db`.`external_users` LIKE `source_db`.`users`;
INSERT INTO `source_extra_db`.`external_users`
    SELECT `id`, `name`, `email` FROM `source_db`.`users` WHERE `id` <= 2;
UPDATE `source_extra_db`.`external_users` SET `email` = 'external_alice@example.com' WHERE `id` = 1;

CREATE TABLE `source_extra_db`.`external_users_from_default` LIKE `users`;
INSERT INTO `source_extra_db`.`external_users_from_default`
    SELECT `id`, `name`, `email` FROM `source_db`.`users` WHERE `id` IN (1, 3);
UPDATE `source_extra_db`.`external_users_from_default` SET `email` = 'default_charlie@example.com' WHERE `id` = 3;

CREATE TABLE `source_db`.`cross_move_source` (
    id INT PRIMARY KEY,
    value VARCHAR(50)
);
INSERT INTO `source_db`.`cross_move_source` VALUES (1, 'move_source');
RENAME TABLE `source_db`.`cross_move_source` TO `source_extra_db`.`cross_move_target`;
INSERT INTO `source_extra_db`.`cross_move_target` VALUES (2, 'move_target');

-- ============================================
-- DDL: RENAME TABLE
-- ============================================
CREATE TABLE temp_table (
    id INT PRIMARY KEY,
    value VARCHAR(50)
);
INSERT INTO temp_table VALUES (1, 'test');

RENAME TABLE temp_table TO renamed_table;
INSERT INTO renamed_table VALUES (2, 'test2');
UPDATE renamed_table SET value = 'updated' WHERE id = 1;

-- ============================================
-- DDL: RENAME TABLE with multiple table pairs
-- ============================================
CREATE TABLE multi_rename_a (
    id INT PRIMARY KEY,
    value VARCHAR(50)
);
CREATE TABLE multi_rename_b (
    id INT PRIMARY KEY,
    value VARCHAR(50)
);
INSERT INTO multi_rename_a VALUES (1, 'a');
INSERT INTO multi_rename_b VALUES (1, 'b');

RENAME TABLE multi_rename_a TO multi_rename_a_new, multi_rename_b TO multi_rename_b_new;
INSERT INTO multi_rename_a_new VALUES (2, 'a2');
UPDATE multi_rename_b_new SET value = 'b2' WHERE id = 1;

-- ============================================
-- DDL: CREATE VIEW and DROP VIEW
-- ============================================
CREATE VIEW `source_db`.`user_order_view` AS
    SELECT `u`.`id`, `u`.`name`, `o`.`amount`
    FROM `source_db`.`users` AS `u`
    JOIN `source_db`.`orders` AS `o` ON `u`.`id` = `o`.`user_id`;

CREATE VIEW `source_db`.`transient_view` AS
    SELECT `id`, `name` FROM `source_db`.`users`;

DROP VIEW `source_db`.`transient_view`;

-- ============================================
-- DDL: TRUNCATE TABLE and DROP TABLE
-- ============================================
CREATE TABLE truncate_test (
    id INT PRIMARY KEY,
    data VARCHAR(100)
);
INSERT INTO truncate_test VALUES (1, 'will be truncated');
INSERT INTO truncate_test VALUES (2, 'also truncated');

TRUNCATE TABLE truncate_test;
INSERT INTO truncate_test VALUES (10, 'after truncate');

CREATE TABLE to_be_dropped (
    id INT PRIMARY KEY
);
INSERT INTO to_be_dropped VALUES (1);
DROP TABLE to_be_dropped;

-- ============================================
-- Mixed operations on existing tables
-- ============================================
INSERT INTO users VALUES (4, 'Diana', 'diana@example.com');
INSERT INTO users VALUES (5, 'Eve', 'eve@example.com');
UPDATE users SET name = CONCAT(name, '_v2') WHERE id IN (3, 4);
DELETE FROM users WHERE id = 5;
UPDATE products SET name = 'Super Widget', price = 12.99 WHERE id = 1;
DELETE FROM products WHERE price < 15.00;

-- ============================================
-- Redo replay regression tables and finish marker
-- ============================================
-- The unique-key updates in the redo-only phase must be split into deferred
-- inserts. Each transaction changes 64 rows, exceeding the spill threshold.
CREATE TABLE generated_updates (
    id INT PRIMARY KEY,
    uk INT NOT NULL UNIQUE,
    g INT GENERATED ALWAYS AS (uk + 1) STORED,
    v INT
);
INSERT INTO generated_updates (id, uk, v) VALUES (1, 1, 10);
INSERT INTO generated_updates (id, uk, v) SELECT id + 1, uk + 1, v FROM generated_updates;
INSERT INTO generated_updates (id, uk, v) SELECT id + 2, uk + 2, v FROM generated_updates;
INSERT INTO generated_updates (id, uk, v) SELECT id + 4, uk + 4, v FROM generated_updates;
INSERT INTO generated_updates (id, uk, v) SELECT id + 8, uk + 8, v FROM generated_updates;
INSERT INTO generated_updates (id, uk, v) SELECT id + 16, uk + 16, v FROM generated_updates;
INSERT INTO generated_updates (id, uk, v) SELECT id + 32, uk + 32, v FROM generated_updates;

CREATE TABLE recovery_guard (id INT PRIMARY KEY, v VARCHAR(32));

CREATE TABLE finish_mark (id INT PRIMARY KEY);
INSERT INTO finish_mark VALUES (1);
