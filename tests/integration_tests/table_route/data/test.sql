-- Test mixed DDL and DML operations for table route.
DROP DATABASE IF EXISTS source_db;
CREATE DATABASE source_db;
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
-- DML: INSERT more data
-- ============================================
INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');
INSERT INTO orders VALUES (3, 3, 300.00);

-- ============================================
-- DML: UPDATE data
-- ============================================
UPDATE users SET email = 'alice_updated@example.com' WHERE id = 1;
UPDATE orders SET amount = 150.00 WHERE id = 1;

-- ============================================
-- DML: DELETE data
-- ============================================
DELETE FROM orders WHERE id = 2;

-- ============================================
-- DDL: ALTER TABLE ADD COLUMN
-- ============================================
ALTER TABLE users ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- ============================================
-- DDL: CREATE TABLE (new table should be routed)
-- ============================================
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10, 2)
);

-- Widget starts at 29.99 (>= 15.00), so DELETE WHERE price < 15.00 won't affect it
-- unless the UPDATE (price = 12.99) is applied first
INSERT INTO products VALUES (1, 'Widget', 29.99);
INSERT INTO products VALUES (2, 'Gadget', 19.99);

-- ============================================
-- DDL: CREATE TABLE LIKE
-- ============================================
CREATE TABLE products_backup LIKE products;

INSERT INTO products_backup VALUES (1, 'Widget', 29.99);

-- ============================================
-- DDL: ALTER TABLE DROP COLUMN
-- ============================================
ALTER TABLE users DROP COLUMN created_at;

-- ============================================
-- DDL: ALTER TABLE ADD INDEX
-- ============================================
ALTER TABLE orders ADD INDEX idx_user_id (user_id);

-- ============================================
-- DDL: RENAME TABLE
-- ============================================
CREATE TABLE temp_table (
    id INT PRIMARY KEY,
    value VARCHAR(50)
);
INSERT INTO temp_table VALUES (1, 'test');

RENAME TABLE temp_table TO renamed_table;

-- Verify renamed table works with DML
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
-- DDL: PARTITION TABLE
-- ============================================
CREATE TABLE partitioned_events (
    id INT PRIMARY KEY,
    bucket INT NOT NULL,
    value VARCHAR(50)
) PARTITION BY RANGE (bucket) (
    PARTITION p0 VALUES LESS THAN (10),
    PARTITION p1 VALUES LESS THAN (20)
);

INSERT INTO partitioned_events VALUES (1, 5, 'p0');
INSERT INTO partitioned_events VALUES (2, 15, 'p1');
ALTER TABLE partitioned_events ADD PARTITION (PARTITION p2 VALUES LESS THAN (30));
INSERT INTO partitioned_events VALUES (3, 25, 'p2');
ALTER TABLE partitioned_events TRUNCATE PARTITION p0;
INSERT INTO partitioned_events VALUES (4, 6, 'p0_after_truncate');
ALTER TABLE partitioned_events DROP PARTITION p1;
INSERT INTO partitioned_events VALUES (5, 26, 'p2_more');

-- ============================================
-- DDL: TRUNCATE TABLE
-- ============================================
CREATE TABLE truncate_test (
    id INT PRIMARY KEY,
    data VARCHAR(100)
);
INSERT INTO truncate_test VALUES (1, 'will be truncated');
INSERT INTO truncate_test VALUES (2, 'also truncated');

TRUNCATE TABLE truncate_test;

-- Insert new data after truncate
INSERT INTO truncate_test VALUES (10, 'after truncate');

-- ============================================
-- DDL: DROP TABLE
-- ============================================
CREATE TABLE to_be_dropped (
    id INT PRIMARY KEY
);
INSERT INTO to_be_dropped VALUES (1);

DROP TABLE to_be_dropped;

-- ============================================
-- Mixed operations on existing tables
-- ============================================
-- More inserts
INSERT INTO users VALUES (4, 'Diana', 'diana@example.com');
INSERT INTO users VALUES (5, 'Eve', 'eve@example.com');

-- Batch update
UPDATE users SET name = CONCAT(name, '_v2') WHERE id IN (3, 4);

-- More deletes
DELETE FROM users WHERE id = 5;

-- Update with multiple columns
UPDATE products SET name = 'Super Widget', price = 12.99 WHERE id = 1;

-- Delete with condition
DELETE FROM products WHERE price < 15.00;

-- ============================================
-- Create finish marker table
-- ============================================
CREATE TABLE finish_mark (id INT PRIMARY KEY);
INSERT INTO finish_mark VALUES (1);
