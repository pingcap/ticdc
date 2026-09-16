DROP DATABASE IF EXISTS ignore_update_only_columns;
CREATE DATABASE ignore_update_only_columns;
USE ignore_update_only_columns;

CREATE TABLE user_a (
  id INT PRIMARY KEY,
  email VARCHAR(128) NOT NULL,
  name VARCHAR(128),
  version INT,
  updated_at INT,
  UNIQUE KEY uk_email (email)
);

CREATE TABLE user_b (
  id INT PRIMARY KEY,
  email VARCHAR(128) NOT NULL,
  name VARCHAR(128),
  version INT,
  updated_at INT,
  UNIQUE KEY uk_email (email)
);

INSERT INTO user_a VALUES
  (1, 'a@example.com', 'Alice', 1, 10),
  (2, 'b@example.com', 'Bob', 1, 10),
  (3, 'c@example.com', 'Carol', 1, 10),
  (4, 'd@example.com', 'Dave', 1, 10),
  (5, 'e@example.com', 'Eve', 1, 10);

INSERT INTO user_b VALUES
  (1, 'user-b-a@example.com', 'User B Alice', 1, 10),
  (2, 'user-b-b@example.com', 'User B Bob', 1, 10);

UPDATE user_a SET version = 2, updated_at = 20 WHERE id = 1;
UPDATE user_b SET version = 2, updated_at = 20 WHERE id = 1;

UPDATE user_a SET name = 'Bob updated', version = 2 WHERE id = 2;
UPDATE user_a SET id = 30, version = 2 WHERE id = 3;
UPDATE user_a SET email = 'd-updated@example.com', version = 2 WHERE id = 4;

INSERT INTO user_a VALUES (6, 'f@example.com', 'Frank', 1, 10);
DELETE FROM user_a WHERE id = 5;

CREATE TABLE finish_mark (id INT PRIMARY KEY);
