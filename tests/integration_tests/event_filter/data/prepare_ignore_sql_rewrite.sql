USE event_filter;

CREATE TABLE sql_query_barrier (id INT PRIMARY KEY);
CREATE TABLE sql_like_source (id INT PRIMARY KEY);
INSERT INTO sql_like_source VALUES (7);

CREATE TABLE sql_rename_raw (id INT PRIMARY KEY);
CREATE TABLE sql_rename_restored (id INT PRIMARY KEY);
CREATE TABLE sql_alter_rename_raw (id INT PRIMARY KEY);
CREATE TABLE sql_alter_rename_restored (id INT PRIMARY KEY);
CREATE VIEW sql_view_rename_raw AS SELECT id FROM sql_like_source;
CREATE VIEW sql_view_rename_restored AS SELECT id FROM sql_like_source;
CREATE TABLE sql_multi_ignored (id INT PRIMARY KEY);
CREATE TABLE sql_multi_kept (id INT PRIMARY KEY);
CREATE TABLE sql_drop_ignored (id INT PRIMARY KEY);
CREATE TABLE sql_drop_kept (id INT PRIMARY KEY);
CREATE VIEW sql_drop_view_ignored AS SELECT id FROM sql_like_source;
CREATE VIEW sql_drop_view_kept AS SELECT id FROM sql_like_source;

CREATE TABLE sql_exchange_ignored (id INT PRIMARY KEY)
    PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (100), PARTITION p1 VALUES LESS THAN MAXVALUE);
CREATE TABLE sql_exchange_ignored_normal (id INT PRIMARY KEY);
CREATE TABLE sql_exchange_kept (id INT PRIMARY KEY)
    PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (100), PARTITION p1 VALUES LESS THAN MAXVALUE);
CREATE TABLE sql_exchange_kept_normal (id INT PRIMARY KEY);
INSERT INTO sql_exchange_ignored VALUES (1);
INSERT INTO sql_exchange_ignored_normal VALUES (2);
INSERT INTO sql_exchange_kept VALUES (1);
INSERT INTO sql_exchange_kept_normal VALUES (2);

CREATE VIEW filter.sql_view_replace_ignored AS SELECT id FROM sql_like_source;
CREATE VIEW filter.sql_view_replace_kept AS SELECT id FROM sql_like_source;
