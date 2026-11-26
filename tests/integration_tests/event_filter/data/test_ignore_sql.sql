USE event_filter;

CREATE TABLE t_ignore_sql_original (id INT PRIMARY KEY);
CREATE TABLE t_ignore_sql_quoted (id INT PRIMARY KEY);
CREATE TABLE t_ignore_sql_keyword (id INT PRIMARY KEY);
CREATE TABLE t_ignore_sql_whitespace (id INT PRIMARY KEY);

-- Only this ADD COLUMN matches the original DDL text and should be ignored.
ALTER TABLE t_ignore_sql_original ADD COLUMN c INT;
ALTER TABLE t_ignore_sql_quoted ADD COLUMN c INT;
ALTER TABLE t_ignore_sql_keyword ADD c INT;
ALTER   TABLE t_ignore_sql_whitespace ADD COLUMN c INT;

-- These unfiltered DDLs run after the ADDs on the same tables. Their indexes
-- let the test wait until the downstream has processed every preceding DDL.
CREATE INDEX ignore_sql_done ON t_ignore_sql_original (id);
CREATE INDEX ignore_sql_done ON t_ignore_sql_quoted (id);
CREATE INDEX ignore_sql_done ON t_ignore_sql_keyword (id);
CREATE INDEX ignore_sql_done ON t_ignore_sql_whitespace (id);
