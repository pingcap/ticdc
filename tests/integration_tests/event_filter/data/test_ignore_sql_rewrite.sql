USE event_filter;

-- Single-table renames match the original SQL, including ALTER TABLE syntax.
RENAME TABLE sql_rename_raw TO sql_rename_raw_done;
RENAME TABLE sql_rename_restored TO sql_rename_restored_done;
ALTER TABLE sql_alter_rename_raw RENAME TO sql_alter_rename_raw_done;
ALTER TABLE sql_alter_rename_restored RENAME TO sql_alter_rename_restored_done;
RENAME TABLE sql_view_rename_raw TO sql_view_rename_raw_done;
RENAME TABLE sql_view_rename_restored TO sql_view_rename_restored_done;

-- Multi-table renames and drops match each rebuilt, fully qualified statement.
RENAME TABLE sql_multi_ignored TO sql_multi_ignored_done, sql_multi_kept TO sql_multi_kept_done;
DROP TABLE sql_drop_ignored, sql_drop_kept;
DROP VIEW sql_drop_view_ignored, sql_drop_view_kept;

-- EXCHANGE PARTITION matches the rebuilt SQL, including WITHOUT VALIDATION.
ALTER TABLE sql_exchange_ignored EXCHANGE PARTITION p0 WITH TABLE sql_exchange_ignored_normal WITHOUT VALIDATION;
ALTER TABLE sql_exchange_kept EXCHANGE PARTITION p0 WITH TABLE sql_exchange_kept_normal WITHOUT VALIDATION;

-- The target is in another schema, so the executable SQL must qualify the
-- source table. Filtering must still use the original, unqualified reference.
CREATE TABLE filter.sql_like_ignored LIKE sql_like_source;
CREATE TABLE filter.sql_like_kept LIKE sql_like_source;
CREATE VIEW filter.sql_view_ignored AS SELECT id FROM sql_like_source;
CREATE VIEW filter.sql_view_kept AS SELECT id FROM sql_like_source;
CREATE OR REPLACE VIEW filter.sql_view_replace_ignored AS SELECT id + 10 AS id FROM sql_like_source;
CREATE OR REPLACE VIEW filter.sql_view_replace_kept AS SELECT id + 10 AS id FROM sql_like_source;
