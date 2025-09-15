use split_region;

-- These DDLs will break splitable and should cause an error
-- Add unique key to test1 (this will make the table non-splitable)
alter table test1 add unique key uk_c1 (c1);

-- Add unique key to test2 (this will also make the table non-splitable)
alter table test2 add unique key uk_id (id);
