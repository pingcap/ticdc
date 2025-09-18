use split_region;

-- These DDLs should work fine as they don't break splitable
-- Add columns (non-unique)
alter table test1 add column c1 int;
alter table test1 add column c2 varchar(100);

-- Drop columns
alter table test2 drop column val;

-- Add non-unique index
alter table test1 add index idx_c1 (c1);

-- Modify column
alter table test1 modify column c1 bigint;

-- Drop index
alter table test1 drop index idx_c1;
