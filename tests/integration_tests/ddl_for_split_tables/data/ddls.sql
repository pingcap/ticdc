use split_region;

alter table test1 add column c1 int;

alter table test2 drop column val;

alter table test1 add index idx_test (id);

alter table test1 modify column c1 bigint;

alter table test1 drop index idx_test;

truncate table test2;

rename table test1 to test3;

-- drop table test3;

-- recover table test3;