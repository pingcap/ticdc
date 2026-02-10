use `generate_column`;
-- see https://github.com/pingcap/tiflow/issues/11704

create table t2 (a int, b int as (a + 1) stored primary key);
insert into t2(a) values (1),(2), (3),(4),(5),(6),(7);
update t2 set a = 10 where a = 1;
update t2 set a = 11 where b = 3;
delete from t2 where b=4;
delete from t2 where a=4;

create table t3 (a int, b int as (a + 1) stored not null, unique index idx1(b));
insert into t3(a) values (1),(2), (3),(4),(5),(6),(7);
update t3 set a = 10 where a = 1;
update t3 set a = 11 where b = 3;
delete from t3 where b=4;
delete from t3 where a=4;