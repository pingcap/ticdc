use `generate_column`;
-- see https://github.com/pingcap/tiflow/issues/11704

create table s1 (a int, b int as (a + 1) stored primary key);
insert into s1(a) values (1),(2), (3),(4),(5),(6),(7);
update s1 set a = 10 where a = 1;
update s1 set a = 11 where b = 3;
delete from s1 where b=4;
delete from s1 where a=4;

create table s2 (a int, b int as (a + 1) stored not null, unique index idx1(b));
insert into s2(a) values (1),(2), (3),(4),(5),(6),(7);
update s2 set a = 10 where a = 1;
update s2 set a = 11 where b = 3;
delete from s2 where b=4;
delete from s2 where a=4;

create table s3 (a int not null, c int not null, b int as (a + c) stored not null, unique index idx1(b));
insert into s3 (a, c) values (1, 1), (1, 2), (2, 2);
update s3 set a = 10 where b = 3;
update s3 set c = 20 where a = 1 and c = 1;
delete from s3 where b = 4;
delete from s3 where a = 10;

create table s4 (a varchar(16) not null, c int not null, b varchar(32) as (concat(a, '_', c)) stored not null, unique index idx1(b));
insert into s4 (a, c) values ('a', 1), ('b', 2), ('c', 3);
update s4 set c = 10 where a = 'a';
update s4 set a = 'bb' where b = 'b_2';
delete from s4 where b = 'c_3';
delete from s4 where a = 'bb';
