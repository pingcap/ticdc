drop database if exists `generate_column`;
create database `generate_column`;
use `generate_column`;

create table v1 (a int, b int as (a + 1) virtual not null, c int not null, unique index idx1(b));
insert into v1 (a, c) values (1, 2),(2, 3), (3, 4),(4, 5),(5, 6),(6, 7),(7, 8);
update v1 set a = 10 where a = 1;
update v1 set a = 11 where b = 3;
delete from v1 where b=4;
delete from v1 where a=4;

create table v2 (a int, b int as (a + 1) virtual not null, c int not null, unique index idx1(b), unique index idx2(c));
insert into v2 (a, c) values (1, 2),(2, 3), (3, 4),(4, 5),(5, 6),(6, 7),(7, 8);
update v2 set a = 10 where a = 1;
update v2 set a = 11 where b = 3;
delete from v2 where b=4;
delete from v2 where a=4;

create table v3 (a varchar(16) not null, c int not null, b varchar(32) as (concat(a, '_', c)) virtual not null, unique index idx1(b));
insert into v3 (a, c) values ('a', 1), ('b', 2), ('c', 3);
update v3 set c = 10 where a = 'a';
update v3 set a = 'bb' where b = 'b_2';
delete from v3 where b = 'c_3';
delete from v3 where a = 'bb';

create table v4 (a int not null, c int not null, b int as (a * 100 + c) virtual not null, unique index idx1(b));
insert into v4 (a, c) values (1, 1), (1, 2), (2, 2);
update v4 set c = 9 where b = 102;
update v4 set a = 3 where a = 2 and c = 2;
delete from v4 where b = 101;
delete from v4 where a = 3;
