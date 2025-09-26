-- mark finish table
USE common_1;

create database common;
create table a (a bigint primary key, b int);
create table b like a;
rename table a to common.c, b to a, common.c to b;

insert into a values (1, 2);
insert into b values (3, 4), (5, 6);

create table x (a bigint primary key, b int);
create table common.y (a bigint primary key, b int);
rename table x to z, common.y to x, z to common.y;

insert into x values (1, 2);
insert into common.y values (3, 4), (5, 6);

create table test1 (a bigint primary key, b int);
create table common.test4 (a bigint primary key, b int);
rename table test1 to common.test2, common.test2 to test3, common.test4 to test1, test3 to common.test4;

insert into test1 values (1, 2);
insert into common.test4 values (3, 4), (5, 6);

CREATE TABLE finish_mark
(
    a int primary key
);
