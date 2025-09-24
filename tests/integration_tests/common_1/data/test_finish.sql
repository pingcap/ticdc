-- mark finish table
USE common_1;

create database common;
create table a (a bigint primary key, b int);
create table b like a;
rename table a to common.c, b to a, common.c to b;

insert into a values (1, 2);
insert into b values (3, 4), (5, 6);

CREATE TABLE finish_mark
(
    a int primary key
);
