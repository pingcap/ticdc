-- mark finish table
USE `common_1`;

create table a (a bigint primary key, b int);
create table b like a;
rename table a to c, b to a, c to b;

CREATE TABLE finish_mark
(
    a int primary key
);
