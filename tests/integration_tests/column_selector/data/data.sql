drop database if exists `test`;
create database `test`;
use `test`;

create table t1 (
    a int primary key,
    b varchar(64),
    c varchar(64)
);

insert into t1 values (1, 'keep_b_t1', 'filtered_c_t1');
insert into t1 values (2, 'keep_b_t1_2', 'filtered_c_t1_2');

create table t2 (
    a int primary key,
    b varchar(64),
    c varchar(64)
);

insert into t2 values (1, 'filtered_b_t2', 'keep_c_t2');
insert into t2 values (2, 'filtered_b_t2_2', 'keep_c_t2_2');

create table t3 (
    a int primary key,
    b varchar(64),
    c varchar(64)
);

insert into t3 values (1, 'filtered_b_t3', 'keep_c_t3');
insert into t3 values (2, 'filtered_b_t3_2', 'keep_c_t3_2');

drop database if exists `test1`;
create database `test1`;
use `test1`;

create table t1 (
    column0 int primary key,
    column1 varchar(64),
    column2 varchar(64)
);

insert into t1 values (1, 'filtered_column1_test1', 'keep_column2_test1');
insert into t1 values (2, 'filtered_column1_test1_2', 'keep_column2_test1_2');

create table finishmark(id int primary key);
