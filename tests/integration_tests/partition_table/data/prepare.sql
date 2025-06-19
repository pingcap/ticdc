drop database if exists `partition_table`;
drop database if exists `partition_table2`;
create database `partition_table`;
use `partition_table`;

create table t1 (a int primary key) PARTITION BY RANGE ( a ) ( PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11),PARTITION p2 VALUES LESS THAN (21));
insert into t1 values (1),(2),(3),(4),(5),(6);
insert into t1 values (7),(8),(9);
insert into t1 values (11),(12),(20);
alter table t1 add partition (partition p3 values less than (30), partition p4 values less than (40));
insert into t1 values (25),(29),(35); /*these values in p3,p4*/
alter table t1 truncate partition p0;
alter table t1 drop partition p1;
insert into t1 values (7),(8),(9);
update t1 set a=a+10 where a=9;

/* exchange partition case 1: source table and target table in same database */
create table t2 (a int primary key);
ALTER TABLE t1 EXCHANGE PARTITION p3 WITH TABLE t2;
insert into t2 values (100),(101),(102),(103),(104),(105); /*these values will be replicated to in downstream t2*/
insert into t1 values (25),(29); /*these values will be replicated to in downstream t1.p3*/

/* exchange partition case 2: source table and target table in different database */
create database `partition_table2`;
create table partition_table2.t2 (a int primary key);
ALTER TABLE t1 EXCHANGE PARTITION p3 WITH TABLE partition_table2.t2;
-- insert into partition_table2.t2 values (1002),(1012),(1022),(1032),(1042),(1052); /*these values will be replicated to in downstream t2*/
-- insert into t1 values (21),(28); /*these values will be replicated to in downstream t1.p3*/

create table finish_mark (a int primary key);
