drop database if exists `multi_tables_ddl_test`;
create database `multi_tables_ddl_test`;
use `multi_tables_ddl_test`;

create table t1 (
 value64  bigint unsigned  not null,
 primary key(value64)
);
insert into t1 values(17156792991891826145);
insert into t1 values(91891826145);
delete from t1 where value64=17156792991891826145;
update t1 set value64=17156792991891826;
update t1 set value64=56792991891826;

rename table t1 to t1_1;

create table t2 (
 value64  bigint unsigned  not null,
 primary key(value64)
);
insert into t2 values(17156792991891826145);
insert into t2 values(91891826145);
delete from t2 where value64=91891826145;
update t2 set value64=17156792991891826;
update t2 set value64=56792991891826;

rename table t2 to t2_2;

create table t1 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

create table t2 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

create table t3 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

create table t4 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

/*use by error changefeed*/
create table t5 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

/*use by error1 changefeed*/
create table t6 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

/*use by error1 changefeed*/
create table t7 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

/*use by error1 changefeed*/
create table t8 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

/*use by error2 changefeed*/
create table t10 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

/*use by error2 changefeed*/
create table t11 (
 value64  bigint unsigned  not null,
 value32  integer          not null,
 primary key(value64, value32)
);

insert into t1 values(17156792991891826145, 1);
insert into t1 values( 9223372036854775807, 2);
insert into t2 values(17156792991891826145, 3);
insert into t2 values( 9223372036854775807, 4);

insert into t5 values(17156792991891826145, 1);
insert into t6 values( 9223372036854775807, 2);
insert into t7 values(17156792991891826145, 3);
insert into t8 values( 9223372036854775807, 4);


rename table t1 to t1_7, t2 to t2_7;

insert into t1_7 values(91891826145, 5);
insert into t1_7 values(685477580, 6);
insert into t2_7 values(1715679991826145, 7);
insert into t2_7 values(2036854775807, 8);

create table rename_mix_normal_1 (
 id int not null,
 value32 int not null,
 primary key(id)
);

create table rename_mix_part_1 (
 id int not null,
 value32 int not null,
 primary key(id)
) partition by hash(id) partitions 2;

insert into rename_mix_normal_1 values(1, 10), (2, 20);
insert into rename_mix_part_1 values(11, 110), (12, 120);

rename table rename_mix_normal_1 to rename_mix_normal_1_done, rename_mix_part_1 to rename_mix_part_1_done;

insert into rename_mix_normal_1_done values(3, 30);
insert into rename_mix_part_1_done values(13, 130);

create table rename_mix_part_2 (
 id int not null,
 value32 int not null,
 primary key(id)
) partition by hash(id) partitions 2;

create table rename_mix_normal_2 (
 id int not null,
 value32 int not null,
 primary key(id)
);

insert into rename_mix_part_2 values(21, 210), (22, 220);
insert into rename_mix_normal_2 values(31, 310), (32, 320);

rename table rename_mix_part_2 to rename_mix_part_2_done, rename_mix_normal_2 to rename_mix_normal_2_done;

insert into rename_mix_part_2_done values(23, 230);
insert into rename_mix_normal_2_done values(33, 330);

insert into t3 select * from t1_7;
insert into t4 select * from t2_7;
drop table t3, t4;



/* cf_err1, filter.rules = ["multi_tables_ddl_test.t5", "multi_tables_ddl_test.t6", "multi_tables_ddl_test.t7, "multi_tables_ddl_test.t8"] */
/* replicate successful, they are all in `filter.rule` */
rename table t5 to t55, t6 to t66;
/* discard by cdc totally, they are all not in `filter.rule` */
rename table t55 to t555, t66 to t666;
/* replicate successful, since t8 in `filter.rule` */
rename table t8 to t88;
/* discard, t88 and t888 both not in `filter.rule` */
rename table t88 to t888;


/* cf_err2, filter.rules = ["multi_tables_ddl_test.t9", "multi_tables_ddl_test.t10"] */
/* replicate successful, since t10 in `filter.rule` */
rename table t10 to t9;
/* replicate successful, since t9 in `filter.rule` */
rename table t9 to t13;
/* discard, t13 and t14 both not in `filter.rule` */
rename table t13 to t14;
/* error, t11 not match `filter.rule` and t9 match `filter.rule` */
rename table t11 to t9;
create table finish_mark(id int primary key);

USE view_ddl_test;
-- Regression for #6548: excluded views still pass through the shared SchemaStore.
create database unselected_db;
create view unselected_db.conflict_view as select 1 as id;
create or replace view unselected_db.conflict_view as select 2 as id;
rename table unselected_db.conflict_view to unselected_db.conflict_view_done;
drop view unselected_db.conflict_view_done;
drop database unselected_db;

create table finish_mark(id int primary key);
create table rename_view_base(id int primary key);
insert into rename_view_base values(7);
create view rename_view_single as select id from rename_view_base;
rename table rename_view_single to rename_view_single_done;

-- Mixed table/view renames must work in either order.
create table rename_view_table_1(id int primary key);
insert into rename_view_table_1 values(1);
create view rename_view_mixed_1 as select id from rename_view_base;
rename table rename_view_table_1 to rename_view_table_1_done, rename_view_mixed_1 to rename_view_mixed_1_done;
insert into rename_view_table_1_done values(2);

create table rename_view_table_2(id int primary key);
insert into rename_view_table_2 values(1);
create view rename_view_mixed_2 as select id from rename_view_base;
rename table rename_view_mixed_2 to rename_view_mixed_2_done, rename_view_table_2 to rename_view_table_2_done;
insert into rename_view_table_2_done values(2);

-- Ignoring the table rename must preserve the emitted view rename metadata.
create table rename_view_ignored_table_3(id int primary key);
insert into rename_view_ignored_table_3 values(1);
create view rename_view_mixed_3 as select id from rename_view_base;
rename table rename_view_ignored_table_3 to rename_view_ignored_table_3_done, rename_view_mixed_3 to rename_view_mixed_3_done;

create table rename_view_ignored_table_4(id int primary key);
insert into rename_view_ignored_table_4 values(1);
create view rename_view_mixed_4 as select id from rename_view_base;
rename table rename_view_mixed_4 to rename_view_mixed_4_done, rename_view_ignored_table_4 to rename_view_ignored_table_4_done;

-- Ignoring the view rename must not interfere with the table rename or its DML.
create table rename_view_table_5(id int primary key);
insert into rename_view_table_5 values(1);
create view rename_view_ignored_view_5 as select id from rename_view_base;
rename table rename_view_table_5 to rename_view_table_5_done, rename_view_ignored_view_5 to rename_view_ignored_view_5_done;
insert into rename_view_table_5_done values(2);

create table rename_view_table_6(id int primary key);
insert into rename_view_table_6 values(1);
create view rename_view_ignored_view_6 as select id from rename_view_base;
rename table rename_view_ignored_view_6 to rename_view_ignored_view_6_done, rename_view_table_6 to rename_view_table_6_done;
insert into rename_view_table_6_done values(2);

insert into view_ddl_test.finish_mark values(100);
