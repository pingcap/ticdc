drop database if exists `conflict_key_generated_column`;
create database `conflict_key_generated_column`;
use `conflict_key_generated_column`;

drop table if exists `t`;
create table `t` (
  `a` int not null,
  `v` int generated always as (`a` + 1) virtual,
  `b` int null,
  `id` bigint not null auto_increment,
  `payload` varbinary(4096),
  primary key (`id`)
);

insert into `t` (`a`, `b`, `payload`)
select 1, 1, repeat('x', 4096)
from information_schema.columns c1
cross join information_schema.columns c2
limit 5000;

update `t` set `b` = null where `id` = 1;
delete from `t` where `id` = 1;

