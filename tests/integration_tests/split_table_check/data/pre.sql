drop database if exists `split_region`;
create database `split_region`;
use `split_region`;

-- Create tables with primary key but no unique key (splitable)
create table test1 (id int primary key, val int);
create table test2 (id int primary key, val int);

-- Insert some initial data
insert into test1(id, val) values (1, 100), (2, 200), (3, 300), (4, 400), (5, 500);
insert into test2(id, val) values (1, 100), (2, 200), (3, 300), (4, 400), (5, 500);

-- Split tables into multiple regions to enable split table feature
split table split_region.test1 between (1) and (100000) regions 50;
split table split_region.test2 between (1) and (100000) regions 50;

-- Insert more data to populate the regions
insert into test1(id, val) values (10, 1000), (20, 2000), (30, 3000), (40, 4000), (50, 5000);
insert into test1(id, val) values (100, 10000), (200, 20000), (300, 30000), (400, 40000), (500, 50000);
insert into test1(id, val) values (1000, 100000), (2000, 200000), (3000, 300000), (4000, 400000), (5000, 500000);

insert into test2(id, val) values (10, 1000), (20, 2000), (30, 3000), (40, 4000), (50, 5000);
insert into test2(id, val) values (100, 10000), (200, 20000), (300, 30000), (400, 40000), (500, 50000);
insert into test2(id, val) values (1000, 100000), (2000, 200000), (3000, 300000), (4000, 400000), (5000, 500000);
