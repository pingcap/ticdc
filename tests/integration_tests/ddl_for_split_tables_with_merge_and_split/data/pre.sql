drop database if exists `test`;
create database `test`;
use `test`;
create table table_1 (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255));

split table test.table_1 between (1) and (100000) regions 50;