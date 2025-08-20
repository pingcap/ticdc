drop database if exists `test`;
create database `test`;
use `test`;
create table table_1 (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255));
create table table_2 (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255));
create table table_3 (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255));
create table table_4 (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255));
create table table_5 (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255));

split table test.table_1 between (1) and (100000) regions 50;
split table test.table_2 between (1) and (100000) regions 50;
split table test.table_3 between (1) and (100000) regions 50;
split table test.table_4 between (1) and (100000) regions 50;
split table test.table_5 between (1) and (100000) regions 50;