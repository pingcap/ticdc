CREATE DATABASE route_name_src;
CREATE DATABASE route_name_extra;

USE route_name_src;

CREATE TABLE batch_01 (id INT PRIMARY KEY, note VARCHAR(64));
CREATE TABLE batch_02 (id INT PRIMARY KEY, note VARCHAR(64));
CREATE TABLE batch_03 (id INT PRIMARY KEY, note VARCHAR(64));
CREATE TABLE batch_04 (id INT PRIMARY KEY, note VARCHAR(64));
CREATE TABLE batch_05 (id INT PRIMARY KEY, note VARCHAR(64));

INSERT INTO batch_01 VALUES (1, 'batch_01');
INSERT INTO batch_02 VALUES (1, 'batch_02');
INSERT INTO batch_03 VALUES (1, 'batch_03');
INSERT INTO batch_04 VALUES (1, 'batch_04');
INSERT INTO batch_05 VALUES (1, 'batch_05');

CREATE TABLE alt_rename (id INT PRIMARY KEY, note VARCHAR(64));
CREATE TABLE multi_a (id INT PRIMARY KEY, note VARCHAR(64));
CREATE TABLE multi_b (id INT PRIMARY KEY, note VARCHAR(64));
CREATE TABLE cross_move (id INT PRIMARY KEY, note VARCHAR(64));
CREATE TABLE swap_a (id INT PRIMARY KEY, note VARCHAR(64));
CREATE TABLE swap_b (id INT PRIMARY KEY, note VARCHAR(64));
CREATE TABLE drop_1 (id INT PRIMARY KEY, note VARCHAR(64));
CREATE TABLE drop_2 (id INT PRIMARY KEY, note VARCHAR(64));
CREATE TABLE recreate_old (id INT PRIMARY KEY, note VARCHAR(64));
CREATE TABLE failover_old (id INT PRIMARY KEY, note VARCHAR(64));

INSERT INTO alt_rename VALUES (1, 'before alter rename');
INSERT INTO multi_a VALUES (1, 'multi_a');
INSERT INTO multi_b VALUES (1, 'multi_b');
INSERT INTO cross_move VALUES (1, 'cross_move');
INSERT INTO swap_a VALUES (1, 'swap_a_original');
INSERT INTO swap_b VALUES (2, 'swap_b_original');
INSERT INTO drop_1 VALUES (1, 'drop_1');
INSERT INTO drop_2 VALUES (1, 'drop_2');
INSERT INTO recreate_old VALUES (1, 'before recreate');
INSERT INTO failover_old VALUES (1, 'before failover');

CREATE TABLE route_name_extra.extra_seed (id INT PRIMARY KEY, note VARCHAR(64));
INSERT INTO route_name_extra.extra_seed VALUES (1, 'extra_seed');
