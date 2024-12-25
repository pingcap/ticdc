use `fail_over_ddl_test2`;

create table t1 (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    val INT DEFAULT 0,
                    col0 INT NOT NULL
);

create table t2 (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    val INT DEFAULT 0,
                    col0 INT NOT NULL
);

INSERT INTO t1 (val, col0) VALUES (1, 1);
INSERT INTO t1 (val, col0) VALUES (2, 2);
INSERT INTO t1 (val, col0) VALUES (3, 3);
INSERT INTO t1 (val, col0) VALUES (4, 4);
INSERT INTO t1 (val, col0) VALUES (5, 5);
INSERT INTO t1 (val, col0) VALUES (6, 6);
INSERT INTO t1 (val, col0) VALUES (7, 7);
INSERT INTO t1 (val, col0) VALUES (8, 8);
INSERT INTO t1 (val, col0) VALUES (9, 9);
INSERT INTO t1 (val, col0) VALUES (10, 10);

INSERT INTO t2 (val, col0) VALUES (1, 1);
INSERT INTO t2 (val, col0) VALUES (2, 2);
INSERT INTO t2 (val, col0) VALUES (3, 3);
INSERT INTO t2 (val, col0) VALUES (4, 4);
INSERT INTO t2 (val, col0) VALUES (5, 5);
INSERT INTO t2 (val, col0) VALUES (6, 6);
INSERT INTO t2 (val, col0) VALUES (7, 7);
INSERT INTO t2 (val, col0) VALUES (8, 8);
INSERT INTO t2 (val, col0) VALUES (9, 9);
INSERT INTO t2 (val, col0) VALUES (10, 10);
