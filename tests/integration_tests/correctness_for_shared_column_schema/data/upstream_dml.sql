USE test_100;
INSERT INTO t1 (a, b) VALUES (20, 21);
UPDATE t1 SET b = b + 10 WHERE a = 1 AND site_code = '100';
DELETE FROM t1 WHERE a = 2 AND site_code = '100';
UPDATE t1 SET b = b + 1 WHERE a = 12 AND site_code = '100';

USE test_200;
INSERT INTO t1 (a, b) VALUES (20, 21);
UPDATE t1 SET b = b + 10 WHERE a = 1 AND site_code = '200';
DELETE FROM t1 WHERE a = 2 AND site_code = '200';
UPDATE t1 SET b = b + 1 WHERE a = 12 AND site_code = '200';

USE test_300;
INSERT INTO t1 (a, b) VALUES (20, 21);
UPDATE t1 SET b = b + 10 WHERE a = 1 AND site_code = '300';
DELETE FROM t1 WHERE a = 2 AND site_code = '300';
UPDATE t1 SET b = b + 1 WHERE a = 12 AND site_code = '300';

USE test_400;
INSERT INTO t1 (a, b) VALUES (20, 21);
UPDATE t1 SET b = b + 10 WHERE a = 1 AND site_code = '400';
DELETE FROM t1 WHERE a = 2 AND site_code = '400';
UPDATE t1 SET b = b + 1 WHERE a = 12 AND site_code = '400';

USE test_500;
INSERT INTO t1 (a, b) VALUES (20, 21);
UPDATE t1 SET b = b + 10 WHERE a = 1 AND site_code = '500';
DELETE FROM t1 WHERE a = 2 AND site_code = '500';
UPDATE t1 SET b = b + 1 WHERE a = 12 AND site_code = '500';

USE test_600;
INSERT INTO t1 (a, b, e) VALUES (20, 21, 26);
UPDATE t1 SET b = b + 10 WHERE a = 1 AND e = 0;
DELETE FROM t1 WHERE a = 2 AND e = 0;
UPDATE t1 SET b = b + 1 WHERE a = 12 AND e = 0;

USE test_700;
INSERT INTO t1 (a, b, e) VALUES (20, 21, 26);
UPDATE t1 SET b = b + 10 WHERE a = 1 AND e = 0;
DELETE FROM t1 WHERE a = 2 AND e = 0;
UPDATE t1 SET b = b + 1 WHERE a = 12 AND e = 0;

USE test_800;
INSERT INTO t1 (a, b, e) VALUES (20, 21, 26);
UPDATE t1 SET b = b + 10 WHERE a = 1 AND e = 0;
DELETE FROM t1 WHERE a = 2 AND e = 0;
UPDATE t1 SET b = b + 1 WHERE a = 12 AND e = 0; 

USE test_900;
INSERT INTO t1 (a, b, e) VALUES (20, 21, 26);
UPDATE t1 SET b = b + 10 WHERE a = 1 AND e = 0;
DELETE FROM t1 WHERE a = 2 AND e = 0;
UPDATE t1 SET b = b + 1 WHERE a = 12 AND e = 0;

USE test_1000;
INSERT INTO t1 (a, b, e) VALUES (20, 21, 26);
UPDATE t1 SET b = b + 10 WHERE a = 1 AND e = 0;
DELETE FROM t1 WHERE a = 2 AND e = 0;
UPDATE t1 SET b = b + 1 WHERE a = 12 AND e = 0;