USE `test`;

INSERT INTO t_iceberg VALUES
    (1, 'a', 10),
    (2, 'b', 20),
    (3, 'c', 30);

UPDATE t_iceberg SET score = 11 WHERE id = 1;
DELETE FROM t_iceberg WHERE id = 2;

ALTER TABLE t_iceberg ADD COLUMN extra VARCHAR(32);
UPDATE t_iceberg SET extra = 'from-update' WHERE id = 1;
INSERT INTO t_iceberg VALUES (4, 'd', 40, 'from-insert');
