USE `test`;

INSERT INTO t_iceberg VALUES
    (1, 'a', '2026-04-16 10:00:00.000000'),
    (2, 'b', '2026-04-16 10:00:00.000000');

UPDATE t_iceberg
SET v = 'bb', updated_at = '2026-04-16 10:00:01.000000'
WHERE id = 2;

DELETE FROM t_iceberg WHERE id = 1;
