USE test;

INSERT INTO tp_account VALUES (12, 34, 'alice', 12.3400, x'010203');
UPDATE tp_account
SET account_id = 35, name = 'bob', balance = 56.7800, payload = x'040506'
WHERE id = 12;
INSERT INTO tp_account
VALUES (18446744073709551615, 99, 'max', 1.2300, x'ff');
DELETE FROM tp_account WHERE id = 18446744073709551615;
