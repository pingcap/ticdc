USE test;

UPDATE tp_account SET note = 'after ddl' WHERE id = 12;
INSERT INTO tp_account(id, account_id, name, balance, payload, note)
VALUES (15, 45, 'carol', 90.1200, x'070809', 'deleted');
DELETE FROM tp_account WHERE id = 15;
INSERT INTO tp_account(id, account_id, name, balance, payload, note)
VALUES (16, 46, 'dave', NULL, NULL, 'final');
