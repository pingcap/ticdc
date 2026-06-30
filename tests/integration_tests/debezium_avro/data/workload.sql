USE test;

INSERT INTO tp_account VALUES (12, 34, 'alice', 12.3400, x'010203');
UPDATE tp_account
SET account_id = 35, name = 'bob', balance = 56.7800, payload = x'040506'
WHERE id = 12;
INSERT INTO tp_account
VALUES (18446744073709551615, 99, 'max', 1.2300, x'ff');
DELETE FROM tp_account WHERE id = 18446744073709551615;

INSERT INTO tp_int() VALUES ();
INSERT INTO tp_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
VALUES (1, 2, 3, 4, 5);
INSERT INTO tp_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
VALUES (127, 32767, 8388607, 2147483647, 9223372036854775807);
INSERT INTO tp_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
VALUES (-128, -32768, -8388608, -2147483648, -9223372036854775808);
UPDATE tp_int SET c_int = 0, c_tinyint = 0 WHERE c_smallint = 2;
DELETE FROM tp_int WHERE c_int = 0;

INSERT INTO tp_unsigned_int() VALUES ();
INSERT INTO tp_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
                            c_unsigned_int, c_unsigned_bigint)
VALUES (1, 2, 3, 4, 5);
INSERT INTO tp_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
                            c_unsigned_int, c_unsigned_bigint)
VALUES (255, 65535, 16777215, 4294967295, 18446744073709551615);
INSERT INTO tp_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
                            c_unsigned_int, c_unsigned_bigint)
VALUES (127, 32767, 8388607, 2147483647, 9223372036854775807);
INSERT INTO tp_unsigned_int(c_unsigned_tinyint, c_unsigned_smallint, c_unsigned_mediumint,
                            c_unsigned_int, c_unsigned_bigint)
VALUES (128, 32768, 8388608, 2147483648, 9223372036854775808);
UPDATE tp_unsigned_int SET c_unsigned_int = 0, c_unsigned_tinyint = 0 WHERE c_unsigned_smallint = 65535;
DELETE FROM tp_unsigned_int WHERE c_unsigned_int = 0;

INSERT INTO tp_real() VALUES ();
INSERT INTO tp_real(c_float, c_double, c_decimal, c_decimal_2)
VALUES (2020.0202, 2020.0303, 2020.0404, 2021.1208);
INSERT INTO tp_real(c_float, c_double, c_decimal, c_decimal_2)
VALUES (-2.7182818284, -3.1415926, -8000, -179394.233);
UPDATE tp_real SET c_double = 2.333 WHERE c_double = 2020.0303;

INSERT INTO tp_unsigned_real() VALUES ();
INSERT INTO tp_unsigned_real(c_unsigned_float, c_unsigned_double, c_unsigned_decimal, c_unsigned_decimal_2)
VALUES (2020.0202, 2020.0303, 2020.0404, 2021.1208);
UPDATE tp_unsigned_real SET c_unsigned_double = 2020.0404 WHERE c_unsigned_double = 2020.0303;

INSERT INTO tp_time() VALUES ();
INSERT INTO tp_time(c_date, c_datetime, c_timestamp, c_time, c_year)
VALUES ('2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020');
INSERT INTO tp_time(c_date, c_datetime, c_timestamp, c_time, c_year)
VALUES ('2022-02-22', '2022-02-22 22:22:22', '2020-02-20 02:20:20', '02:20:20', '2021');
UPDATE tp_time SET c_year = '2022' WHERE c_year = '2020';
UPDATE tp_time SET c_date = '2022-02-22' WHERE c_datetime = '2020-02-20 02:20:20';

INSERT INTO tp_text() VALUES ();
INSERT INTO tp_text(c_tinytext, c_text, c_mediumtext, c_longtext)
VALUES ('89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A');
INSERT INTO tp_text(c_tinytext, c_text, c_mediumtext, c_longtext)
VALUES ('89504E470D0A1A0B', '89504E470D0A1A0B', '89504E470D0A1A0B', '89504E470D0A1A0B');
UPDATE tp_text SET c_text = '89504E470D0A1A0B' WHERE c_mediumtext = '89504E470D0A1A0A';

INSERT INTO tp_blob() VALUES ();
INSERT INTO tp_blob(c_tinyblob, c_blob, c_mediumblob, c_longblob)
VALUES (x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A');
INSERT INTO tp_blob(c_tinyblob, c_blob, c_mediumblob, c_longblob)
VALUES (x'89504E470D0A1A0B', x'89504E470D0A1A0B', x'89504E470D0A1A0B', x'89504E470D0A1A0B');
UPDATE tp_blob SET c_blob = x'89504E470D0A1A0B' WHERE c_mediumblob = x'89504E470D0A1A0A';

INSERT INTO tp_char_binary() VALUES ();
INSERT INTO tp_char_binary(c_char, c_varchar, c_binary, c_varbinary)
VALUES ('89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A');
INSERT INTO tp_char_binary(c_char, c_varchar, c_binary, c_varbinary)
VALUES ('89504E470D0A1A0B', '89504E470D0A1A0B', x'89504E470D0A1A0B', x'89504E470D0A1A0B');
UPDATE tp_char_binary SET c_varchar = '89504E470D0A1A0B' WHERE c_binary = x'89504E470D0A1A0A';

INSERT INTO tp_other() VALUES ();
INSERT INTO tp_other(c_enum, c_set, c_bit, c_json)
VALUES ('a', 'a,b', b'1000001', '{
  "key1": "value1",
  "key2": "value2"
}');
INSERT INTO tp_other(c_enum, c_set, c_bit, c_json)
VALUES ('b', 'b,c', b'1000001', '{
  "key1": "value1",
  "key2": "value2",
  "key3": "123"
}');
UPDATE tp_other SET c_enum = 'c' WHERE c_set = 'b,c';

INSERT INTO cs_gbk
VALUES (1, '测试', '中国', '上海', '你好,世界', 0xC4E3BAC3CAC0BDE7);
INSERT INTO cs_gbk
VALUES (2, '部署', '美国', '纽约', '世界,你好', 0xCAC0BDE7C4E3BAC3);
UPDATE cs_gbk SET name = '开发' WHERE name = '测试';
DELETE FROM cs_gbk
WHERE name = '部署'
  AND country = '美国'
  AND city = '纽约'
  AND description = '世界,你好';

INSERT INTO t1 VALUES (12, 34);
INSERT INTO vec(id, data) VALUES (1, '[1,2,3,4,5]');
