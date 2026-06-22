USE test;

INSERT INTO tp_int() VALUES ();
INSERT INTO tp_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
VALUES (1, 2, 3, 4, 5);
INSERT INTO tp_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
VALUES (127, 32767, 8388607, 2147483647, 9223372036854775807);
INSERT INTO tp_int(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
VALUES (-128, -32768, -8388608, -2147483648, -9223372036854775808);
UPDATE tp_int SET c_int = 0, c_tinyint = 0 WHERE id = 2;
DELETE FROM tp_int WHERE id = 2;

INSERT INTO tp_unsigned_int() VALUES ();
INSERT INTO tp_unsigned_int(
    c_unsigned_tinyint,
    c_unsigned_smallint,
    c_unsigned_mediumint,
    c_unsigned_int,
    c_unsigned_bigint
) VALUES (1, 2, 3, 4, 5);
INSERT INTO tp_unsigned_int(
    c_unsigned_tinyint,
    c_unsigned_smallint,
    c_unsigned_mediumint,
    c_unsigned_int,
    c_unsigned_bigint
) VALUES (255, 65535, 16777215, 4294967295, 18446744073709551615);
UPDATE tp_unsigned_int SET c_unsigned_int = 0, c_unsigned_tinyint = 0 WHERE id = 3;
DELETE FROM tp_unsigned_int WHERE id = 3;

INSERT INTO tp_real() VALUES ();
INSERT INTO tp_real(c_float, c_double, c_decimal, c_decimal_2)
VALUES (2020.0202, 2020.0303, 2020.0404, 2021.1208);
INSERT INTO tp_real(c_float, c_double, c_decimal, c_decimal_2)
VALUES (-2.7182818284, -3.1415926, -8000, -179394.233);
UPDATE tp_real SET c_double = 2.333 WHERE id = 2;

INSERT INTO tp_time() VALUES ();
INSERT INTO tp_time(c_date, c_datetime, c_timestamp, c_time, c_year)
VALUES ('2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020');
INSERT INTO tp_time(c_date, c_datetime, c_timestamp, c_time, c_year)
VALUES ('2022-02-22', '2022-02-22 22:22:22', '2020-02-20 02:20:20', '02:20:20', '2021');
UPDATE tp_time SET c_year = '2022' WHERE id = 2;

INSERT INTO tp_text() VALUES ();
INSERT INTO tp_text(c_tinytext, c_text, c_mediumtext, c_longtext)
VALUES ('89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A');
INSERT INTO tp_text(c_tinytext, c_text, c_mediumtext, c_longtext)
VALUES ('89504E470D0A1A0B', '89504E470D0A1A0B', '89504E470D0A1A0B', '89504E470D0A1A0B');
UPDATE tp_text SET c_text = '89504E470D0A1A0B' WHERE id = 2;

INSERT INTO tp_blob() VALUES ();
INSERT INTO tp_blob(c_tinyblob, c_blob, c_mediumblob, c_longblob)
VALUES (x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A');
INSERT INTO tp_blob(c_tinyblob, c_blob, c_mediumblob, c_longblob)
VALUES (x'89504E470D0A1A0B', x'89504E470D0A1A0B', x'89504E470D0A1A0B', x'89504E470D0A1A0B');
UPDATE tp_blob SET c_blob = x'89504E470D0A1A0B' WHERE id = 2;

INSERT INTO tp_char_binary() VALUES ();
INSERT INTO tp_char_binary(c_char, c_varchar, c_binary, c_varbinary)
VALUES ('89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A');
INSERT INTO tp_char_binary(c_char, c_varchar, c_binary, c_varbinary)
VALUES ('89504E470D0A1A0B', '89504E470D0A1A0B', x'89504E470D0A1A0B', x'89504E470D0A1A0B');
UPDATE tp_char_binary SET c_varchar = '89504E470D0A1A0B' WHERE id = 2;

INSERT INTO tp_other() VALUES ();
INSERT INTO tp_other(c_enum, c_set, c_bit, c_json)
VALUES ('a', 'a,b', b'1000001', '{"key1":"value1","key2":"value2"}');
INSERT INTO tp_other(c_enum, c_set, c_bit, c_json)
VALUES ('b', 'b,c', b'1000001', '{"key1":"value1","key2":"value2","key3":"123"}');
UPDATE tp_other SET c_enum = 'c' WHERE id = 3;

INSERT INTO tp_account VALUES (12, 34);
UPDATE tp_account SET account_id = 35 WHERE id = 12;
DELETE FROM tp_account WHERE id = 12;
