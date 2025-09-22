use `mysql_sink_retry_duplicate_entry`;

-- make sure `nullable` can be handled by the mounter and mq encoding protocol
INSERT INTO cdc_multi_data_type() VALUES ();

INSERT INTO cdc_multi_data_type( t_boolean, t_bigint, t_double, t_decimal, t_bit
                               , t_date, t_datetime, t_timestamp, t_time, t_year
                               , t_char, t_varchar, t_blob, t_text, t_enum
                               , t_set, t_json)
VALUES ( true, 9223372036854775807, 123.123, 123456789012.123456789012, b'1000001'
       , '1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
       , '测', '测试', 'blob', '测试text', 'enum2'
       , 'a,b', NULL);

INSERT INTO cdc_multi_data_type( t_boolean, t_bigint, t_double, t_decimal, t_bit
                               , t_date, t_datetime, t_timestamp, t_time, t_year
                               , t_char, t_varchar, t_blob, t_text, t_enum
                               , t_set, t_json)
VALUES ( true, 9223372036854775807, 678, 321, b'1000001'
       , '1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
       , '测', '测试', 'blob', '测试text', 'enum2'
       , 'a,b', NULL);

INSERT INTO cdc_multi_data_type(t_boolean)
VALUES (TRUE);

INSERT INTO cdc_multi_data_type(t_boolean)
VALUES (FALSE);

INSERT INTO cdc_multi_data_type(t_bigint)
VALUES (-9223372036854775808);

INSERT INTO cdc_multi_data_type(t_bigint)
VALUES (9223372036854775807);

INSERT INTO cdc_multi_data_type(t_json)
VALUES ('{
  "key1": "value1",
  "key2": "value2"
}');