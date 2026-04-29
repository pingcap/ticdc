CREATE OR REPLACE PROCEDURE TICDC_META.SP_APPLY_DDL_UP_TO(
  p_integration_id STRING,
  p_upper_ts NUMBER(20, 0)
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_applied NUMBER(20, 0) DEFAULT 0;
  v_event_id STRING;
  c_ddl CURSOR FOR
    SELECT event_id
      FROM TICDC_META.DDL_EVENT_QUEUE
     WHERE integration_id = ?
       AND commit_ts <= ?
       AND apply_status = 'PENDING'
     ORDER BY commit_ts, seq, event_id;
BEGIN
  CREATE TABLE IF NOT EXISTS TICDC_META.PROCEDURE_ERROR_LOG (
    run_uuid STRING,
    procedure_name STRING,
    object_id STRING,
    upper_ts NUMBER(20, 0),
    sqlstate STRING,
    sqlcode NUMBER,
    error_message STRING,
    snowflake_query_id STRING,
    error_time TIMESTAMP_NTZ(6)
  );

  OPEN c_ddl USING (p_integration_id, p_upper_ts);
  FOR rec IN c_ddl DO
    v_event_id := rec.event_id;
    CALL TICDC_META.SP_APPLY_ONE_DDL(:p_integration_id, :v_event_id);
    v_applied := v_applied + 1;
  END FOR;
  CLOSE c_ddl;

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_APPLY_DDL_UP_TO',
    'integration_id',
    p_integration_id,
    'upper_ts',
    p_upper_ts,
    'applied',
    v_applied
  );
EXCEPTION
  WHEN OTHER THEN
    INSERT INTO TICDC_META.PROCEDURE_ERROR_LOG(
      run_uuid,
      procedure_name,
      object_id,
      upper_ts,
      sqlstate,
      sqlcode,
      error_message,
      snowflake_query_id,
      error_time
    )
    SELECT NULL, 'SP_APPLY_DDL_UP_TO', :p_integration_id, :p_upper_ts, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
