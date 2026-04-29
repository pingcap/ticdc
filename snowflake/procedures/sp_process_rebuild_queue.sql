CREATE OR REPLACE PROCEDURE TICDC_META.SP_PROCESS_REBUILD_QUEUE(p_integration_id STRING)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_rebuilt NUMBER(20, 0) DEFAULT 0;
  v_object_id STRING;
  c_rebuild CURSOR FOR
    SELECT object_id
      FROM TICDC_META.REBUILD_QUEUE
     WHERE integration_id = ?
       AND status = 'PENDING'
     GROUP BY object_id
     ORDER BY MIN(rebuild_from_ts), object_id;
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

  OPEN c_rebuild USING (p_integration_id);
  FOR rec IN c_rebuild DO
    v_object_id := rec.object_id;
    CALL TICDC_META.SP_REBUILD_ONE_TABLE(:p_integration_id, :v_object_id);
    v_rebuilt := v_rebuilt + 1;
  END FOR;
  CLOSE c_rebuild;

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_PROCESS_REBUILD_QUEUE',
    'integration_id',
    p_integration_id,
    'rebuilt',
    v_rebuilt
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
    SELECT NULL, 'SP_PROCESS_REBUILD_QUEUE', :p_integration_id, NULL, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
