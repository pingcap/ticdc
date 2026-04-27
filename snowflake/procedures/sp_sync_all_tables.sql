CREATE OR REPLACE PROCEDURE TICDC_META.SP_SYNC_ALL_TABLES(
  p_integration_id STRING,
  p_upper_ts NUMBER(20, 0)
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_tables_synced NUMBER(20, 0) DEFAULT 0;
  v_object_id STRING;
  c_tables CURSOR FOR
    SELECT object_id
      FROM TICDC_META.OBJECT_REGISTRY
     WHERE integration_id = ?
       AND COALESCE(is_enabled, TRUE)
       AND COALESCE(materialization_status, 'ACTIVE') NOT IN ('PAUSED_FOR_REBUILD', 'REBUILDING')
     ORDER BY source_db, source_table, object_id;
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

  OPEN c_tables USING (p_integration_id);
  FOR rec IN c_tables DO
    v_object_id := rec.object_id;
    CALL TICDC_META.SP_SYNC_ONE_TABLE(:p_integration_id, :v_object_id, :p_upper_ts);
    v_tables_synced := v_tables_synced + 1;
  END FOR;
  CLOSE c_tables;

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_SYNC_ALL_TABLES',
    'integration_id',
    p_integration_id,
    'upper_ts',
    p_upper_ts,
    'tables_synced',
    v_tables_synced
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
    SELECT NULL, 'SP_SYNC_ALL_TABLES', :p_integration_id, :p_upper_ts, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
