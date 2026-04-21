CREATE OR REPLACE PROCEDURE TICDC_META.SP_ORCHESTRATE()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_run_uuid STRING DEFAULT UUID_STRING();
  v_upper_ts NUMBER(20, 0) DEFAULT 0;
BEGIN
  CALL TICDC_META.SP_DISCOVER_CONTROL_FILES();
  CALL TICDC_META.SP_LOAD_DDL_MANIFESTS();

  BEGIN
    SELECT COALESCE(MAX(resolved_ts), 0)
      INTO :v_upper_ts
      FROM TICDC_META.GLOBAL_CHECKPOINT_STATE;
  EXCEPTION
    WHEN OTHER THEN
      v_upper_ts := 0;
  END;

  CALL TICDC_META.SP_APPLY_DDL_UP_TO(v_upper_ts);
  CALL TICDC_META.SP_SYNC_ALL_TABLES(v_upper_ts);
  CALL TICDC_META.SP_PROCESS_REBUILD_QUEUE();

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_ORCHESTRATE',
    'run_uuid',
    v_run_uuid,
    'upper_ts',
    v_upper_ts
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
    SELECT v_run_uuid, 'SP_ORCHESTRATE', NULL, v_upper_ts, SQLSTATE, SQLCODE, SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
