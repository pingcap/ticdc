CREATE OR REPLACE PROCEDURE TICDC_META.SP_PROCESS_REBUILD_QUEUE()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_rebuilt NUMBER(20, 0) DEFAULT 0;
BEGIN
  -- TODO: consume rebuild queue and call SP_REBUILD_ONE_TABLE in priority order.
  RETURN OBJECT_CONSTRUCT('status', 'ok', 'procedure', 'SP_PROCESS_REBUILD_QUEUE', 'rebuilt', v_rebuilt);
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
    SELECT NULL, 'SP_PROCESS_REBUILD_QUEUE', NULL, NULL, SQLSTATE, SQLCODE, SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
