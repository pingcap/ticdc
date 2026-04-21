CREATE OR REPLACE PROCEDURE TICDC_META.SP_SYNC_ALL_TABLES(p_upper_ts NUMBER(20, 0))
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_tables_synced NUMBER(20, 0) DEFAULT 0;
BEGIN
  -- TODO: iterate enabled objects and call SP_SYNC_ONE_TABLE for each object.
  RETURN OBJECT_CONSTRUCT('status', 'ok', 'procedure', 'SP_SYNC_ALL_TABLES', 'upper_ts', p_upper_ts, 'tables_synced', v_tables_synced);
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
    SELECT NULL, 'SP_SYNC_ALL_TABLES', NULL, p_upper_ts, SQLSTATE, SQLCODE, SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
