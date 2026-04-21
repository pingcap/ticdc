CREATE OR REPLACE PROCEDURE TICDC_META.SP_APPLY_DDL_UP_TO(p_upper_ts NUMBER(20, 0))
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_applied NUMBER(20, 0) DEFAULT 0;
BEGIN
  -- TODO: load pending DDL queue events with commit_ts <= p_upper_ts and apply in order.
  RETURN OBJECT_CONSTRUCT('status', 'ok', 'procedure', 'SP_APPLY_DDL_UP_TO', 'upper_ts', p_upper_ts, 'applied', v_applied);
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
    SELECT NULL, 'SP_APPLY_DDL_UP_TO', NULL, p_upper_ts, SQLSTATE, SQLCODE, SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
