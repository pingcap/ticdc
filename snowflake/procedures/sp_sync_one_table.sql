CREATE OR REPLACE PROCEDURE TICDC_META.SP_SYNC_ONE_TABLE(
  p_object_id STRING,
  p_upper_ts NUMBER(20, 0)
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  CALL TICDC_META.SP_ENSURE_RAW_CHANGE_TABLE(p_object_id);
  CALL TICDC_META.SP_ENSURE_TARGET_TABLE(p_object_id);

  -- TODO: apply delete pass then upsert pass in (last_watermark, p_upper_ts].
  RETURN OBJECT_CONSTRUCT('status', 'ok', 'procedure', 'SP_SYNC_ONE_TABLE', 'object_id', p_object_id, 'upper_ts', p_upper_ts);
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
    SELECT NULL, 'SP_SYNC_ONE_TABLE', p_object_id, p_upper_ts, SQLSTATE, SQLCODE, SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
