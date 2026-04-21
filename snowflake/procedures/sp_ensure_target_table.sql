CREATE OR REPLACE PROCEDURE TICDC_META.SP_ENSURE_TARGET_TABLE(p_object_id STRING)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  -- TODO: create or evolve internal target base table and serving view for p_object_id.
  RETURN OBJECT_CONSTRUCT('status', 'ok', 'procedure', 'SP_ENSURE_TARGET_TABLE', 'object_id', p_object_id);
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
    SELECT NULL, 'SP_ENSURE_TARGET_TABLE', p_object_id, NULL, SQLSTATE, SQLCODE, SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
