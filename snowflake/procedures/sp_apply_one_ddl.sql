CREATE OR REPLACE PROCEDURE TICDC_META.SP_APPLY_ONE_DDL(p_event_id STRING)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  -- TODO: fetch DDL manifest by event id and apply direct replay or enqueue rebuild.
  RETURN OBJECT_CONSTRUCT('status', 'ok', 'procedure', 'SP_APPLY_ONE_DDL', 'event_id', p_event_id);
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
    SELECT NULL, 'SP_APPLY_ONE_DDL', p_event_id, NULL, SQLSTATE, SQLCODE, SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
