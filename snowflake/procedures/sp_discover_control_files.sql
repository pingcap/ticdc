CREATE OR REPLACE PROCEDURE TICDC_META.SP_DISCOVER_CONTROL_FILES()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_discovered NUMBER(20, 0) DEFAULT 0;
BEGIN
  -- TODO: discover new control manifests from stage directory table.
  RETURN OBJECT_CONSTRUCT('status', 'ok', 'procedure', 'SP_DISCOVER_CONTROL_FILES', 'discovered', v_discovered);
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
    SELECT NULL, 'SP_DISCOVER_CONTROL_FILES', NULL, NULL, SQLSTATE, SQLCODE, SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
