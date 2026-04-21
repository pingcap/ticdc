CREATE OR REPLACE PROCEDURE TICDC_META.SP_LOAD_DDL_MANIFESTS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_loaded NUMBER(20, 0) DEFAULT 0;
BEGIN
  -- TODO: load discovered DDL manifests into DDL queue table.
  RETURN OBJECT_CONSTRUCT('status', 'ok', 'procedure', 'SP_LOAD_DDL_MANIFESTS', 'loaded', v_loaded);
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
    SELECT NULL, 'SP_LOAD_DDL_MANIFESTS', NULL, NULL, SQLSTATE, SQLCODE, SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
