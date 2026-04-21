CREATE OR REPLACE PROCEDURE TICDC_META.SP_REGISTER_SNAPSHOT_TABLES(p_generation STRING)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_registered NUMBER(20, 0) DEFAULT 0;
BEGIN
  -- TODO: register external snapshot tables for the given generation.
  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_REGISTER_SNAPSHOT_TABLES',
    'generation',
    p_generation,
    'registered',
    v_registered
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
    SELECT NULL, 'SP_REGISTER_SNAPSHOT_TABLES', NULL, NULL, SQLSTATE, SQLCODE, SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
