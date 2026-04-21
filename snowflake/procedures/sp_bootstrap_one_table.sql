CREATE OR REPLACE PROCEDURE TICDC_META.SP_BOOTSTRAP_ONE_TABLE(
  p_object_id STRING,
  p_generation STRING,
  p_bootstrap_ts NUMBER(20, 0)
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  CALL TICDC_META.SP_ENSURE_TARGET_TABLE(p_object_id);

  -- TODO: INSERT INTO target table FROM snapshot external table for p_generation.
  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_BOOTSTRAP_ONE_TABLE',
    'object_id',
    p_object_id,
    'generation',
    p_generation,
    'bootstrap_ts',
    p_bootstrap_ts
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
    SELECT NULL, 'SP_BOOTSTRAP_ONE_TABLE', p_object_id, p_bootstrap_ts, SQLSTATE, SQLCODE, SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
