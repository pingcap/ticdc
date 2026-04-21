CREATE OR REPLACE PROCEDURE TICDC_META.SP_BOOTSTRAP_ALL_TABLES(
  p_generation STRING,
  p_bootstrap_ts NUMBER(20, 0)
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_bootstrapped NUMBER(20, 0) DEFAULT 0;
BEGIN
  -- TODO: iterate table registry and call SP_BOOTSTRAP_ONE_TABLE for each enabled object.
  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_BOOTSTRAP_ALL_TABLES',
    'generation',
    p_generation,
    'bootstrap_ts',
    p_bootstrap_ts,
    'bootstrapped',
    v_bootstrapped
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
    SELECT NULL, 'SP_BOOTSTRAP_ALL_TABLES', NULL, p_bootstrap_ts, SQLSTATE, SQLCODE, SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
