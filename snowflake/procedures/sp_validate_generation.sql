CREATE OR REPLACE PROCEDURE TICDC_META.SP_VALIDATE_GENERATION(
  p_integration_id STRING,
  p_generation STRING,
  p_upper_ts NUMBER(20, 0)
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_object_count NUMBER(20, 0) DEFAULT 0;
  v_ready_count NUMBER(20, 0) DEFAULT 0;
  v_min_watermark NUMBER(20, 0) DEFAULT 0;
BEGIN
  CREATE TABLE IF NOT EXISTS TICDC_META.PROCEDURE_ERROR_LOG (
    run_uuid STRING,
    procedure_name STRING,
    object_id STRING,
    upper_ts NUMBER(20, 0),
    sqlstate STRING,
    sqlcode NUMBER,
    error_message STRING,
    snowflake_query_id STRING,
    error_time TIMESTAMP_NTZ(6)
  );

  SELECT COUNT(*)
    INTO :v_object_count
    FROM TICDC_META.OBJECT_REGISTRY
   WHERE integration_id = :p_integration_id
     AND generation = :p_generation
     AND COALESCE(is_enabled, TRUE);

  SELECT COUNT(*), COALESCE(MIN(last_applied_commit_ts), 0)
    INTO :v_ready_count, :v_min_watermark
    FROM TICDC_META.TABLE_SYNC_STATE
   WHERE integration_id = :p_integration_id
     AND generation = :p_generation
     AND status IN ('FULL_LOADED', 'INCREMENTAL_LOADED', 'DDL_APPLIED');

  RETURN OBJECT_CONSTRUCT(
    'status',
    IFF(v_object_count > 0 AND v_object_count = v_ready_count AND v_min_watermark >= p_upper_ts, 'ok', 'pending'),
    'procedure',
    'SP_VALIDATE_GENERATION',
    'integration_id',
    p_integration_id,
    'generation',
    p_generation,
    'upper_ts',
    p_upper_ts,
    'object_count',
    v_object_count,
    'ready_count',
    v_ready_count,
    'min_watermark',
    v_min_watermark
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
    SELECT NULL, 'SP_VALIDATE_GENERATION', :p_integration_id || '@' || :p_generation, :p_upper_ts, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
