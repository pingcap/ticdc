CREATE OR REPLACE PROCEDURE TICDC_META.SP_REBUILD_ONE_TABLE(
  p_integration_id STRING,
  p_object_id STRING
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_event_id STRING DEFAULT NULL;
  v_generation STRING DEFAULT NULL;
  v_bootstrap_ts NUMBER(20, 0) DEFAULT 0;
  v_upper_ts NUMBER(20, 0) DEFAULT 0;
  v_rebuild_from_ts NUMBER(20, 0) DEFAULT 0;
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

  SELECT
    MAX(event_id),
    COALESCE(MIN(rebuild_from_ts), 0)
    INTO :v_event_id, :v_rebuild_from_ts
    FROM TICDC_META.REBUILD_QUEUE
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id
     AND status IN ('PENDING', 'RUNNING');

  SELECT
    COALESCE(MAX(generation), (SELECT MAX(generation) FROM TICDC_META.OBJECT_REGISTRY WHERE integration_id = :p_integration_id AND object_id = :p_object_id)),
    COALESCE(MAX(bootstrap_ts), (SELECT MAX(bootstrap_ts) FROM TICDC_META.OBJECT_REGISTRY WHERE integration_id = :p_integration_id AND object_id = :p_object_id), 0)
    INTO :v_generation, :v_bootstrap_ts
    FROM TICDC_META.TABLE_SYNC_STATE
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id;

  SELECT COALESCE(MAX(resolved_ts), :v_bootstrap_ts)
    INTO :v_upper_ts
    FROM TICDC_META.GLOBAL_CHECKPOINT_STATE
   WHERE integration_id = :p_integration_id;

  IF (v_generation IS NULL OR v_bootstrap_ts = 0) THEN
    UPDATE TICDC_META.REBUILD_QUEUE
       SET status = 'FAILED',
           last_error = 'missing bootstrap generation or bootstrap ts',
           updated_at = CURRENT_TIMESTAMP()
     WHERE integration_id = :p_integration_id
       AND object_id = :p_object_id
       AND status IN ('PENDING', 'RUNNING');

    RETURN OBJECT_CONSTRUCT(
      'status',
      'skipped',
      'procedure',
      'SP_REBUILD_ONE_TABLE',
      'integration_id',
      p_integration_id,
      'object_id',
      p_object_id,
      'reason',
      'missing bootstrap generation or bootstrap ts'
    );
  END IF;

  UPDATE TICDC_META.OBJECT_REGISTRY
     SET materialization_status = 'REBUILDING',
         updated_at = CURRENT_TIMESTAMP()
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id
     AND COALESCE(materialization_status, 'PAUSED_FOR_REBUILD') IN ('PAUSED_FOR_REBUILD', 'REBUILDING', 'ACTIVE');

  UPDATE TICDC_META.REBUILD_QUEUE
     SET status = 'RUNNING',
         attempts = COALESCE(attempts, 0) + 1,
         updated_at = CURRENT_TIMESTAMP(),
         last_error = NULL
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id
     AND status IN ('PENDING', 'RUNNING');

  CALL TICDC_META.SP_BOOTSTRAP_ONE_TABLE(:p_integration_id, :p_object_id, :v_generation, :v_bootstrap_ts);
  CALL TICDC_META.SP_SYNC_ONE_TABLE(:p_integration_id, :p_object_id, :v_upper_ts);

  UPDATE TICDC_META.REBUILD_QUEUE
     SET status = 'DONE',
         updated_at = CURRENT_TIMESTAMP(),
         last_error = NULL
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id
     AND status = 'RUNNING';

  UPDATE TICDC_META.DDL_EVENT_QUEUE
     SET apply_status = 'APPLIED',
         applied_at = CURRENT_TIMESTAMP(),
         last_error = NULL
   WHERE integration_id = :p_integration_id
     AND event_id = :v_event_id
     AND apply_status = 'REBUILD_REQUIRED';

  UPDATE TICDC_META.OBJECT_REGISTRY
     SET materialization_status = 'ACTIVE',
         updated_at = CURRENT_TIMESTAMP()
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id;

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_REBUILD_ONE_TABLE',
    'integration_id',
    p_integration_id,
    'object_id',
    p_object_id,
    'event_id',
    v_event_id,
    'rebuild_from_ts',
    v_rebuild_from_ts,
    'upper_ts',
    v_upper_ts
  );
EXCEPTION
  WHEN OTHER THEN
    UPDATE TICDC_META.REBUILD_QUEUE
       SET status = 'FAILED',
           last_error = :SQLERRM,
           updated_at = CURRENT_TIMESTAMP()
     WHERE integration_id = :p_integration_id
       AND object_id = :p_object_id
       AND status = 'RUNNING';

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
    SELECT NULL, 'SP_REBUILD_ONE_TABLE', :p_object_id || '@' || :p_integration_id, :v_upper_ts, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
