CREATE OR REPLACE PROCEDURE TICDC_META.SP_ORCHESTRATE(p_integration_id STRING)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_run_uuid STRING DEFAULT UUID_STRING();
  v_upper_ts NUMBER(20, 0) DEFAULT 0;
  v_watermark_before NUMBER(20, 0) DEFAULT 0;
  v_watermark_after NUMBER(20, 0) DEFAULT 0;
  v_ddl_pending_count NUMBER(20, 0) DEFAULT 0;
  v_rebuild_queue_size NUMBER(20, 0) DEFAULT 0;
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

  CALL TICDC_META.SP_DISCOVER_CONTROL_FILES(:p_integration_id);
  CALL TICDC_META.SP_LOAD_DDL_MANIFESTS(:p_integration_id);

  SELECT COALESCE(MAX(last_applied_commit_ts), 0)
    INTO :v_watermark_before
    FROM TICDC_META.TABLE_SYNC_STATE
   WHERE integration_id = :p_integration_id;

  SELECT COALESCE(MAX(resolved_ts), 0)
    INTO :v_upper_ts
    FROM TICDC_META.GLOBAL_CHECKPOINT_STATE
   WHERE integration_id = :p_integration_id;

  CALL TICDC_META.SP_APPLY_DDL_UP_TO(:p_integration_id, :v_upper_ts);
  CALL TICDC_META.SP_SYNC_ALL_TABLES(:p_integration_id, :v_upper_ts);
  CALL TICDC_META.SP_PROCESS_REBUILD_QUEUE(:p_integration_id);

  SELECT COALESCE(MAX(last_applied_commit_ts), 0)
    INTO :v_watermark_after
    FROM TICDC_META.TABLE_SYNC_STATE
   WHERE integration_id = :p_integration_id;

  SELECT COUNT(*)
    INTO :v_ddl_pending_count
    FROM TICDC_META.DDL_EVENT_QUEUE
   WHERE integration_id = :p_integration_id
     AND apply_status = 'PENDING';

  SELECT COUNT(*)
    INTO :v_rebuild_queue_size
    FROM TICDC_META.REBUILD_QUEUE
   WHERE integration_id = :p_integration_id
     AND status IN ('PENDING', 'RUNNING');

  INSERT INTO TICDC_META.TASK_RUN_LOG(
    integration_id,
    run_uuid,
    task_name,
    status,
    upper_ts,
    started_at,
    ended_at,
    apply_watermark_before,
    apply_watermark_after,
    ddl_pending_count,
    rebuild_queue_size,
    error_message
  )
  SELECT
    :p_integration_id,
    :v_run_uuid,
    'SP_ORCHESTRATE',
    'SUCCEEDED',
    :v_upper_ts,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    :v_watermark_before,
    :v_watermark_after,
    :v_ddl_pending_count,
    :v_rebuild_queue_size,
    NULL;

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_ORCHESTRATE',
    'integration_id',
    p_integration_id,
    'run_uuid',
    v_run_uuid,
    'upper_ts',
    v_upper_ts,
    'apply_watermark_before',
    v_watermark_before,
    'apply_watermark_after',
    v_watermark_after,
    'ddl_pending_count',
    v_ddl_pending_count,
    'rebuild_queue_size',
    v_rebuild_queue_size
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
    SELECT :v_run_uuid, 'SP_ORCHESTRATE', :p_integration_id, :v_upper_ts, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();

    INSERT INTO TICDC_META.TASK_RUN_LOG(
      integration_id,
      run_uuid,
      task_name,
      status,
      upper_ts,
      started_at,
      ended_at,
      apply_watermark_before,
      apply_watermark_after,
      ddl_pending_count,
      rebuild_queue_size,
      error_message
    )
    SELECT
      :p_integration_id,
      :v_run_uuid,
      'SP_ORCHESTRATE',
      'FAILED',
      :v_upper_ts,
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP(),
      :v_watermark_before,
      :v_watermark_after,
      :v_ddl_pending_count,
      :v_rebuild_queue_size,
      :SQLERRM;
    RAISE;
END;
$$;
