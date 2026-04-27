CREATE OR REPLACE PROCEDURE TICDC_META.SP_DISCOVER_CONTROL_FILES(p_integration_id STRING)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_control_prefix STRING DEFAULT NULL;
  v_discovered NUMBER(20, 0) DEFAULT 0;
BEGIN
  CREATE TABLE IF NOT EXISTS TICDC_META.INTEGRATION_REGISTRY (
    integration_id STRING,
    control_prefix STRING,
    raw_database STRING,
    raw_schema STRING,
    replica_database STRING,
    replica_schema STRING,
    external_volume STRING,
    catalog_integration STRING,
    is_enabled BOOLEAN,
    created_at TIMESTAMP_NTZ(6),
    updated_at TIMESTAMP_NTZ(6)
  );

  CREATE TABLE IF NOT EXISTS TICDC_META.OBJECT_REGISTRY (
    integration_id STRING,
    object_id STRING,
    source_db STRING,
    source_table STRING,
    target_database STRING,
    target_schema STRING,
    target_table STRING,
    target_base_table STRING,
    snapshot_external_table STRING,
    change_external_table STRING,
    snapshot_metadata_file_path STRING,
    change_metadata_file_path STRING,
    snapshot_base_location STRING,
    change_base_location STRING,
    table_id NUMBER(20, 0),
    table_version NUMBER(20, 0),
    bootstrap_ts NUMBER(20, 0),
    generation STRING,
    materialization_status STRING,
    is_enabled BOOLEAN,
    created_at TIMESTAMP_NTZ(6),
    updated_at TIMESTAMP_NTZ(6)
  );

  CREATE TABLE IF NOT EXISTS TICDC_META.COLUMN_REGISTRY (
    integration_id STRING,
    object_id STRING,
    column_name STRING,
    target_column_name STRING,
    snowflake_type STRING,
    ordinal_position NUMBER(20, 0),
    is_nullable BOOLEAN,
    is_primary_key BOOLEAN,
    is_handle_key BOOLEAN,
    is_deleted BOOLEAN,
    updated_at TIMESTAMP_NTZ(6)
  );

  CREATE TABLE IF NOT EXISTS TICDC_META.INDEX_REGISTRY (
    integration_id STRING,
    object_id STRING,
    index_name STRING,
    index_type STRING,
    columns VARIANT,
    is_unique BOOLEAN,
    is_primary BOOLEAN,
    updated_at TIMESTAMP_NTZ(6)
  );

  CREATE TABLE IF NOT EXISTS TICDC_META.CONTROL_FILE_REGISTRY (
    integration_id STRING,
    file_path STRING,
    file_type STRING,
    file_date DATE,
    commit_ts NUMBER(20, 0),
    seq NUMBER(20, 0),
    discovered_at TIMESTAMP_NTZ(6),
    last_seen_at TIMESTAMP_NTZ(6),
    loaded_at TIMESTAMP_NTZ(6),
    load_status STRING,
    last_error STRING
  );

  CREATE TABLE IF NOT EXISTS TICDC_META.DDL_EVENT_RAW (
    integration_id STRING,
    event_id STRING,
    file_path STRING,
    payload VARIANT,
    loaded_at TIMESTAMP_NTZ(6)
  );

  CREATE TABLE IF NOT EXISTS TICDC_META.DDL_EVENT_QUEUE (
    integration_id STRING,
    event_id STRING,
    ddl_group_id STRING,
    object_id STRING,
    commit_ts NUMBER(20, 0),
    seq NUMBER(20, 0),
    source_db STRING,
    source_table STRING,
    ddl_type STRING,
    ddl_apply_class STRING,
    need_rebuild BOOLEAN,
    rebuild_from_ts NUMBER(20, 0),
    table_id_after NUMBER(20, 0),
    table_version_after NUMBER(20, 0),
    ddl_query STRING,
    payload VARIANT,
    file_path STRING,
    loaded_at TIMESTAMP_NTZ(6),
    applied_at TIMESTAMP_NTZ(6),
    apply_status STRING,
    last_error STRING
  );

  CREATE TABLE IF NOT EXISTS TICDC_META.GLOBAL_CHECKPOINT_STATE (
    integration_id STRING,
    changefeed_id STRING,
    keyspace STRING,
    changefeed_gid STRING,
    resolved_ts NUMBER(20, 0),
    source_file STRING,
    updated_at TIMESTAMP_NTZ(6)
  );

  CREATE TABLE IF NOT EXISTS TICDC_META.TABLE_SYNC_STATE (
    integration_id STRING,
    object_id STRING,
    generation STRING,
    bootstrap_ts NUMBER(20, 0),
    table_version NUMBER(20, 0),
    last_applied_commit_ts NUMBER(20, 0),
    last_apply_time TIMESTAMP_NTZ(6),
    status STRING,
    updated_at TIMESTAMP_NTZ(6)
  );

  CREATE TABLE IF NOT EXISTS TICDC_META.REBUILD_QUEUE (
    integration_id STRING,
    object_id STRING,
    event_id STRING,
    rebuild_from_ts NUMBER(20, 0),
    reason STRING,
    status STRING,
    attempts NUMBER(20, 0),
    created_at TIMESTAMP_NTZ(6),
    updated_at TIMESTAMP_NTZ(6),
    last_error STRING
  );

  CREATE TABLE IF NOT EXISTS TICDC_META.TASK_RUN_LOG (
    integration_id STRING,
    run_uuid STRING,
    task_name STRING,
    status STRING,
    upper_ts NUMBER(20, 0),
    started_at TIMESTAMP_NTZ(6),
    ended_at TIMESTAMP_NTZ(6),
    apply_watermark_before NUMBER(20, 0),
    apply_watermark_after NUMBER(20, 0),
    ddl_pending_count NUMBER(20, 0),
    rebuild_queue_size NUMBER(20, 0),
    error_message STRING
  );

  CREATE TABLE IF NOT EXISTS TICDC_META.DDL_APPLY_LOG (
    integration_id STRING,
    event_id STRING,
    object_id STRING,
    commit_ts NUMBER(20, 0),
    ddl_type STRING,
    apply_status STRING,
    ddl_query STRING,
    error_message STRING,
    applied_at TIMESTAMP_NTZ(6)
  );

  CREATE TABLE IF NOT EXISTS TICDC_META.RESYNC_LOG (
    integration_id STRING,
    resync_id STRING,
    generation STRING,
    bootstrap_ts NUMBER(20, 0),
    status STRING,
    created_at TIMESTAMP_NTZ(6),
    updated_at TIMESTAMP_NTZ(6)
  );

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

  SELECT MAX(control_prefix)
    INTO :v_control_prefix
    FROM TICDC_META.INTEGRATION_REGISTRY
   WHERE integration_id = :p_integration_id
     AND COALESCE(is_enabled, TRUE);

  IF (v_control_prefix IS NULL OR v_control_prefix = '') THEN
    RETURN OBJECT_CONSTRUCT(
      'status',
      'skipped',
      'procedure',
      'SP_DISCOVER_CONTROL_FILES',
      'integration_id',
      p_integration_id,
      'reason',
      'integration is not registered or has an empty control prefix'
    );
  END IF;

  MERGE INTO TICDC_META.CONTROL_FILE_REGISTRY t
  USING (
    SELECT
      :p_integration_id AS integration_id,
      RELATIVE_PATH AS file_path,
      CASE
        WHEN REGEXP_LIKE(RELATIVE_PATH, '.*/control/ddl/.*\\.json$', 'i') THEN 'ddl'
        WHEN REGEXP_LIKE(RELATIVE_PATH, '.*/control/schema/.*\\.json$', 'i') THEN 'schema'
        WHEN REGEXP_LIKE(RELATIVE_PATH, '.*/control/checkpoint/global/.*\\.json$', 'i') THEN 'checkpoint'
        WHEN REGEXP_LIKE(RELATIVE_PATH, '.*/control/checkpoint/table/.*\\.json$', 'i') THEN 'table_checkpoint'
        WHEN REGEXP_LIKE(RELATIVE_PATH, '.*/control/resync/.*\\.json$', 'i') THEN 'resync'
        ELSE 'other'
      END AS file_type,
      TO_DATE(REGEXP_SUBSTR(RELATIVE_PATH, 'date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1, 1, 'e', 1)) AS file_date,
      TRY_TO_NUMBER(REGEXP_SUBSTR(RELATIVE_PATH, 'commit_ts=([0-9]+)', 1, 1, 'e', 1)) AS commit_ts,
      TRY_TO_NUMBER(REGEXP_SUBSTR(RELATIVE_PATH, 'seq=([0-9]+)', 1, 1, 'e', 1)) AS seq
    FROM DIRECTORY(@TICDC_META.CTL_STAGE)
    WHERE STARTSWITH(RELATIVE_PATH, :v_control_prefix)
      AND REGEXP_LIKE(RELATIVE_PATH, '.*/control/(ddl|schema|checkpoint|resync)/.*\\.json$', 'i')
  ) s
  ON t.integration_id = s.integration_id AND t.file_path = s.file_path
  WHEN MATCHED THEN
    UPDATE SET
      file_type = s.file_type,
      file_date = s.file_date,
      commit_ts = s.commit_ts,
      seq = s.seq,
      last_seen_at = CURRENT_TIMESTAMP(),
      load_status = COALESCE(t.load_status, 'DISCOVERED')
  WHEN NOT MATCHED THEN
    INSERT (
      integration_id,
      file_path,
      file_type,
      file_date,
      commit_ts,
      seq,
      discovered_at,
      last_seen_at,
      load_status
    )
    VALUES (
      s.integration_id,
      s.file_path,
      s.file_type,
      s.file_date,
      s.commit_ts,
      s.seq,
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP(),
      'DISCOVERED'
    );

  SELECT COUNT(*)
    INTO :v_discovered
    FROM TICDC_META.CONTROL_FILE_REGISTRY
   WHERE integration_id = :p_integration_id
     AND load_status = 'DISCOVERED';

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_DISCOVER_CONTROL_FILES',
    'integration_id',
    p_integration_id,
    'control_prefix',
    v_control_prefix,
    'discovered',
    v_discovered
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
    SELECT NULL, 'SP_DISCOVER_CONTROL_FILES', :p_integration_id, NULL, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
