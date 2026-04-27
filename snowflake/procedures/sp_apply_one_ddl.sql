CREATE OR REPLACE PROCEDURE TICDC_META.SP_APPLY_ONE_DDL(
  p_integration_id STRING,
  p_event_id STRING
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_exists NUMBER(20, 0) DEFAULT 0;
  v_object_id STRING DEFAULT NULL;
  v_commit_ts NUMBER(20, 0) DEFAULT NULL;
  v_ddl_type STRING DEFAULT NULL;
  v_apply_class STRING DEFAULT NULL;
  v_need_rebuild BOOLEAN DEFAULT FALSE;
  v_rebuild_from_ts NUMBER(20, 0) DEFAULT NULL;
  v_ddl_sql STRING DEFAULT NULL;
  v_table_version_after NUMBER(20, 0) DEFAULT 0;
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

  SELECT
    COUNT(*),
    MAX(object_id),
    MAX(commit_ts),
    MAX(ddl_type),
    MAX(ddl_apply_class),
    BOOLOR_AGG(COALESCE(need_rebuild, FALSE)),
    MAX(rebuild_from_ts),
    MAX(ddl_query),
    COALESCE(MAX(table_version_after), 0)
    INTO
      :v_exists,
      :v_object_id,
      :v_commit_ts,
      :v_ddl_type,
      :v_apply_class,
      :v_need_rebuild,
      :v_rebuild_from_ts,
      :v_ddl_sql,
      :v_table_version_after
    FROM TICDC_META.DDL_EVENT_QUEUE
   WHERE integration_id = :p_integration_id
     AND event_id = :p_event_id;

  IF (v_exists = 0) THEN
    RETURN OBJECT_CONSTRUCT(
      'status',
      'skipped',
      'procedure',
      'SP_APPLY_ONE_DDL',
      'integration_id',
      p_integration_id,
      'event_id',
      p_event_id,
      'reason',
      'event is not found'
    );
  END IF;

  IF (v_need_rebuild OR LOWER(COALESCE(v_apply_class, '')) = 'rebuild_required') THEN
    MERGE INTO TICDC_META.REBUILD_QUEUE t
    USING (
      SELECT
        :p_integration_id AS integration_id,
        :v_object_id AS object_id,
        :p_event_id AS event_id,
        COALESCE(:v_rebuild_from_ts, :v_commit_ts) AS rebuild_from_ts,
        COALESCE(:v_ddl_type, 'ddl') AS reason
    ) s
    ON t.integration_id = s.integration_id AND t.object_id = s.object_id AND t.event_id = s.event_id
    WHEN MATCHED THEN
      UPDATE SET
        rebuild_from_ts = s.rebuild_from_ts,
        reason = s.reason,
        status = IFF(t.status = 'DONE', 'DONE', 'PENDING'),
        updated_at = CURRENT_TIMESTAMP(),
        last_error = NULL
    WHEN NOT MATCHED THEN
      INSERT (
        integration_id,
        object_id,
        event_id,
        rebuild_from_ts,
        reason,
        status,
        attempts,
        created_at,
        updated_at
      )
      VALUES (
        s.integration_id,
        s.object_id,
        s.event_id,
        s.rebuild_from_ts,
        s.reason,
        'PENDING',
        0,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
      );

    UPDATE TICDC_META.OBJECT_REGISTRY
       SET materialization_status = 'PAUSED_FOR_REBUILD',
           updated_at = CURRENT_TIMESTAMP()
     WHERE integration_id = :p_integration_id
       AND object_id = :v_object_id;

    UPDATE TICDC_META.DDL_EVENT_QUEUE
       SET apply_status = 'REBUILD_REQUIRED',
           applied_at = CURRENT_TIMESTAMP(),
           last_error = NULL
     WHERE integration_id = :p_integration_id
       AND event_id = :p_event_id;

    INSERT INTO TICDC_META.DDL_APPLY_LOG(
      integration_id,
      event_id,
      object_id,
      commit_ts,
      ddl_type,
      apply_status,
      ddl_query,
      error_message,
      applied_at
    )
    SELECT :p_integration_id, :p_event_id, :v_object_id, :v_commit_ts, :v_ddl_type, 'REBUILD_REQUIRED', :v_ddl_sql, NULL, CURRENT_TIMESTAMP();

    RETURN OBJECT_CONSTRUCT(
      'status',
      'rebuild_required',
      'procedure',
      'SP_APPLY_ONE_DDL',
      'integration_id',
      p_integration_id,
      'event_id',
      p_event_id,
      'object_id',
      v_object_id
    );
  END IF;

  IF (LOWER(COALESCE(v_ddl_type, '')) = 'create table') THEN
    CALL TICDC_META.SP_ENSURE_RAW_CHANGE_TABLE(:p_integration_id, :v_object_id);
    CALL TICDC_META.SP_ENSURE_TARGET_TABLE(:p_integration_id, :v_object_id);

    UPDATE TICDC_META.OBJECT_REGISTRY
       SET bootstrap_ts = COALESCE(bootstrap_ts, :v_commit_ts),
           table_version = COALESCE(table_version, :v_table_version_after),
           materialization_status = 'ACTIVE',
           is_enabled = TRUE,
           updated_at = CURRENT_TIMESTAMP()
     WHERE integration_id = :p_integration_id
       AND object_id = :v_object_id;

    MERGE INTO TICDC_META.TABLE_SYNC_STATE t
    USING (
      SELECT
        :p_integration_id AS integration_id,
        :v_object_id AS object_id,
        MAX(generation) AS generation,
        COALESCE(MAX(bootstrap_ts), :v_commit_ts) AS bootstrap_ts,
        COALESCE(MAX(table_version), :v_table_version_after) AS table_version,
        :v_commit_ts AS last_applied_commit_ts
      FROM TICDC_META.OBJECT_REGISTRY
      WHERE integration_id = :p_integration_id
        AND object_id = :v_object_id
    ) s
    ON t.integration_id = s.integration_id AND t.object_id = s.object_id
    WHEN MATCHED THEN
      UPDATE SET
        generation = COALESCE(s.generation, t.generation),
        bootstrap_ts = COALESCE(s.bootstrap_ts, t.bootstrap_ts),
        table_version = GREATEST(COALESCE(t.table_version, 0), COALESCE(s.table_version, 0)),
        last_applied_commit_ts = GREATEST(COALESCE(t.last_applied_commit_ts, 0), s.last_applied_commit_ts),
        last_apply_time = CURRENT_TIMESTAMP(),
        status = 'DDL_APPLIED',
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
      INSERT (
        integration_id,
        object_id,
        generation,
        bootstrap_ts,
        table_version,
        last_applied_commit_ts,
        last_apply_time,
        status,
        updated_at
      )
      VALUES (
        s.integration_id,
        s.object_id,
        s.generation,
        s.bootstrap_ts,
        s.table_version,
        s.last_applied_commit_ts,
        CURRENT_TIMESTAMP(),
        'DDL_APPLIED',
        CURRENT_TIMESTAMP()
      );
  ELSEIF (LOWER(COALESCE(v_apply_class, '')) = 'direct_replay' AND v_ddl_sql IS NOT NULL AND v_ddl_sql <> '') THEN
    EXECUTE IMMEDIATE v_ddl_sql;
  END IF;

  UPDATE TICDC_META.DDL_EVENT_QUEUE
     SET apply_status = 'APPLIED',
         applied_at = CURRENT_TIMESTAMP(),
         last_error = NULL
   WHERE integration_id = :p_integration_id
     AND event_id = :p_event_id;

  INSERT INTO TICDC_META.DDL_APPLY_LOG(
    integration_id,
    event_id,
    object_id,
    commit_ts,
    ddl_type,
    apply_status,
    ddl_query,
    error_message,
    applied_at
  )
  SELECT :p_integration_id, :p_event_id, :v_object_id, :v_commit_ts, :v_ddl_type, 'APPLIED', :v_ddl_sql, NULL, CURRENT_TIMESTAMP();

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_APPLY_ONE_DDL',
    'integration_id',
    p_integration_id,
    'event_id',
    p_event_id,
    'object_id',
    v_object_id,
    'apply_class',
    v_apply_class
  );
EXCEPTION
  WHEN OTHER THEN
    UPDATE TICDC_META.DDL_EVENT_QUEUE
       SET apply_status = 'FAILED',
           last_error = :SQLERRM
     WHERE integration_id = :p_integration_id
       AND event_id = :p_event_id;

    INSERT INTO TICDC_META.DDL_APPLY_LOG(
      integration_id,
      event_id,
      object_id,
      commit_ts,
      ddl_type,
      apply_status,
      ddl_query,
      error_message,
      applied_at
    )
    SELECT :p_integration_id, :p_event_id, :v_object_id, :v_commit_ts, :v_ddl_type, 'FAILED', :v_ddl_sql, :SQLERRM, CURRENT_TIMESTAMP();

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
    SELECT NULL, 'SP_APPLY_ONE_DDL', :p_event_id || '@' || :p_integration_id, NULL, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
