CREATE OR REPLACE PROCEDURE TICDC_META.SP_LOAD_DDL_MANIFESTS(p_integration_id STRING)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_control_prefix STRING DEFAULT NULL;
  v_loaded NUMBER(20, 0) DEFAULT 0;
  v_resolved_ts NUMBER(20, 0) DEFAULT 0;
BEGIN
  CREATE FILE FORMAT IF NOT EXISTS TICDC_META.FF_TICDC_MANIFEST_JSON
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE;

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
      'SP_LOAD_DDL_MANIFESTS',
      'integration_id',
      p_integration_id,
      'reason',
      'integration is not registered or has an empty control prefix'
    );
  END IF;

  MERGE INTO TICDC_META.DDL_EVENT_RAW t
  USING (
    SELECT
      :p_integration_id AS integration_id,
      $1:event_id::STRING AS event_id,
      METADATA$FILENAME::STRING AS file_path,
      $1 AS payload
    FROM @TICDC_META.CTL_STAGE
      (FILE_FORMAT => 'TICDC_META.FF_TICDC_MANIFEST_JSON', PATTERN => '.*\\/control\\/ddl\\/.*\\.json')
    WHERE $1:event_id IS NOT NULL
      AND STARTSWITH(METADATA$FILENAME, :v_control_prefix)
      AND REGEXP_LIKE(METADATA$FILENAME, '.*/control/ddl/.*\\.json$', 'i')
  ) s
  ON t.integration_id = s.integration_id AND t.event_id = s.event_id
  WHEN MATCHED THEN
    UPDATE SET
      file_path = s.file_path,
      payload = s.payload,
      loaded_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (integration_id, event_id, file_path, payload, loaded_at)
    VALUES (s.integration_id, s.event_id, s.file_path, s.payload, CURRENT_TIMESTAMP());

  MERGE INTO TICDC_META.DDL_EVENT_QUEUE t
  USING (
    SELECT *
    FROM (
      SELECT
        :p_integration_id AS integration_id,
        $1:event_id::STRING AS event_id,
        $1:ddl_group_id::STRING AS ddl_group_id,
        COALESCE($1:object_id::STRING, CONCAT($1:source_db::STRING, '.', $1:source_table::STRING), $1:table_id_after::STRING) AS object_id,
        $1:commit_ts::NUMBER(20, 0) AS commit_ts,
        COALESCE($1:seq::NUMBER(20, 0), 0) AS seq,
        $1:source_db::STRING AS source_db,
        $1:source_table::STRING AS source_table,
        $1:ddl_type::STRING AS ddl_type,
        COALESCE($1:ddl_apply_class::STRING, IFF(COALESCE($1:need_rebuild::BOOLEAN, FALSE), 'rebuild_required', 'direct_replay')) AS ddl_apply_class,
        COALESCE($1:need_rebuild::BOOLEAN, FALSE) AS need_rebuild,
        COALESCE($1:rebuild_from_ts::NUMBER(20, 0), $1:commit_ts::NUMBER(20, 0)) AS rebuild_from_ts,
        $1:table_id_after::NUMBER(20, 0) AS table_id_after,
        $1:table_version_after::NUMBER(20, 0) AS table_version_after,
        COALESCE($1:snowflake_query::STRING, $1:snowflake_ddl::STRING, $1:target_sql::STRING, $1:query::STRING) AS ddl_query,
        $1 AS payload,
        METADATA$FILENAME::STRING AS file_path,
        ROW_NUMBER() OVER (
          PARTITION BY :p_integration_id, $1:event_id::STRING
          ORDER BY $1:commit_ts::NUMBER(20, 0) DESC, COALESCE($1:seq::NUMBER(20, 0), 0) DESC
        ) AS rn
      FROM @TICDC_META.CTL_STAGE
        (FILE_FORMAT => 'TICDC_META.FF_TICDC_MANIFEST_JSON', PATTERN => '.*\\/control\\/ddl\\/.*\\.json')
      WHERE $1:event_id IS NOT NULL
        AND STARTSWITH(METADATA$FILENAME, :v_control_prefix)
        AND REGEXP_LIKE(METADATA$FILENAME, '.*/control/ddl/.*\\.json$', 'i')
    )
    WHERE rn = 1
  ) s
  ON t.integration_id = s.integration_id AND t.event_id = s.event_id
  WHEN MATCHED THEN
    UPDATE SET
      ddl_group_id = s.ddl_group_id,
      object_id = s.object_id,
      commit_ts = s.commit_ts,
      seq = s.seq,
      source_db = s.source_db,
      source_table = s.source_table,
      ddl_type = s.ddl_type,
      ddl_apply_class = s.ddl_apply_class,
      need_rebuild = s.need_rebuild,
      rebuild_from_ts = s.rebuild_from_ts,
      table_id_after = s.table_id_after,
      table_version_after = s.table_version_after,
      ddl_query = s.ddl_query,
      payload = s.payload,
      file_path = s.file_path,
      loaded_at = CURRENT_TIMESTAMP(),
      apply_status = IFF(t.apply_status IN ('APPLIED', 'REBUILD_REQUIRED'), t.apply_status, 'PENDING'),
      last_error = NULL
  WHEN NOT MATCHED THEN
    INSERT (
      integration_id,
      event_id,
      ddl_group_id,
      object_id,
      commit_ts,
      seq,
      source_db,
      source_table,
      ddl_type,
      ddl_apply_class,
      need_rebuild,
      rebuild_from_ts,
      table_id_after,
      table_version_after,
      ddl_query,
      payload,
      file_path,
      loaded_at,
      apply_status
    )
    VALUES (
      s.integration_id,
      s.event_id,
      s.ddl_group_id,
      s.object_id,
      s.commit_ts,
      s.seq,
      s.source_db,
      s.source_table,
      s.ddl_type,
      s.ddl_apply_class,
      s.need_rebuild,
      s.rebuild_from_ts,
      s.table_id_after,
      s.table_version_after,
      s.ddl_query,
      s.payload,
      s.file_path,
      CURRENT_TIMESTAMP(),
      'PENDING'
    );

  UPDATE TICDC_META.CONTROL_FILE_REGISTRY r
     SET load_status = 'LOADED',
         loaded_at = CURRENT_TIMESTAMP(),
         last_error = NULL,
         last_seen_at = CURRENT_TIMESTAMP()
    FROM TICDC_META.DDL_EVENT_QUEUE q
   WHERE r.integration_id = :p_integration_id
     AND q.integration_id = r.integration_id
     AND r.file_type = 'ddl'
     AND r.file_path = q.file_path;

  MERGE INTO TICDC_META.GLOBAL_CHECKPOINT_STATE t
  USING (
    SELECT *
    FROM (
      SELECT
        :p_integration_id AS integration_id,
        $1:changefeed_id::STRING AS changefeed_id,
        $1:keyspace::STRING AS keyspace,
        $1:changefeed_gid::STRING AS changefeed_gid,
        $1:resolved_ts::NUMBER(20, 0) AS resolved_ts,
        METADATA$FILENAME::STRING AS source_file,
        ROW_NUMBER() OVER (
          PARTITION BY :p_integration_id
          ORDER BY $1:resolved_ts::NUMBER(20, 0) DESC
        ) AS rn
      FROM @TICDC_META.CTL_STAGE
        (FILE_FORMAT => 'TICDC_META.FF_TICDC_MANIFEST_JSON', PATTERN => '.*\\/control\\/checkpoint\\/global\\/.*\\.json')
      WHERE $1:resolved_ts IS NOT NULL
        AND STARTSWITH(METADATA$FILENAME, :v_control_prefix)
        AND REGEXP_LIKE(METADATA$FILENAME, '.*/control/checkpoint/global/.*\\.json$', 'i')
    )
    WHERE rn = 1
  ) s
  ON t.integration_id = s.integration_id
  WHEN MATCHED AND NVL(s.resolved_ts, 0) >= NVL(t.resolved_ts, 0) THEN
    UPDATE SET
      changefeed_id = s.changefeed_id,
      keyspace = s.keyspace,
      changefeed_gid = s.changefeed_gid,
      resolved_ts = s.resolved_ts,
      source_file = s.source_file,
      updated_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (
      integration_id,
      changefeed_id,
      keyspace,
      changefeed_gid,
      resolved_ts,
      source_file,
      updated_at
    )
    VALUES (
      s.integration_id,
      s.changefeed_id,
      s.keyspace,
      s.changefeed_gid,
      s.resolved_ts,
      s.source_file,
      CURRENT_TIMESTAMP()
    );

  UPDATE TICDC_META.CONTROL_FILE_REGISTRY r
     SET load_status = 'LOADED',
         loaded_at = CURRENT_TIMESTAMP(),
         last_error = NULL,
         last_seen_at = CURRENT_TIMESTAMP()
   WHERE r.integration_id = :p_integration_id
     AND r.file_type = 'checkpoint'
     AND STARTSWITH(r.file_path, :v_control_prefix);

  SELECT COUNT(*)
    INTO :v_loaded
    FROM TICDC_META.DDL_EVENT_QUEUE
   WHERE integration_id = :p_integration_id
     AND apply_status = 'PENDING';

  SELECT COALESCE(MAX(resolved_ts), 0)
    INTO :v_resolved_ts
    FROM TICDC_META.GLOBAL_CHECKPOINT_STATE
   WHERE integration_id = :p_integration_id;

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_LOAD_DDL_MANIFESTS',
    'integration_id',
    p_integration_id,
    'control_prefix',
    v_control_prefix,
    'loaded',
    v_loaded,
    'resolved_ts',
    v_resolved_ts
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
    SELECT NULL, 'SP_LOAD_DDL_MANIFESTS', :p_integration_id, NULL, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
