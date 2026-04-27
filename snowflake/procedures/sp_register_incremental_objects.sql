CREATE OR REPLACE PROCEDURE TICDC_META.SP_REGISTER_INCREMENTAL_OBJECTS(p_integration_id STRING, p_upper_ts NUMBER(20, 0))
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_control_prefix STRING DEFAULT NULL;
  v_data_prefix STRING DEFAULT NULL;
  v_raw_database STRING DEFAULT 'TICDC_RAW';
  v_raw_schema STRING DEFAULT 'PUBLIC';
  v_replica_database STRING DEFAULT 'TICDC_REPLICA';
  v_replica_schema STRING DEFAULT 'PUBLIC';
  v_registered NUMBER(20, 0) DEFAULT 0;
BEGIN
  CALL TICDC_META.SP_DISCOVER_CONTROL_FILES(:p_integration_id);

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

  SELECT
    MAX(control_prefix),
    COALESCE(MAX(raw_database), 'TICDC_RAW'),
    COALESCE(MAX(raw_schema), 'PUBLIC'),
    COALESCE(MAX(replica_database), 'TICDC_REPLICA'),
    COALESCE(MAX(replica_schema), 'PUBLIC')
    INTO
      :v_control_prefix,
      :v_raw_database,
      :v_raw_schema,
      :v_replica_database,
      :v_replica_schema
    FROM TICDC_META.INTEGRATION_REGISTRY
   WHERE integration_id = :p_integration_id
     AND COALESCE(is_enabled, TRUE);

  IF (v_control_prefix IS NULL OR v_control_prefix = '') THEN
    RETURN OBJECT_CONSTRUCT(
      'status',
      'skipped',
      'procedure',
      'SP_REGISTER_INCREMENTAL_OBJECTS',
      'integration_id',
      p_integration_id,
      'upper_ts',
      p_upper_ts,
      'reason',
      'integration is not registered or has an empty control prefix'
    );
  END IF;

  v_data_prefix := REGEXP_REPLACE(v_control_prefix, 'control/.*$', '');

  MERGE INTO TICDC_META.OBJECT_REGISTRY t
  USING (
    WITH latest_schema AS (
      SELECT
        :p_integration_id AS integration_id,
        COALESCE($1:object_id::STRING, $1:table_id::STRING, CONCAT($1:source_db::STRING, '.', $1:source_table::STRING)) AS object_id,
        $1:source_db::STRING AS source_db,
        $1:source_table::STRING AS source_table,
        COALESCE($1:table_id::NUMBER(20, 0), $1:tidb_table_info:id::NUMBER(20, 0)) AS table_id,
        COALESCE($1:table_version::NUMBER(20, 0), $1:"TableVersion"::NUMBER(20, 0), 0) AS table_version,
        COALESCE($1:generation::STRING, 'INCREMENTAL') AS generation,
        $1 AS payload
      FROM @TICDC_META.CTL_STAGE
        (FILE_FORMAT => 'TICDC_META.FF_TICDC_MANIFEST_JSON', PATTERN => '.*\\/control\\/schema\\/.*\\.json')
      WHERE STARTSWITH(METADATA$FILENAME, :v_control_prefix)
        AND REGEXP_LIKE(METADATA$FILENAME, '.*/control/schema/.*\\.json$', 'i')
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY COALESCE($1:object_id::STRING, $1:table_id::STRING, CONCAT($1:source_db::STRING, '.', $1:source_table::STRING))
        ORDER BY COALESCE($1:table_version::NUMBER(20, 0), $1:"TableVersion"::NUMBER(20, 0), 0) DESC, METADATA$FILENAME DESC
      ) = 1
    ),
    latest_metadata AS (
      SELECT
        s.object_id,
        d.relative_path AS change_metadata_file_path
      FROM DIRECTORY(@TICDC_META.CTL_STAGE) d
      JOIN latest_schema s
        ON STARTSWITH(d.relative_path, :v_data_prefix || s.source_db || '/' || s.source_table || '/metadata/')
       AND REGEXP_LIKE(d.relative_path, '.*/metadata/v[^/]+\\.metadata\\.json$', 'i')
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.object_id
        ORDER BY d.last_modified DESC, d.relative_path DESC
      ) = 1
    ),
    create_ddl AS (
      SELECT
        object_id,
        commit_ts AS bootstrap_ts,
        table_version_after
      FROM TICDC_META.DDL_EVENT_QUEUE
      WHERE integration_id = :p_integration_id
        AND commit_ts <= :p_upper_ts
        AND LOWER(COALESCE(ddl_type, '')) = 'create table'
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY object_id
        ORDER BY commit_ts DESC, seq DESC, event_id DESC
      ) = 1
    )
    SELECT
      s.integration_id,
      s.object_id,
      s.source_db,
      s.source_table,
      :v_replica_database AS target_database,
      :v_replica_schema AS target_schema,
      UPPER(REGEXP_REPLACE(s.source_db || '_' || s.source_table, '[^A-Za-z0-9_]', '_')) AS target_table,
      :v_replica_database || '.' || :v_replica_schema || '.' ||
        UPPER(REGEXP_REPLACE(s.source_db || '_' || s.source_table, '[^A-Za-z0-9_]', '_')) || '__BASE' AS target_base_table,
      :v_raw_database || '.' || :v_raw_schema || '.' ||
        UPPER(REGEXP_REPLACE(s.source_db || '_' || s.source_table, '[^A-Za-z0-9_]', '_')) || '__CHANGE' AS change_external_table,
      COALESCE(
        m.change_metadata_file_path,
        s.payload:change_metadata_file_path::STRING,
        s.payload:change_metadata::STRING
      ) AS change_metadata_file_path,
      :v_data_prefix || s.source_db || '/' || s.source_table || '/' AS change_base_location,
      s.table_id,
      COALESCE(c.table_version_after, s.table_version) AS table_version,
      COALESCE(c.bootstrap_ts, s.payload:bootstrap_ts::NUMBER(20, 0)) AS bootstrap_ts,
      s.generation
    FROM latest_schema s
    LEFT JOIN latest_metadata m
      ON m.object_id = s.object_id
    LEFT JOIN create_ddl c
      ON c.object_id = s.object_id
  ) s
  ON t.integration_id = s.integration_id AND t.object_id = s.object_id
  WHEN MATCHED THEN
    UPDATE SET
      source_db = COALESCE(s.source_db, t.source_db),
      source_table = COALESCE(s.source_table, t.source_table),
      target_database = COALESCE(t.target_database, s.target_database),
      target_schema = COALESCE(t.target_schema, s.target_schema),
      target_table = COALESCE(t.target_table, s.target_table),
      target_base_table = COALESCE(t.target_base_table, s.target_base_table),
      change_external_table = COALESCE(t.change_external_table, s.change_external_table),
      change_metadata_file_path = COALESCE(s.change_metadata_file_path, t.change_metadata_file_path),
      change_base_location = COALESCE(s.change_base_location, t.change_base_location),
      table_id = COALESCE(s.table_id, t.table_id),
      table_version = COALESCE(s.table_version, t.table_version),
      bootstrap_ts = COALESCE(s.bootstrap_ts, t.bootstrap_ts),
      generation = COALESCE(s.generation, t.generation),
      materialization_status = CASE
        WHEN COALESCE(t.materialization_status, '') IN ('ACTIVE', 'PAUSED_FOR_REBUILD', 'REBUILDING') THEN t.materialization_status
        ELSE 'PENDING_DDL'
      END,
      is_enabled = TRUE,
      updated_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (
      integration_id,
      object_id,
      source_db,
      source_table,
      target_database,
      target_schema,
      target_table,
      target_base_table,
      change_external_table,
      change_metadata_file_path,
      change_base_location,
      table_id,
      table_version,
      bootstrap_ts,
      generation,
      materialization_status,
      is_enabled,
      created_at,
      updated_at
    )
    VALUES (
      s.integration_id,
      s.object_id,
      s.source_db,
      s.source_table,
      s.target_database,
      s.target_schema,
      s.target_table,
      s.target_base_table,
      s.change_external_table,
      s.change_metadata_file_path,
      s.change_base_location,
      s.table_id,
      s.table_version,
      s.bootstrap_ts,
      s.generation,
      'PENDING_DDL',
      TRUE,
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP()
    );

  MERGE INTO TICDC_META.COLUMN_REGISTRY t
  USING (
    WITH latest_schema AS (
      SELECT
        :p_integration_id AS integration_id,
        COALESCE($1:object_id::STRING, $1:table_id::STRING, CONCAT($1:source_db::STRING, '.', $1:source_table::STRING)) AS object_id,
        $1 AS payload
      FROM @TICDC_META.CTL_STAGE
        (FILE_FORMAT => 'TICDC_META.FF_TICDC_MANIFEST_JSON', PATTERN => '.*\\/control\\/schema\\/.*\\.json')
      WHERE STARTSWITH(METADATA$FILENAME, :v_control_prefix)
        AND REGEXP_LIKE(METADATA$FILENAME, '.*/control/schema/.*\\.json$', 'i')
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY COALESCE($1:object_id::STRING, $1:table_id::STRING, CONCAT($1:source_db::STRING, '.', $1:source_table::STRING))
        ORDER BY COALESCE($1:table_version::NUMBER(20, 0), $1:"TableVersion"::NUMBER(20, 0), 0) DESC, METADATA$FILENAME DESC
      ) = 1
    ),
    flattened_columns AS (
      SELECT
        s.integration_id,
        s.object_id,
        f.value:"ColumnName"::STRING AS column_name,
        COALESCE(f.value:"TargetColumnName"::STRING, f.value:"ColumnName"::STRING) AS target_column_name,
        UPPER(COALESCE(f.value:"ColumnType"::STRING, 'VARIANT')) AS column_type,
        NULLIF(f.value:"ColumnPrecision"::STRING, '') AS column_precision,
        NULLIF(f.value:"ColumnScale"::STRING, '') AS column_scale,
        COALESCE(f.index::NUMBER(20, 0), 0) + 1 AS ordinal_position,
        IFF(LOWER(COALESCE(f.value:"ColumnNullable"::STRING, 'true')) IN ('false', '0', 'no'), FALSE, TRUE) AS is_nullable,
        IFF(LOWER(COALESCE(f.value:"ColumnIsPk"::STRING, 'false')) IN ('true', '1', 'yes'), TRUE, FALSE) AS is_primary_key,
        IFF(LOWER(COALESCE(f.value:"ColumnIsPk"::STRING, 'false')) IN ('true', '1', 'yes'), TRUE, FALSE) AS is_handle_key
      FROM latest_schema s,
        LATERAL FLATTEN(input => COALESCE(s.payload:columns, s.payload:"TableColumns", PARSE_JSON('[]'))) f
      WHERE f.value:"ColumnName" IS NOT NULL
    )
    SELECT
      integration_id,
      object_id,
      column_name,
      target_column_name,
      CASE
        WHEN REGEXP_LIKE(column_type, '^(DECIMAL|NUMERIC)') THEN
          'NUMBER(' || COALESCE(column_precision, '38') || ',' || COALESCE(column_scale, '0') || ')'
        WHEN column_type = 'BIGINT' OR column_type = 'BIGINT UNSIGNED' THEN
          'NUMBER(' || COALESCE(column_precision, '19') || ',0)'
        WHEN REGEXP_LIKE(column_type, '^(TINYINT|SMALLINT|MEDIUMINT|INT|INTEGER|BIT|YEAR)') OR column_type = 'INT UNSIGNED' THEN
          'NUMBER(' || COALESCE(column_precision, '10') || ',0)'
        WHEN REGEXP_LIKE(column_type, '^(FLOAT|DOUBLE|REAL)') THEN column_type
        WHEN column_type = 'DATE' THEN 'DATE'
        WHEN column_type = 'TIME' THEN
          'TIME(' || COALESCE(column_scale, '6') || ')'
        WHEN column_type = 'DATETIME' OR column_type = 'TIMESTAMP' THEN
          'TIMESTAMP_NTZ(' || COALESCE(column_scale, '6') || ')'
        WHEN REGEXP_LIKE(column_type, 'VARCHAR|CHAR|TEXT|ENUM|SET|JSON') OR column_type = 'VAR_STRING' THEN 'VARCHAR'
        WHEN REGEXP_LIKE(column_type, 'BLOB|BINARY') THEN 'BINARY'
        ELSE 'VARIANT'
      END AS snowflake_type,
      ordinal_position,
      is_nullable,
      is_primary_key,
      is_handle_key,
      FALSE AS is_deleted
    FROM flattened_columns
  ) s
  ON t.integration_id = s.integration_id AND t.object_id = s.object_id AND t.column_name = s.column_name
  WHEN MATCHED THEN
    UPDATE SET
      target_column_name = s.target_column_name,
      snowflake_type = s.snowflake_type,
      ordinal_position = s.ordinal_position,
      is_nullable = s.is_nullable,
      is_primary_key = s.is_primary_key,
      is_handle_key = s.is_handle_key,
      is_deleted = s.is_deleted,
      updated_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (
      integration_id,
      object_id,
      column_name,
      target_column_name,
      snowflake_type,
      ordinal_position,
      is_nullable,
      is_primary_key,
      is_handle_key,
      is_deleted,
      updated_at
    )
    VALUES (
      s.integration_id,
      s.object_id,
      s.column_name,
      s.target_column_name,
      s.snowflake_type,
      s.ordinal_position,
      s.is_nullable,
      s.is_primary_key,
      s.is_handle_key,
      s.is_deleted,
      CURRENT_TIMESTAMP()
    );

  SELECT COUNT(*)
    INTO :v_registered
    FROM TICDC_META.OBJECT_REGISTRY
   WHERE integration_id = :p_integration_id
     AND COALESCE(is_enabled, TRUE);

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_REGISTER_INCREMENTAL_OBJECTS',
    'integration_id',
    p_integration_id,
    'upper_ts',
    p_upper_ts,
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
    SELECT NULL, 'SP_REGISTER_INCREMENTAL_OBJECTS', :p_integration_id, :p_upper_ts, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
