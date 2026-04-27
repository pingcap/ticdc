CREATE OR REPLACE PROCEDURE TICDC_META.SP_REGISTER_SNAPSHOT_TABLES(
  p_integration_id STRING,
  p_generation STRING
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_control_prefix STRING DEFAULT NULL;
  v_registered NUMBER(20, 0) DEFAULT 0;
  v_external_volume STRING DEFAULT NULL;
  v_catalog_integration STRING DEFAULT NULL;
  v_snapshot_external_table STRING DEFAULT NULL;
  v_snapshot_metadata_file_path STRING DEFAULT NULL;
  v_sql STRING DEFAULT NULL;
  c_snapshots CURSOR FOR
    SELECT snapshot_external_table, snapshot_metadata_file_path
      FROM TICDC_META.OBJECT_REGISTRY
     WHERE integration_id = ?
       AND generation = ?
       AND COALESCE(is_enabled, TRUE)
       AND snapshot_external_table IS NOT NULL
       AND snapshot_metadata_file_path IS NOT NULL
     ORDER BY object_id;
BEGIN
  CALL TICDC_META.SP_DISCOVER_CONTROL_FILES(:p_integration_id);

  CREATE FILE FORMAT IF NOT EXISTS TICDC_META.FF_TICDC_MANIFEST_JSON
    TYPE = JSON
    STRIP_OUTER_ARRAY = FALSE;

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
      'SP_REGISTER_SNAPSHOT_TABLES',
      'integration_id',
      p_integration_id,
      'generation',
      p_generation,
      'reason',
      'integration is not registered or has an empty control prefix'
    );
  END IF;

  MERGE INTO TICDC_META.OBJECT_REGISTRY t
  USING (
    SELECT *
    FROM (
      SELECT
        :p_integration_id AS integration_id,
        COALESCE($1:object_id::STRING, $1:table_id::STRING, CONCAT($1:source_db::STRING, '.', $1:source_table::STRING)) AS object_id,
        $1:source_db::STRING AS source_db,
        $1:source_table::STRING AS source_table,
        COALESCE($1:target_database::STRING, 'TICDC_REPLICA') AS target_database,
        COALESCE($1:target_schema::STRING, $1:source_db::STRING, 'PUBLIC') AS target_schema,
        COALESCE($1:target_table::STRING, $1:source_table::STRING, $1:table_name::STRING) AS target_table,
        $1:target_base_table::STRING AS target_base_table,
        $1:snapshot_external_table::STRING AS snapshot_external_table,
        $1:change_external_table::STRING AS change_external_table,
        COALESCE($1:snapshot_metadata_file_path::STRING, $1:snapshot_metadata::STRING) AS snapshot_metadata_file_path,
        COALESCE($1:change_metadata_file_path::STRING, $1:change_metadata::STRING) AS change_metadata_file_path,
        COALESCE($1:snapshot_base_location::STRING, $1:snapshot_location::STRING) AS snapshot_base_location,
        COALESCE($1:change_base_location::STRING, $1:change_location::STRING) AS change_base_location,
        $1:table_id::NUMBER(20, 0) AS table_id,
        COALESCE($1:table_version::NUMBER(20, 0), $1:table_version_after::NUMBER(20, 0)) AS table_version,
        $1:bootstrap_ts::NUMBER(20, 0) AS bootstrap_ts,
        COALESCE($1:generation::STRING, :p_generation) AS generation,
        ROW_NUMBER() OVER (
          PARTITION BY :p_integration_id, COALESCE($1:object_id::STRING, $1:table_id::STRING, CONCAT($1:source_db::STRING, '.', $1:source_table::STRING))
          ORDER BY COALESCE($1:commit_ts::NUMBER(20, 0), $1:table_version::NUMBER(20, 0), 0) DESC
        ) AS rn
      FROM @TICDC_META.CTL_STAGE
        (FILE_FORMAT => 'TICDC_META.FF_TICDC_MANIFEST_JSON', PATTERN => '.*\\/control\\/schema\\/.*\\.json')
      WHERE STARTSWITH(METADATA$FILENAME, :v_control_prefix)
        AND REGEXP_LIKE(METADATA$FILENAME, '.*/control/schema/.*\\.json$', 'i')
        AND COALESCE($1:generation::STRING, :p_generation) = :p_generation
    )
    WHERE rn = 1
  ) s
  ON t.integration_id = s.integration_id AND t.object_id = s.object_id
  WHEN MATCHED THEN
    UPDATE SET
      source_db = COALESCE(s.source_db, t.source_db),
      source_table = COALESCE(s.source_table, t.source_table),
      target_database = COALESCE(s.target_database, t.target_database),
      target_schema = COALESCE(s.target_schema, t.target_schema),
      target_table = COALESCE(s.target_table, t.target_table),
      target_base_table = COALESCE(s.target_base_table, t.target_base_table),
      snapshot_external_table = COALESCE(s.snapshot_external_table, t.snapshot_external_table),
      change_external_table = COALESCE(s.change_external_table, t.change_external_table),
      snapshot_metadata_file_path = COALESCE(s.snapshot_metadata_file_path, t.snapshot_metadata_file_path),
      change_metadata_file_path = COALESCE(s.change_metadata_file_path, t.change_metadata_file_path),
      snapshot_base_location = COALESCE(s.snapshot_base_location, t.snapshot_base_location),
      change_base_location = COALESCE(s.change_base_location, t.change_base_location),
      table_id = COALESCE(s.table_id, t.table_id),
      table_version = COALESCE(s.table_version, t.table_version),
      bootstrap_ts = COALESCE(s.bootstrap_ts, t.bootstrap_ts),
      generation = COALESCE(s.generation, t.generation),
      materialization_status = COALESCE(t.materialization_status, 'ACTIVE'),
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
      snapshot_external_table,
      change_external_table,
      snapshot_metadata_file_path,
      change_metadata_file_path,
      snapshot_base_location,
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
      s.snapshot_external_table,
      s.change_external_table,
      s.snapshot_metadata_file_path,
      s.change_metadata_file_path,
      s.snapshot_base_location,
      s.change_base_location,
      s.table_id,
      s.table_version,
      s.bootstrap_ts,
      s.generation,
      'ACTIVE',
      TRUE,
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP()
    );

  MERGE INTO TICDC_META.COLUMN_REGISTRY t
  USING (
    SELECT
      :p_integration_id AS integration_id,
      COALESCE($1:object_id::STRING, $1:table_id::STRING, CONCAT($1:source_db::STRING, '.', $1:source_table::STRING)) AS object_id,
      f.value:name::STRING AS column_name,
      COALESCE(f.value:target_name::STRING, f.value:name::STRING) AS target_column_name,
      COALESCE(f.value:snowflake_type::STRING, f.value:type::STRING, 'VARIANT') AS snowflake_type,
      COALESCE(f.index::NUMBER(20, 0), f.value:ordinal_position::NUMBER(20, 0), 0) AS ordinal_position,
      COALESCE(f.value:nullable::BOOLEAN, TRUE) AS is_nullable,
      COALESCE(f.value:is_primary_key::BOOLEAN, FALSE) AS is_primary_key,
      COALESCE(f.value:is_handle_key::BOOLEAN, f.value:is_primary_key::BOOLEAN, FALSE) AS is_handle_key,
      COALESCE(f.value:is_deleted::BOOLEAN, FALSE) AS is_deleted
    FROM @TICDC_META.CTL_STAGE
      (FILE_FORMAT => 'TICDC_META.FF_TICDC_MANIFEST_JSON', PATTERN => '.*\\/control\\/schema\\/.*\\.json'),
      LATERAL FLATTEN(input => $1:columns) f
    WHERE STARTSWITH(METADATA$FILENAME, :v_control_prefix)
      AND REGEXP_LIKE(METADATA$FILENAME, '.*/control/schema/.*\\.json$', 'i')
      AND COALESCE($1:generation::STRING, :p_generation) = :p_generation
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

  SELECT MAX(external_volume), MAX(catalog_integration)
    INTO :v_external_volume, :v_catalog_integration
    FROM TICDC_META.INTEGRATION_REGISTRY
   WHERE integration_id = :p_integration_id
     AND COALESCE(is_enabled, TRUE);

  IF (v_external_volume IS NOT NULL AND v_catalog_integration IS NOT NULL) THEN
    OPEN c_snapshots USING (p_integration_id, p_generation);
    FOR rec IN c_snapshots DO
      v_snapshot_external_table := rec.snapshot_external_table;
      v_snapshot_metadata_file_path := rec.snapshot_metadata_file_path;
      v_sql := 'CREATE ICEBERG TABLE IF NOT EXISTS ' || v_snapshot_external_table ||
        ' EXTERNAL_VOLUME = ''' || REPLACE(v_external_volume, '''', '''''') ||
        ''' CATALOG = ''' || REPLACE(v_catalog_integration, '''', '''''') ||
        ''' METADATA_FILE_PATH = ''' || REPLACE(v_snapshot_metadata_file_path, '''', '''''') || '''';
      EXECUTE IMMEDIATE v_sql;
    END FOR;
    CLOSE c_snapshots;
  END IF;

  SELECT COUNT(*)
    INTO :v_registered
    FROM TICDC_META.OBJECT_REGISTRY
   WHERE integration_id = :p_integration_id
     AND generation = :p_generation
     AND COALESCE(is_enabled, TRUE);

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_REGISTER_SNAPSHOT_TABLES',
    'integration_id',
    p_integration_id,
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
    SELECT NULL, 'SP_REGISTER_SNAPSHOT_TABLES', :p_integration_id, NULL, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
