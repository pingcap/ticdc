CREATE OR REPLACE PROCEDURE TICDC_META.SP_ENSURE_RAW_CHANGE_TABLE(
  p_integration_id STRING,
  p_object_id STRING
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_safe_object_id STRING DEFAULT NULL;
  v_change_external_table STRING DEFAULT NULL;
  v_change_metadata_file_path STRING DEFAULT NULL;
  v_change_base_location STRING DEFAULT NULL;
  v_external_volume STRING DEFAULT NULL;
  v_catalog_integration STRING DEFAULT NULL;
  v_external_table_columns STRING DEFAULT NULL;
  v_sql STRING DEFAULT NULL;
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

  v_safe_object_id := REGEXP_REPLACE(p_object_id, '[^A-Za-z0-9_]', '_');

  SELECT
    COALESCE(MAX(change_external_table), 'TICDC_RAW.PUBLIC.' || :v_safe_object_id || '__CHANGE'),
    MAX(change_metadata_file_path),
    MAX(change_base_location)
    INTO :v_change_external_table, :v_change_metadata_file_path, :v_change_base_location
    FROM TICDC_META.OBJECT_REGISTRY
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id;

  SELECT MAX(external_volume), MAX(catalog_integration)
    INTO :v_external_volume, :v_catalog_integration
    FROM TICDC_META.INTEGRATION_REGISTRY
   WHERE integration_id = :p_integration_id
     AND COALESCE(is_enabled, TRUE);

  MERGE INTO TICDC_META.OBJECT_REGISTRY t
  USING (
    SELECT
      :p_integration_id AS integration_id,
      :p_object_id AS object_id,
      :v_change_external_table AS change_external_table
  ) s
  ON t.integration_id = s.integration_id AND t.object_id = s.object_id
  WHEN MATCHED THEN
    UPDATE SET
      change_external_table = COALESCE(t.change_external_table, s.change_external_table),
      updated_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (integration_id, object_id, change_external_table, materialization_status, is_enabled, created_at, updated_at)
    VALUES (s.integration_id, s.object_id, s.change_external_table, 'ACTIVE', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());

  CREATE FILE FORMAT IF NOT EXISTS TICDC.TICDC_META.FF_PARQUET
    TYPE = PARQUET;

  SELECT COALESCE(
      LISTAGG(
        '"' || REPLACE(COALESCE(target_column_name, column_name), '"', '""') || '" ' ||
        COALESCE(snowflake_type, 'VARIANT') || ' AS (' ||
        CASE
          WHEN COALESCE(snowflake_type, '') LIKE 'NUMBER(%' THEN
            'VALUE:' || COALESCE(target_column_name, column_name) || '::' || snowflake_type
          WHEN COALESCE(snowflake_type, '') LIKE 'TIMESTAMP_NTZ(%' THEN
            'TO_TIMESTAMP_NTZ(VALUE:' || COALESCE(target_column_name, column_name) || '::NUMBER, 6)'
          WHEN COALESCE(snowflake_type, '') = 'DATE' THEN
            'TO_DATE(VALUE:' || COALESCE(target_column_name, column_name) || '::STRING)'
          WHEN COALESCE(snowflake_type, '') LIKE 'TIME(%' THEN
            'TO_TIME(VALUE:' || COALESCE(target_column_name, column_name) || '::STRING)'
          WHEN COALESCE(snowflake_type, '') = 'BINARY' THEN
            'VALUE:' || COALESCE(target_column_name, column_name) || '::BINARY'
          WHEN COALESCE(snowflake_type, '') = 'VARCHAR' THEN
            'VALUE:' || COALESCE(target_column_name, column_name) || '::STRING'
          ELSE
            'VALUE:' || COALESCE(target_column_name, column_name) || '::VARIANT'
        END || ')',
        ', '
      ) WITHIN GROUP (ORDER BY ordinal_position),
      ''
    )
    INTO :v_external_table_columns
    FROM TICDC_META.COLUMN_REGISTRY
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id
     AND COALESCE(is_deleted, FALSE) = FALSE;

  IF (v_change_metadata_file_path IS NOT NULL AND v_external_volume IS NOT NULL AND v_catalog_integration IS NOT NULL) THEN
    v_sql := 'CREATE ICEBERG TABLE IF NOT EXISTS ' || v_change_external_table ||
      ' EXTERNAL_VOLUME = ''' || REPLACE(v_external_volume, '''', '''''') ||
      ''' CATALOG = ''' || REPLACE(v_catalog_integration, '''', '''''') ||
      ''' METADATA_FILE_PATH = ''' || REPLACE(v_change_metadata_file_path, '''', '''''') || '''';
    EXECUTE IMMEDIATE v_sql;

    v_sql := 'ALTER ICEBERG TABLE IF EXISTS ' || v_change_external_table ||
      ' REFRESH ''' || REPLACE(v_change_metadata_file_path, '''', '''''') || '''';
    EXECUTE IMMEDIATE v_sql;
  ELSEIF (v_change_base_location IS NOT NULL AND v_change_base_location <> '') THEN
    v_sql := 'CREATE OR REPLACE EXTERNAL TABLE ' || v_change_external_table || ' (' ||
      '"_tidb_op" STRING AS (VALUE:_tidb_op::STRING), ' ||
      '"_tidb_commit_ts" NUMBER(38,0) AS (VALUE:_tidb_commit_ts::NUMBER(38,0)), ' ||
      '"_tidb_commit_time" TIMESTAMP_NTZ(6) AS (TO_TIMESTAMP_NTZ(VALUE:_tidb_commit_time::NUMBER, 6)), ' ||
      '"_tidb_table_version" NUMBER(38,0) AS (VALUE:_tidb_table_version::NUMBER(38,0)), ' ||
      '"_tidb_row_identity" STRING AS (VALUE:_tidb_row_identity::STRING), ' ||
      '"_tidb_old_row_identity" STRING AS (VALUE:_tidb_old_row_identity::STRING), ' ||
      '"_tidb_identity_kind" STRING AS (VALUE:_tidb_identity_kind::STRING)' ||
      IFF(v_external_table_columns = '', '', ', ' || v_external_table_columns) ||
      ') WITH LOCATION = @TICDC.TICDC_META.CTL_STAGE/' || REPLACE(v_change_base_location || 'data/', '''', '''''') ||
      ' AUTO_REFRESH = FALSE FILE_FORMAT = (FORMAT_NAME = ''TICDC.TICDC_META.FF_PARQUET'')';
    EXECUTE IMMEDIATE v_sql;

    v_sql := 'ALTER EXTERNAL TABLE ' || v_change_external_table || ' REFRESH';
    EXECUTE IMMEDIATE v_sql;
  END IF;

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_ENSURE_RAW_CHANGE_TABLE',
    'integration_id',
    p_integration_id,
    'object_id',
    p_object_id,
    'change_external_table',
    v_change_external_table
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
    SELECT NULL, 'SP_ENSURE_RAW_CHANGE_TABLE', :p_object_id || '@' || :p_integration_id, NULL, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
