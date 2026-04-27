CREATE OR REPLACE PROCEDURE TICDC_META.SP_ENSURE_TARGET_TABLE(
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
  v_target_base_table STRING DEFAULT NULL;
  v_target_view STRING DEFAULT NULL;
  v_business_columns STRING DEFAULT '';
  v_view_columns STRING DEFAULT '';
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
    COALESCE(
      MAX(target_base_table),
      MAX(target_database || '.' || target_schema || '.' || target_table || '__BASE'),
      'TICDC_REPLICA.PUBLIC.' || :v_safe_object_id || '__BASE'
    ),
    COALESCE(
      MAX(target_database || '.' || target_schema || '.' || target_table),
      'TICDC_REPLICA.PUBLIC.' || :v_safe_object_id
    )
    INTO :v_target_base_table, :v_target_view
    FROM TICDC_META.OBJECT_REGISTRY
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id;

  MERGE INTO TICDC_META.OBJECT_REGISTRY t
  USING (
    SELECT
      :p_integration_id AS integration_id,
      :p_object_id AS object_id,
      :v_target_base_table AS target_base_table,
      :v_target_view AS target_table
  ) s
  ON t.integration_id = s.integration_id AND t.object_id = s.object_id
  WHEN MATCHED THEN
    UPDATE SET
      target_base_table = COALESCE(t.target_base_table, s.target_base_table),
      target_table = COALESCE(t.target_table, s.target_table),
      materialization_status = COALESCE(t.materialization_status, 'ACTIVE'),
      is_enabled = COALESCE(t.is_enabled, TRUE),
      updated_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (
      integration_id,
      object_id,
      target_base_table,
      target_table,
      materialization_status,
      is_enabled,
      created_at,
      updated_at
    )
    VALUES (
      s.integration_id,
      s.object_id,
      s.target_base_table,
      s.target_table,
      'ACTIVE',
      TRUE,
      CURRENT_TIMESTAMP(),
      CURRENT_TIMESTAMP()
    );

  SELECT COALESCE(
      LISTAGG(
        '"' || REPLACE(COALESCE(target_column_name, column_name), '"', '""') || '" ' ||
        COALESCE(snowflake_type, 'VARIANT') ||
        IFF(COALESCE(is_nullable, TRUE), '', ' NOT NULL'),
        ', '
      ) WITHIN GROUP (ORDER BY ordinal_position),
      ''
    )
    INTO :v_business_columns
    FROM TICDC_META.COLUMN_REGISTRY
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id
     AND COALESCE(is_deleted, FALSE) = FALSE;

  v_sql := 'CREATE TABLE IF NOT EXISTS ' || v_target_base_table ||
    ' (__ticdc_row_identity VARCHAR NOT NULL, __ticdc_last_commit_ts NUMBER(20,0) NOT NULL, __ticdc_last_commit_time TIMESTAMP_NTZ(6), __ticdc_table_version NUMBER(20,0)' ||
    IFF(v_business_columns = '', '', ', ' || v_business_columns) ||
    ')';
  EXECUTE IMMEDIATE v_sql;

  SELECT COALESCE(
      LISTAGG(
        '"' || REPLACE(COALESCE(target_column_name, column_name), '"', '""') || '"',
        ', '
      ) WITHIN GROUP (ORDER BY ordinal_position),
      '__ticdc_row_identity, __ticdc_last_commit_ts'
    )
    INTO :v_view_columns
    FROM TICDC_META.COLUMN_REGISTRY
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id
     AND COALESCE(is_deleted, FALSE) = FALSE;

  v_sql := 'CREATE OR REPLACE VIEW ' || v_target_view || ' AS SELECT ' || v_view_columns || ' FROM ' || v_target_base_table;
  EXECUTE IMMEDIATE v_sql;

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_ENSURE_TARGET_TABLE',
    'integration_id',
    p_integration_id,
    'object_id',
    p_object_id,
    'target_base_table',
    v_target_base_table,
    'target_view',
    v_target_view
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
    SELECT NULL, 'SP_ENSURE_TARGET_TABLE', :p_object_id || '@' || :p_integration_id, NULL, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
