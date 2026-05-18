CREATE OR REPLACE PROCEDURE TICDC_META.SP_ENSURE_TARGET_TABLE(
  p_integration_id STRING,
  p_object_id STRING,
  p_generation STRING,
  p_bind_serving_view BOOLEAN
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_safe_object_id STRING DEFAULT NULL;
  v_safe_generation STRING DEFAULT NULL;
  v_target_base_table STRING DEFAULT NULL;
  v_serving_base_table STRING DEFAULT NULL;
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

  ALTER TABLE IF EXISTS TICDC_META.OBJECT_REGISTRY ADD COLUMN IF NOT EXISTS serving_base_table STRING;
  ALTER TABLE IF EXISTS TICDC_META.OBJECT_REGISTRY ADD COLUMN IF NOT EXISTS cutover_state STRING;
  ALTER TABLE IF EXISTS TICDC_META.OBJECT_REGISTRY ADD COLUMN IF NOT EXISTS is_active_generation BOOLEAN;
  ALTER TABLE IF EXISTS TICDC_META.COLUMN_REGISTRY ADD COLUMN IF NOT EXISTS generation STRING;

  v_safe_object_id := REGEXP_REPLACE(p_object_id, '[^A-Za-z0-9_]', '_');
  v_safe_generation := UPPER(REGEXP_REPLACE(p_generation, '[^A-Za-z0-9_]', '_'));

  SELECT
    COALESCE(
      MAX(target_base_table),
      'TICDC_REPLICA.PUBLIC.' || :v_safe_object_id || '__BASE__G_' || :v_safe_generation
    ),
    COALESCE(
      MAX(serving_base_table),
      'TICDC_REPLICA.PUBLIC.' || :v_safe_object_id || '__BASE'
    ),
    COALESCE(
      MAX(target_database || '.' || target_schema || '.' || target_table),
      'TICDC_REPLICA.PUBLIC.' || :v_safe_object_id
    )
    INTO :v_target_base_table, :v_serving_base_table, :v_target_view
    FROM TICDC_META.OBJECT_REGISTRY
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id
     AND generation = :p_generation;

  MERGE INTO TICDC_META.OBJECT_REGISTRY t
  USING (
    SELECT
      :p_integration_id AS integration_id,
      :p_object_id AS object_id,
      :p_generation AS generation,
      :v_target_base_table AS target_base_table,
      :v_serving_base_table AS serving_base_table,
      :v_target_view AS target_table,
      IFF(:p_bind_serving_view, 'ACTIVE', 'SHADOW_READY') AS cutover_state,
      IFF(:p_bind_serving_view, TRUE, FALSE) AS is_active_generation
  ) s
  ON t.integration_id = s.integration_id AND t.object_id = s.object_id AND t.generation = s.generation
  WHEN MATCHED THEN
    UPDATE SET
      target_base_table = COALESCE(t.target_base_table, s.target_base_table),
      serving_base_table = COALESCE(t.serving_base_table, s.serving_base_table),
      target_table = COALESCE(t.target_table, s.target_table),
      cutover_state = COALESCE(t.cutover_state, s.cutover_state),
      is_active_generation = COALESCE(t.is_active_generation, s.is_active_generation),
      materialization_status = COALESCE(t.materialization_status, 'ACTIVE'),
      is_enabled = COALESCE(t.is_enabled, TRUE),
      updated_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (
      integration_id,
      object_id,
      generation,
      target_base_table,
      serving_base_table,
      target_table,
      cutover_state,
      materialization_status,
      is_active_generation,
      is_enabled,
      created_at,
      updated_at
    )
    VALUES (
      s.integration_id,
      s.object_id,
      s.generation,
      s.target_base_table,
      s.serving_base_table,
      s.target_table,
      s.cutover_state,
      'ACTIVE',
      s.is_active_generation,
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
     AND generation = :p_generation
     AND COALESCE(is_deleted, FALSE) = FALSE;

  v_sql := 'CREATE TABLE IF NOT EXISTS ' || v_serving_base_table ||
    ' (__ticdc_row_identity VARCHAR NOT NULL, __ticdc_last_commit_ts NUMBER(20,0) NOT NULL, __ticdc_last_commit_time TIMESTAMP_NTZ(6), __ticdc_table_version NUMBER(20,0)' ||
    IFF(v_business_columns = '', '', ', ' || v_business_columns) ||
    ')';
  EXECUTE IMMEDIATE v_sql;

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
     AND generation = :p_generation
     AND COALESCE(is_deleted, FALSE) = FALSE;

  IF (p_bind_serving_view) THEN
    v_sql := 'CREATE OR REPLACE VIEW ' || v_target_view || ' AS SELECT ' || v_view_columns || ' FROM ' || v_serving_base_table;
    EXECUTE IMMEDIATE v_sql;
  END IF;

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_ENSURE_TARGET_TABLE',
    'integration_id',
    p_integration_id,
    'object_id',
    p_object_id,
    'generation',
    p_generation,
    'target_base_table',
    v_target_base_table,
    'serving_base_table',
    v_serving_base_table,
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
