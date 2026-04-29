CREATE OR REPLACE PROCEDURE TICDC_META.SP_BOOTSTRAP_ONE_TABLE(
  p_integration_id STRING,
  p_object_id STRING,
  p_generation STRING,
  p_bootstrap_ts NUMBER(20, 0)
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_target_base_table STRING DEFAULT NULL;
  v_snapshot_external_table STRING DEFAULT NULL;
  v_table_version NUMBER(20, 0) DEFAULT 0;
  v_insert_columns STRING DEFAULT '';
  v_select_columns STRING DEFAULT '';
  v_identity_pairs STRING DEFAULT '';
  v_identity_expression STRING DEFAULT NULL;
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

  CALL TICDC_META.SP_ENSURE_TARGET_TABLE(:p_integration_id, :p_object_id);

  SELECT
    MAX(target_base_table),
    MAX(snapshot_external_table),
    COALESCE(MAX(table_version), 0)
    INTO :v_target_base_table, :v_snapshot_external_table, :v_table_version
    FROM TICDC_META.OBJECT_REGISTRY
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id;

  IF (v_snapshot_external_table IS NULL OR v_snapshot_external_table = '') THEN
    RETURN OBJECT_CONSTRUCT(
      'status',
      'skipped',
      'procedure',
      'SP_BOOTSTRAP_ONE_TABLE',
      'integration_id',
      p_integration_id,
      'object_id',
      p_object_id,
      'reason',
      'snapshot external table is not registered'
    );
  END IF;

  SELECT COALESCE(
      LISTAGG('"' || REPLACE(COALESCE(target_column_name, column_name), '"', '""') || '"', ', ') WITHIN GROUP (ORDER BY ordinal_position),
      ''
    ),
    COALESCE(
      LISTAGG('"' || REPLACE(COALESCE(target_column_name, column_name), '"', '""') || '"', ', ') WITHIN GROUP (ORDER BY ordinal_position),
      ''
    )
    INTO :v_insert_columns, :v_select_columns
    FROM TICDC_META.COLUMN_REGISTRY
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id
     AND COALESCE(is_deleted, FALSE) = FALSE;

  SELECT COALESCE(
      LISTAGG(
        '''' || REPLACE(COALESCE(target_column_name, column_name), '''', '''''') || ''', TO_VARCHAR("' ||
        REPLACE(COALESCE(target_column_name, column_name), '"', '""') || '")',
        ', '
      ) WITHIN GROUP (ORDER BY ordinal_position),
      ''
    )
    INTO :v_identity_pairs
    FROM TICDC_META.COLUMN_REGISTRY
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id
     AND COALESCE(is_deleted, FALSE) = FALSE
     AND COALESCE(is_handle_key, is_primary_key, FALSE);

  IF (v_identity_pairs IS NULL OR v_identity_pairs = '') THEN
    SELECT COALESCE(
        LISTAGG(
          '''' || REPLACE(COALESCE(target_column_name, column_name), '''', '''''') || ''', TO_VARCHAR("' ||
          REPLACE(COALESCE(target_column_name, column_name), '"', '""') || '")',
          ', '
        ) WITHIN GROUP (ORDER BY ordinal_position),
        ''
      )
      INTO :v_identity_pairs
      FROM TICDC_META.COLUMN_REGISTRY
     WHERE integration_id = :p_integration_id
       AND object_id = :p_object_id
       AND COALESCE(is_deleted, FALSE) = FALSE;
  END IF;

  IF (v_identity_pairs IS NULL OR v_identity_pairs = '') THEN
    v_identity_expression := 'SHA2(TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(*)), 256)';
  ELSE
    v_identity_expression := 'SHA2(TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL(' || v_identity_pairs || ')), 256)';
  END IF;

  v_sql := 'DELETE FROM ' || v_target_base_table;
  EXECUTE IMMEDIATE v_sql;

  v_sql := 'INSERT INTO ' || v_target_base_table ||
    ' (__ticdc_row_identity, __ticdc_last_commit_ts, __ticdc_last_commit_time, __ticdc_table_version' ||
    IFF(v_insert_columns = '', '', ', ' || v_insert_columns) ||
    ') SELECT ' || v_identity_expression || ', :1, CURRENT_TIMESTAMP(), :2' ||
    IFF(v_select_columns = '', '', ', ' || v_select_columns) ||
    ' FROM ' || v_snapshot_external_table;
  EXECUTE IMMEDIATE v_sql USING (p_bootstrap_ts, v_table_version);

  MERGE INTO TICDC_META.TABLE_SYNC_STATE t
  USING (
    SELECT
      :p_integration_id AS integration_id,
      :p_object_id AS object_id,
      :p_generation AS generation,
      :p_bootstrap_ts AS bootstrap_ts,
      :v_table_version AS table_version
  ) s
  ON t.integration_id = s.integration_id AND t.object_id = s.object_id
  WHEN MATCHED THEN
    UPDATE SET
      generation = s.generation,
      bootstrap_ts = s.bootstrap_ts,
      table_version = s.table_version,
      last_applied_commit_ts = s.bootstrap_ts,
      last_apply_time = CURRENT_TIMESTAMP(),
      status = 'FULL_LOADED',
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
      s.bootstrap_ts,
      CURRENT_TIMESTAMP(),
      'FULL_LOADED',
      CURRENT_TIMESTAMP()
    );

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_BOOTSTRAP_ONE_TABLE',
    'integration_id',
    p_integration_id,
    'object_id',
    p_object_id,
    'generation',
    p_generation,
    'bootstrap_ts',
    p_bootstrap_ts
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
    SELECT NULL, 'SP_BOOTSTRAP_ONE_TABLE', :p_object_id || '@' || :p_integration_id, :p_bootstrap_ts, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
