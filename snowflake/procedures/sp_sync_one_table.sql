CREATE OR REPLACE PROCEDURE TICDC_META.SP_SYNC_ONE_TABLE(
  p_integration_id STRING,
  p_object_id STRING,
  p_upper_ts NUMBER(20, 0)
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_target_base_table STRING DEFAULT NULL;
  v_change_external_table STRING DEFAULT NULL;
  v_generation STRING DEFAULT NULL;
  v_bootstrap_ts NUMBER(20, 0) DEFAULT 0;
  v_last_watermark NUMBER(20, 0) DEFAULT 0;
  v_table_version NUMBER(20, 0) DEFAULT 0;
  v_insert_columns STRING DEFAULT '';
  v_insert_values STRING DEFAULT '';
  v_update_assignments STRING DEFAULT '';
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

  CALL TICDC_META.SP_ENSURE_RAW_CHANGE_TABLE(:p_integration_id, :p_object_id);
  CALL TICDC_META.SP_ENSURE_TARGET_TABLE(:p_integration_id, :p_object_id);

  SELECT
    MAX(target_base_table),
    MAX(change_external_table),
    COALESCE(MAX(table_version), 0),
    COALESCE(MAX(bootstrap_ts), 0),
    MAX(generation)
    INTO :v_target_base_table, :v_change_external_table, :v_table_version, :v_bootstrap_ts, :v_generation
    FROM TICDC_META.OBJECT_REGISTRY
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id;

  IF (v_change_external_table IS NULL OR v_change_external_table = '') THEN
    RETURN OBJECT_CONSTRUCT(
      'status',
      'skipped',
      'procedure',
      'SP_SYNC_ONE_TABLE',
      'integration_id',
      p_integration_id,
      'object_id',
      p_object_id,
      'upper_ts',
      p_upper_ts,
      'reason',
      'change external table is not registered'
    );
  END IF;

  SELECT COALESCE(MAX(last_applied_commit_ts), :v_bootstrap_ts, 0)
    INTO :v_last_watermark
    FROM TICDC_META.TABLE_SYNC_STATE
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id;

  IF (p_upper_ts <= v_last_watermark) THEN
    RETURN OBJECT_CONSTRUCT(
      'status',
      'ok',
      'procedure',
      'SP_SYNC_ONE_TABLE',
      'integration_id',
      p_integration_id,
      'object_id',
      p_object_id,
      'upper_ts',
      p_upper_ts,
      'last_watermark',
      v_last_watermark,
      'applied',
      0
    );
  END IF;

  SELECT COALESCE(
      LISTAGG('"' || REPLACE(COALESCE(target_column_name, column_name), '"', '""') || '"', ', ') WITHIN GROUP (ORDER BY ordinal_position),
      ''
    ),
    COALESCE(
      LISTAGG('s."' || REPLACE(COALESCE(target_column_name, column_name), '"', '""') || '"', ', ') WITHIN GROUP (ORDER BY ordinal_position),
      ''
    ),
    COALESCE(
      LISTAGG(
        '"' || REPLACE(COALESCE(target_column_name, column_name), '"', '""') || '" = s."' ||
        REPLACE(COALESCE(target_column_name, column_name), '"', '""') || '"',
        ', '
      ) WITHIN GROUP (ORDER BY ordinal_position),
      ''
    )
    INTO :v_insert_columns, :v_insert_values, :v_update_assignments
    FROM TICDC_META.COLUMN_REGISTRY
   WHERE integration_id = :p_integration_id
     AND object_id = :p_object_id
     AND COALESCE(is_deleted, FALSE) = FALSE;

  v_sql := 'DELETE FROM ' || v_target_base_table || ' t USING (' ||
    'SELECT DISTINCT COALESCE("_tidb_old_row_identity", "_tidb_row_identity") AS identity_key ' ||
    'FROM ' || v_change_external_table || ' ' ||
    'WHERE "_tidb_commit_ts" > :1 AND "_tidb_commit_ts" <= :2 ' ||
    'AND ("_tidb_op" = ''D'' OR NVL("_tidb_old_row_identity", "_tidb_row_identity") <> "_tidb_row_identity")' ||
    ') s WHERE t.__ticdc_row_identity = s.identity_key';
  EXECUTE IMMEDIATE v_sql USING (v_last_watermark, p_upper_ts);

  v_sql := 'MERGE INTO ' || v_target_base_table || ' t USING (' ||
    'SELECT * FROM (' ||
    'SELECT r.*, ROW_NUMBER() OVER (PARTITION BY "_tidb_row_identity" ORDER BY "_tidb_commit_ts" DESC) AS rn ' ||
    'FROM ' || v_change_external_table || ' r ' ||
    'WHERE r."_tidb_commit_ts" > :1 AND r."_tidb_commit_ts" <= :2 AND r."_tidb_op" IN (''I'', ''U'')' ||
    ') WHERE rn = 1' ||
    ') s ON t.__ticdc_row_identity = s."_tidb_row_identity" ' ||
    'WHEN MATCHED THEN UPDATE SET ' ||
    '__ticdc_last_commit_ts = s."_tidb_commit_ts", ' ||
    '__ticdc_last_commit_time = s."_tidb_commit_time", ' ||
    '__ticdc_table_version = :3' ||
    IFF(v_update_assignments = '', '', ', ' || v_update_assignments) ||
    ' WHEN NOT MATCHED THEN INSERT (__ticdc_row_identity, __ticdc_last_commit_ts, __ticdc_last_commit_time, __ticdc_table_version' ||
    IFF(v_insert_columns = '', '', ', ' || v_insert_columns) ||
    ') VALUES (s."_tidb_row_identity", s."_tidb_commit_ts", s."_tidb_commit_time", :3' ||
    IFF(v_insert_values = '', '', ', ' || v_insert_values) ||
    ')';
  EXECUTE IMMEDIATE v_sql USING (v_last_watermark, p_upper_ts, v_table_version);

  MERGE INTO TICDC_META.TABLE_SYNC_STATE t
  USING (
    SELECT
      :p_integration_id AS integration_id,
      :p_object_id AS object_id,
      :v_generation AS generation,
      :v_bootstrap_ts AS bootstrap_ts,
      :v_table_version AS table_version,
      :p_upper_ts AS last_applied_commit_ts
  ) s
  ON t.integration_id = s.integration_id AND t.object_id = s.object_id
  WHEN MATCHED THEN
    UPDATE SET
      generation = COALESCE(s.generation, t.generation),
      bootstrap_ts = COALESCE(s.bootstrap_ts, t.bootstrap_ts),
      table_version = s.table_version,
      last_applied_commit_ts = GREATEST(COALESCE(t.last_applied_commit_ts, 0), s.last_applied_commit_ts),
      last_apply_time = CURRENT_TIMESTAMP(),
      status = 'INCREMENTAL_LOADED',
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
      'INCREMENTAL_LOADED',
      CURRENT_TIMESTAMP()
    );

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_SYNC_ONE_TABLE',
    'integration_id',
    p_integration_id,
    'object_id',
    p_object_id,
    'upper_ts',
    p_upper_ts,
    'last_watermark',
    v_last_watermark
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
    SELECT NULL, 'SP_SYNC_ONE_TABLE', :p_object_id || '@' || :p_integration_id, :p_upper_ts, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
