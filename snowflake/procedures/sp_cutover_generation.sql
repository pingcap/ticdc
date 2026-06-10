CREATE OR REPLACE PROCEDURE TICDC_META.SP_CUTOVER_GENERATION(
  p_integration_id STRING,
  p_generation STRING
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_current_active_generation STRING DEFAULT NULL;
  v_shadow_generation STRING DEFAULT NULL;
  v_shadow_bootstrap_ts NUMBER(20, 0) DEFAULT 0;
  v_object_id STRING DEFAULT NULL;
  v_serving_base_table STRING DEFAULT NULL;
  v_target_base_table STRING DEFAULT NULL;
  v_sql STRING DEFAULT NULL;
  v_swapped NUMBER(20, 0) DEFAULT 0;
  c_tables CURSOR FOR
    SELECT object_id, serving_base_table, target_base_table
      FROM TICDC_META.OBJECT_REGISTRY
     WHERE integration_id = ?
       AND generation = ?
       AND COALESCE(is_enabled, TRUE)
     ORDER BY source_db, source_table, object_id;
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

  SELECT
    MAX(active_generation),
    MAX(shadow_generation),
    COALESCE(MAX(shadow_bootstrap_ts), 0)
    INTO :v_current_active_generation, :v_shadow_generation, :v_shadow_bootstrap_ts
    FROM TICDC_META.INTEGRATION_REGISTRY
   WHERE integration_id = :p_integration_id;

  IF (v_shadow_generation IS NOT NULL AND v_shadow_generation <> p_generation) THEN
    RETURN OBJECT_CONSTRUCT(
      'status',
      'skipped',
      'procedure',
      'SP_CUTOVER_GENERATION',
      'integration_id',
      p_integration_id,
      'generation',
      p_generation,
      'reason',
      'requested generation does not match shadow generation'
    );
  END IF;

  OPEN c_tables USING (p_integration_id, p_generation);
  FOR rec IN c_tables DO
    v_object_id := rec.object_id;
    v_serving_base_table := rec.serving_base_table;
    v_target_base_table := rec.target_base_table;

    IF (v_serving_base_table IS NOT NULL AND v_target_base_table IS NOT NULL AND v_serving_base_table <> v_target_base_table) THEN
      v_sql := 'ALTER TABLE ' || v_serving_base_table || ' SWAP WITH ' || v_target_base_table;
      EXECUTE IMMEDIATE v_sql;

      UPDATE TICDC_META.OBJECT_REGISTRY
         SET target_base_table = serving_base_table,
             cutover_state = 'ACTIVE',
             is_active_generation = TRUE,
             materialization_status = 'ACTIVE',
             updated_at = CURRENT_TIMESTAMP()
       WHERE integration_id = :p_integration_id
         AND object_id = :v_object_id
         AND generation = :p_generation;

      UPDATE TICDC_META.TABLE_SYNC_STATE
         SET target_base_table = :v_serving_base_table,
             is_active_generation = TRUE,
             updated_at = CURRENT_TIMESTAMP()
       WHERE integration_id = :p_integration_id
         AND object_id = :v_object_id
         AND generation = :p_generation;

      UPDATE TICDC_META.OBJECT_REGISTRY
         SET cutover_state = 'REPLACED',
             is_active_generation = FALSE,
             updated_at = CURRENT_TIMESTAMP()
       WHERE integration_id = :p_integration_id
         AND object_id = :v_object_id
         AND generation <> :p_generation
         AND COALESCE(is_active_generation, FALSE);

      UPDATE TICDC_META.TABLE_SYNC_STATE
         SET is_active_generation = FALSE,
             updated_at = CURRENT_TIMESTAMP()
       WHERE integration_id = :p_integration_id
         AND object_id = :v_object_id
         AND generation <> :p_generation
         AND COALESCE(is_active_generation, FALSE);

      v_sql := 'DROP TABLE IF EXISTS ' || v_target_base_table;
      EXECUTE IMMEDIATE v_sql;
    END IF;

    v_swapped := v_swapped + 1;
  END FOR;
  CLOSE c_tables;

  UPDATE TICDC_META.INTEGRATION_REGISTRY
     SET active_generation = :p_generation,
         active_bootstrap_ts = :v_shadow_bootstrap_ts,
         shadow_generation = NULL,
         shadow_bootstrap_ts = NULL,
         cutover_state = 'IDLE',
         updated_at = CURRENT_TIMESTAMP()
   WHERE integration_id = :p_integration_id;

  RETURN OBJECT_CONSTRUCT(
    'status',
    'ok',
    'procedure',
    'SP_CUTOVER_GENERATION',
    'integration_id',
    p_integration_id,
    'generation',
    p_generation,
    'previous_active_generation',
    v_current_active_generation,
    'swapped',
    v_swapped
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
    SELECT NULL, 'SP_CUTOVER_GENERATION', :p_integration_id || '@' || :p_generation, NULL, :SQLSTATE, :SQLCODE, :SQLERRM, LAST_QUERY_ID(), CURRENT_TIMESTAMP();
    RAISE;
END;
$$;
