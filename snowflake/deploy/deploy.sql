-- Ordered SnowSQL deployment entry for TiCDC Snowflake procedures.

!source ../procedures/sp_discover_control_files.sql
!source ../procedures/sp_load_ddl_manifests.sql
!source ../procedures/sp_register_incremental_objects.sql
!source ../procedures/sp_register_snapshot_tables.sql
!source ../procedures/sp_bootstrap_one_table.sql
!source ../procedures/sp_bootstrap_all_tables.sql
!source ../procedures/sp_apply_ddl_up_to.sql
!source ../procedures/sp_apply_one_ddl.sql
!source ../procedures/sp_ensure_raw_change_table.sql
!source ../procedures/sp_ensure_target_table.sql
!source ../procedures/sp_sync_one_table.sql
!source ../procedures/sp_sync_all_tables.sql
!source ../procedures/sp_rebuild_one_table.sql
!source ../procedures/sp_process_rebuild_queue.sql
!source ../procedures/sp_orchestrate.sql
