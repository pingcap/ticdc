#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR="$OUT_DIR/$TEST_NAME"
CDC_BINARY=cdc.test
SINK_TYPE="$1"

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"

	start_tidb_cluster --workdir "$WORK_DIR"

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME"

	SINK_URI="mysql://normal:123456@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"
	cdc_cli_changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"

	run_sql_file "$CUR/data/test.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	check_table_exists target_db.finish_mark_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 90
	check_sync_diff "$WORK_DIR" "$CUR/conf/diff_config.toml" 120

	check_table_not_exists source_db.users "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_db.orders "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.external_users "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.external_users_from_default "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.users_view_from_default "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.orders_column_view_from_default "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.partitioned_events_like_from_default "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists target_db.temp_table_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists target_db.cross_move_source_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists target_db.multi_rename_a_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists target_db.multi_rename_b_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists target_db.to_be_dropped_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql "SHOW CREATE VIEW target_db.user_order_view_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "user_order_view_routed"
	check_contains "users_routed"
	check_contains "orders_routed"
	run_sql "SHOW CREATE VIEW target_extra_db.users_view_from_default_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "users_view_from_default_routed"
	check_contains "target_db"
	check_contains "users_routed"
	run_sql "SHOW CREATE VIEW target_extra_db.orders_column_view_from_default_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "orders_column_view_from_default_routed"
	check_contains 'target_db`.`orders_routed`.`id'
	check_contains 'FROM `target_db`.`orders_routed`'
	check_table_not_exists source_extra_db.correlated_users_view "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql "SHOW CREATE VIEW target_extra_db.correlated_users_view_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains 'target_db`.`orders_routed`.`user_id'
	check_contains 'target_db`.`users_routed`.`id'
	# Users 2 and 4 have no orders: the result also detects a lost correlation.
	run_sql "SELECT GROUP_CONCAT(id ORDER BY id) AS matched_ids FROM source_extra_db.correlated_users_view" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_contains 'matched_ids: 1,3'
	run_sql "SELECT GROUP_CONCAT(id ORDER BY id) AS matched_ids FROM target_extra_db.correlated_users_view_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains 'matched_ids: 1,3'
	check_table_not_exists target_db.transient_view_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"

	run_sql "DROP DATABASE source_extra_db" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "DROP DATABASE source_db" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_db_not_exists target_extra_db "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 90
	check_db_not_exists target_db "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 90

	cleanup_process "$CDC_BINARY"
}

trap 'stop_test "$WORK_DIR"' EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
