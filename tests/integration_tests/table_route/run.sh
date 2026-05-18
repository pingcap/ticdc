#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR="$OUT_DIR/$TEST_NAME"
CDC_BINARY=cdc.test
SINK_TYPE="$1"

function verify_table_route_result() {
	local work_dir=$1

	check_table_exists target_db.finish_mark_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 90
	check_sync_diff "$work_dir" "$CUR/conf/diff_config.toml" 120

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
	check_table_not_exists target_db.transient_view_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
}

function verify_table_route_drop_database() {
	run_sql "DROP DATABASE source_extra_db" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "DROP DATABASE source_db" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_db_not_exists target_extra_db "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 90
	check_db_not_exists target_db "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 90
}

function check_storage_files_use_target_names() {
	local storage_dir=$1

	ensure 60 test -d "$storage_dir/target_db/users_routed/meta"
	ensure 60 test -d "$storage_dir/target_extra_db/external_users_routed/meta"

	if [ -e "$storage_dir/source_db" ] || [ -e "$storage_dir/source_extra_db" ]; then
		echo "storage table route wrote source table directories:"
		find "$storage_dir" -maxdepth 2 -type d | sort
		exit 1
	fi
}

function run_mysql() {
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"

	start_tidb_cluster --workdir "$WORK_DIR"

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME"

	SINK_URI="mysql://normal:123456@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"
	cdc_cli_changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"

	run_sql_file "$CUR/data/test.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	verify_table_route_result "$WORK_DIR"
	verify_table_route_drop_database

	cleanup_process "$CDC_BINARY"
	check_logs "$WORK_DIR"
}

function run_storage_case() {
	local protocol=$1
	local work_dir="$WORK_DIR/$protocol"
	local storage_dir="$work_dir/storage_test"
	local sink_uri="file://$storage_dir?flush-interval=5s&protocol=$protocol"
	if [ "$protocol" = "canal-json" ]; then
		sink_uri="$sink_uri&enable-tidb-extension=true"
	fi

	rm -rf "$work_dir" && mkdir -p "$work_dir"

	start_tidb_cluster --workdir "$work_dir"

	run_cdc_server --workdir "$work_dir" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME"

	cdc_cli_changefeed create --sink-uri="$sink_uri" --config="$CUR/conf/changefeed.toml"

	run_sql_file "$CUR/data/test.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	run_storage_consumer "$work_dir" "$sink_uri" "$CUR/conf/changefeed.toml" "$protocol"

	verify_table_route_result "$work_dir"
	check_storage_files_use_target_names "$storage_dir"
	verify_table_route_drop_database

	stop_test "$work_dir"
	check_logs "$work_dir"
}

function run_storage() {
	run_storage_case canal-json
	run_storage_case csv
}

function run() {
	case "$SINK_TYPE" in
	mysql) run_mysql ;;
	storage) run_storage ;;
	*) return ;;
	esac
}

trap 'stop_test "$WORK_DIR"' EXIT
run "$@"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
