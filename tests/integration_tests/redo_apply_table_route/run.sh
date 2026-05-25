#!/bin/bash

# [DESCRIPTION]:
#   This test verifies that redo log replay preserves table route target names.
#   It first runs a table_route-style SQL workload through normal replication,
#   then blocks MySQL DML writes, writes more source DML, and applies redo logs
#   to confirm the replayed rows still land in routed downstream tables.

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"

WORK_DIR="$OUT_DIR/$TEST_NAME"
CDC_BINARY=cdc.test
SINK_TYPE="$1"

REDO_DIR="/tmp/tidb_cdc_test/redo_apply_table_route/redo"
SQL_RES_FILE="$OUT_DIR/$TEST_NAME/sql_res.$TEST_NAME.log"

function cleanup_redo_dir() {
	rm -rf "$REDO_DIR"
	mkdir -p "$REDO_DIR"
}

function get_sql_count() {
	grep -oE 'cnt:[[:space:]]*[0-9]+' "$SQL_RES_FILE" | grep -oE '[0-9]+' || echo "0"
}

function query_count() {
	local sql="$1"
	local host="$2"
	local port="$3"

	run_sql "$sql" "$host" "$port" >/dev/null
	get_sql_count
}

function wait_query_count() {
	local sql="$1"
	local host="$2"
	local port="$3"
	local expected="$4"
	local retries="${5:-30}"

	while [ "$retries" -gt 0 ]; do
		local actual
		actual=$(query_count "$sql" "$host" "$port")
		if [ "$actual" = "$expected" ]; then
			return 0
		fi
		retries=$((retries - 1))
		sleep 1
	done

	echo "ERROR: timeout waiting for expected count ($expected) for query: $sql"
	return 1
}

function require_equal() {
	local actual="$1"
	local expected="$2"
	local message="$3"

	if [ "$actual" != "$expected" ]; then
		echo "ERROR: $message, expected $expected, got $actual"
		exit 1
	fi
}

function verify_normal_table_route() {
	check_table_exists target_db.finish_mark_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 90

	local source_users_count
	local target_users_count
	local source_extra_count
	local target_extra_count
	source_users_count=$(query_count "SELECT COUNT(*) AS cnt FROM source_db.users;" "$UP_TIDB_HOST" "$UP_TIDB_PORT")
	source_extra_count=$(query_count "SELECT COUNT(*) AS cnt FROM source_extra_db.external_users;" "$UP_TIDB_HOST" "$UP_TIDB_PORT")

	wait_query_count "SELECT COUNT(*) AS cnt FROM target_db.users_routed;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" "$source_users_count" 90
	wait_query_count "SELECT COUNT(*) AS cnt FROM target_extra_db.external_users_routed;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" "$source_extra_count" 90

	target_users_count=$(query_count "SELECT COUNT(*) AS cnt FROM target_db.users_routed;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT")
	target_extra_count=$(query_count "SELECT COUNT(*) AS cnt FROM target_extra_db.external_users_routed;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT")

	require_equal "$target_users_count" "$source_users_count" "normal table route users count mismatch"
	require_equal "$target_extra_count" "$source_extra_count" "normal table route external_users count mismatch"

	check_table_not_exists source_db.users "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.external_users "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists target_db.temp_table_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists target_db.to_be_dropped_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"

	run_sql "SHOW CREATE VIEW target_db.user_order_view_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "user_order_view_routed"
	check_contains "users_routed"
	check_contains "orders_routed"
}

function write_redo_only_dml() {
	run_sql "INSERT INTO source_db.users VALUES (100, 'redo_user', 'redo_user@example.com');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "UPDATE source_db.users SET email = 'redo_alice@example.com' WHERE id = 1;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "INSERT INTO source_db.orders VALUES (100, 100, 1000.00);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "INSERT INTO source_extra_db.external_users VALUES (100, 'redo_external', 'redo_external@example.com');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "UPDATE source_extra_db.external_users SET email = 'redo_external_alice@example.com' WHERE id = 1;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "INSERT INTO source_db.finish_mark VALUES (2);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
}

function verify_redo_apply_route() {
	local source_users_count
	local target_users_count
	local source_orders_count
	local target_orders_count
	local source_extra_count
	local target_extra_count
	local source_finish_count
	local target_finish_count

	source_users_count=$(query_count "SELECT COUNT(*) AS cnt FROM source_db.users;" "$UP_TIDB_HOST" "$UP_TIDB_PORT")
	target_users_count=$(query_count "SELECT COUNT(*) AS cnt FROM target_db.users_routed;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT")
	source_orders_count=$(query_count "SELECT COUNT(*) AS cnt FROM source_db.orders;" "$UP_TIDB_HOST" "$UP_TIDB_PORT")
	target_orders_count=$(query_count "SELECT COUNT(*) AS cnt FROM target_db.orders_routed;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT")
	source_extra_count=$(query_count "SELECT COUNT(*) AS cnt FROM source_extra_db.external_users;" "$UP_TIDB_HOST" "$UP_TIDB_PORT")
	target_extra_count=$(query_count "SELECT COUNT(*) AS cnt FROM target_extra_db.external_users_routed;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT")
	source_finish_count=$(query_count "SELECT COUNT(*) AS cnt FROM source_db.finish_mark;" "$UP_TIDB_HOST" "$UP_TIDB_PORT")
	target_finish_count=$(query_count "SELECT COUNT(*) AS cnt FROM target_db.finish_mark_routed;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT")

	require_equal "$target_users_count" "$source_users_count" "redo apply users count mismatch"
	require_equal "$target_orders_count" "$source_orders_count" "redo apply orders count mismatch"
	require_equal "$target_extra_count" "$source_extra_count" "redo apply external_users count mismatch"
	require_equal "$target_finish_count" "$source_finish_count" "redo apply finish_mark count mismatch"

	run_sql "SELECT email AS routed_email FROM target_db.users_routed WHERE id = 1;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "redo_alice@example.com"
	run_sql "SELECT name AS routed_name FROM target_db.users_routed WHERE id = 100;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "redo_user"
	run_sql "SELECT email AS routed_email FROM target_extra_db.external_users_routed WHERE id = 1;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "redo_external_alice@example.com"
	run_sql "SELECT name AS routed_name FROM target_extra_db.external_users_routed WHERE id = 100;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "redo_external"

	check_table_not_exists source_db.users "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.external_users "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
}

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		echo "skip redo apply table route test for non-mysql sink"
		return
	fi

	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	cleanup_redo_dir

	start_tidb_cluster --workdir "$WORK_DIR"

	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME"

	local sink_uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"
	local changefeed_id="redo-table-route-test"
	cdc_cli_changefeed create \
		--start-ts="$start_ts" \
		--sink-uri="$sink_uri" \
		--changefeed-id="$changefeed_id" \
		--config="$CUR/conf/changefeed.toml"

	run_sql_file "$CUR/data/test.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	verify_normal_table_route

	local pre_redo_users_count
	pre_redo_users_count=$(query_count "SELECT COUNT(*) AS cnt FROM target_db.users_routed;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT")

	cleanup_process "$CDC_BINARY"
	export GO_FAILPOINTS='github.com/pingcap/ticdc/pkg/sink/mysql/MySQLSinkHangLongTime=return(true)'
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME"

	write_redo_only_dml

	local storage_path="file://$REDO_DIR"
	local tmp_download_path="$WORK_DIR/cdc_data/redo/$changefeed_id"
	local current_tso
	current_tso=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	ensure 50 check_redo_resolved_ts "$changefeed_id" "$current_tso" "$storage_path" "$tmp_download_path/meta"

	cleanup_process "$CDC_BINARY"
	export GO_FAILPOINTS=''

	local target_users_before_redo
	target_users_before_redo=$(query_count "SELECT COUNT(*) AS cnt FROM target_db.users_routed;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT")
	require_equal "$target_users_before_redo" "$pre_redo_users_count" "failpoint did not block MySQL DML before redo apply"

	"$CDC_BINARY" redo apply \
		--tmp-dir="$tmp_download_path/apply" \
		--storage="$storage_path" \
		--sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/?safe-mode=true" \
		2>&1 | tee "$WORK_DIR/redo_apply.log" || {
		echo "Redo apply failed"
		cat "$WORK_DIR/redo_apply.log"
		exit 1
	}

	verify_redo_apply_route

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME"
	run_sql "INSERT INTO source_db.users VALUES (101, 'after_redo', 'after_redo@example.com');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	wait_query_count "SELECT COUNT(*) AS cnt FROM target_db.users_routed WHERE id = 101 AND name = 'after_redo';" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" "1" 30

	cleanup_process "$CDC_BINARY"
}

trap 'stop_test "$WORK_DIR"' EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
