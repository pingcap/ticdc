#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR="$OUT_DIR/$TEST_NAME"
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf "$WORK_DIR"
	mkdir -p "$WORK_DIR"
	start_tidb_cluster --workdir "$WORK_DIR"

	local source_db=rename_table_start_ts_source
	local target_db=rename_table_start_ts_target
	local table_name=t
	local changefeed_id=rename-table-start-ts
	local gc_worker_key
	local gc_worker_value
	local pd_cluster_id
	local rename_finished_ts
	local start_ts
	local table_id

	run_sql "CREATE DATABASE $source_db; CREATE DATABASE $target_db; CREATE TABLE $source_db.$table_name (id INT PRIMARY KEY); INSERT INTO $source_db.$table_name VALUES (1);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "CREATE DATABASE $source_db; CREATE DATABASE $target_db; CREATE TABLE $source_db.$table_name (id INT PRIMARY KEY); INSERT INTO $source_db.$table_name VALUES (1);" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"

	table_id=$(get_table_id "$source_db" "$table_name")
	run_sql "RENAME TABLE $source_db.$table_name TO $target_db.$table_name;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --logsuffix _probe
	ensure 30 "grep 'write ddl event' '$WORK_DIR/cdc_probe.log' | grep 'tableID=$table_id' | grep -q 'RENAME TABLE'"
	rename_finished_ts=$(grep "write ddl event" "$WORK_DIR/cdc_probe.log" |
		grep "tableID=$table_id" |
		grep "RENAME TABLE" |
		head -n 1 |
		grep -oE 'finishedTs=[0-9]+' |
		cut -d= -f2)
	if ! [[ "$rename_finished_ts" =~ ^[0-9]+$ ]]; then
		echo "failed to get rename table finishedTs"
		exit 1
	fi
	start_ts=$((rename_finished_ts - 1))
	cleanup_process "$CDC_BINARY"

	# Force SchemaStore to initialize from the same snapshot used by the
	# changefeed. Restore the GC worker safepoint before creating the changefeed
	# so its start-ts is still considered readable.
	pd_cluster_id=$(curl -s "http://$UP_PD_HOST_1:$UP_PD_PORT_1/pd/api/v1/cluster" |
		grep -oE '"id":[[:space:]]*[0-9]+' |
		grep -oE '[0-9]+')
	gc_worker_key="/pd/$pd_cluster_id/gc/safe_point/service/gc_worker"
	gc_worker_value=$(curl -fsS "http://$UP_PD_HOST_1:$UP_PD_PORT_1/pd/api/v1/gc/safepoint" |
		jq -cer '.service_gc_safe_points[] | select(.service_id == "gc_worker")')
	if [ -z "$gc_worker_value" ]; then
		echo "failed to get gc_worker service safepoint"
		exit 1
	fi
	GO111MODULE=on go run "$CUR/set_gc_safepoint.go" "$UP_PD_HOST_1:$UP_PD_PORT_1" "$gc_worker_key" \
		"{\"service_id\":\"gc_worker\",\"expired_at\":9223372036854775807,\"safe_point\":$start_ts}"

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"
	ensure 30 "grep 'schema store initialized' '$WORK_DIR/cdc.log' | grep -q 'resolvedTs=$start_ts'"
	GO111MODULE=on go run "$CUR/set_gc_safepoint.go" "$UP_PD_HOST_1:$UP_PD_PORT_1" "$gc_worker_key" "$gc_worker_value"

	cdc_cli_changefeed create -c "$changefeed_id" --start-ts="$start_ts" \
		--sink-uri="mysql://normal:123456@$DOWN_TIDB_HOST:$DOWN_TIDB_PORT/" \
		--config="$CUR/conf/changefeed.toml"

	# The event filter matches the table name before the rename, so the downstream
	# table must keep its original name.
	run_sql "CREATE TABLE $source_db.finish_mark (id INT PRIMARY KEY);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists "$source_db.finish_mark" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 60
	check_table_exists "$source_db.$table_name" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 60
	check_table_not_exists "$target_db.$table_name" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 60
	ensure 30 "run_sql 'SELECT id FROM $source_db.$table_name;' '$DOWN_TIDB_HOST' '$DOWN_TIDB_PORT' && check_contains 'id: 1'"
	check_changefeed_state "http://$UP_PD_HOST_1:$UP_PD_PORT_1" "$changefeed_id" "normal" "null" ""

	cleanup_process "$CDC_BINARY"
}

trap 'stop_test "$WORK_DIR"' EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
