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

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"
	cdc_cli_changefeed create -c "$changefeed_id" --start-ts="$start_ts" \
		--sink-uri="mysql://normal:123456@$DOWN_TIDB_HOST:$DOWN_TIDB_PORT/" \
		--config="$CUR/conf/changefeed.toml"

	# The table is in the filter before the rename and outside it afterwards. The
	# rename DDL must still be replicated to downstream.
	run_sql "CREATE TABLE $source_db.finish_mark (id INT PRIMARY KEY);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists "$source_db.finish_mark" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 60
	check_table_not_exists "$source_db.$table_name" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 60
	check_table_exists "$target_db.$table_name" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 60
	ensure 30 "run_sql 'SELECT id FROM $target_db.$table_name;' '$DOWN_TIDB_HOST' '$DOWN_TIDB_PORT' && check_contains 'id: 1'"
	check_changefeed_state "http://$UP_PD_HOST_1:$UP_PD_PORT_1" "$changefeed_id" "normal" "null" ""

	cleanup_process "$CDC_BINARY"
}

trap 'stop_test "$WORK_DIR"' EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
