#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR="$OUT_DIR/$TEST_NAME"
CDC_BINARY=cdc.test
SINK_TYPE="$1"
MAX_RETRIES=20

function write_route_config() {
	local config_file=$1
	local source_a=$2
	local source_b=$3
	local target_schema=$4

	cat >"$config_file" <<EOF
[filter]
rules = ['$source_a.*', '$source_b.*']

[sink]
[[sink.dispatchers]]
matcher = ['$source_a.*']
target-schema = '$target_schema'
target-table = '{table}_routed'

[[sink.dispatchers]]
matcher = ['$source_b.*']
target-schema = '$target_schema'
target-table = '{table}_routed'
EOF
}

function assert_changefeed_create_conflict() {
	local changefeed_id=$1
	local config_file=$2
	local create_output
	local create_ret

	set +e
	create_output=$(cdc_cli_changefeed create -c "$changefeed_id" --sink-uri="$SINK_URI" --config="$config_file" 2>&1)
	create_ret=$?
	set -e
	echo "$create_output"

	if [ "$create_ret" -eq 0 ]; then
		echo "changefeed $changefeed_id was created successfully, expected table route conflict"
		exit 1
	fi
	if [[ "$create_output" != *"ErrTableRouteConflict"* && "$create_output" != *"table route conflict"* ]]; then
		echo "changefeed $changefeed_id failed with unexpected output"
		exit 1
	fi
}

function run_static_conflict_case() {
	local source_a=route_conflict_static_a
	local source_b=route_conflict_static_b
	local target_schema=target_route_conflict_static
	local changefeed_id=route-conflict-static
	local config_file="$WORK_DIR/$changefeed_id.toml"

	echo "[$(date)] start static table route conflict case"
	write_route_config "$config_file" "$source_a" "$source_b" "$target_schema"
	run_sql "CREATE DATABASE $source_a; CREATE TABLE $source_a.t (id INT PRIMARY KEY);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "CREATE DATABASE $source_b; CREATE TABLE $source_b.t (id INT PRIMARY KEY);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	assert_changefeed_create_conflict "$changefeed_id" "$config_file"
	echo "[$(date)] finish static table route conflict case"
}

function run_create_table_conflict_case() {
	local source_a=route_conflict_create_a
	local source_b=route_conflict_create_b
	local target_schema=target_route_conflict_create
	local changefeed_id=route-conflict-create
	local config_file="$WORK_DIR/$changefeed_id.toml"
	local start_ts

	echo "[$(date)] start create table conflict case"
	write_route_config "$config_file" "$source_a" "$source_b" "$target_schema"
	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	run_sql "CREATE DATABASE $source_a; CREATE TABLE $source_a.t (id INT PRIMARY KEY);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	cdc_cli_changefeed create -c "$changefeed_id" --start-ts="$start_ts" --sink-uri="$SINK_URI" --config="$config_file"
	check_table_exists "$target_schema.t_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 60

	run_sql "CREATE DATABASE $source_b; CREATE TABLE $source_b.t (id INT PRIMARY KEY);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} "$changefeed_id" failed conflict ""
	cdc_cli_changefeed remove -c "$changefeed_id" || true
	echo "[$(date)] finish create table conflict case"
}

function run_multi_rename_table_conflict_case() {
	local source_a=route_conflict_multi_rename_a
	local source_b=route_conflict_multi_rename_b
	local target_schema=target_route_conflict_multi_rename
	local changefeed_id=route-conflict-multi-rename
	local config_file="$WORK_DIR/$changefeed_id.toml"
	local start_ts

	echo "[$(date)] start multi-rename table conflict case"
	write_route_config "$config_file" "$source_a" "$source_b" "$target_schema"
	run_sql "CREATE DATABASE $source_a; CREATE TABLE $source_a.t (id INT PRIMARY KEY);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "CREATE DATABASE $source_b; CREATE TABLE $source_b.tmp (id INT PRIMARY KEY); CREATE TABLE $source_b.other_tmp (id INT PRIMARY KEY);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	cdc_cli_changefeed create -c "$changefeed_id" --start-ts="$start_ts" --sink-uri="$SINK_URI" --config="$config_file"
	check_table_exists "$target_schema.t_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 60
	check_table_exists "$target_schema.tmp_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 60
	check_table_exists "$target_schema.other_tmp_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 60

	run_sql "RENAME TABLE $source_b.tmp TO $source_b.t, $source_b.other_tmp TO $source_b.other_new;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} "$changefeed_id" failed conflict ""
	cdc_cli_changefeed remove -c "$changefeed_id" || true
	echo "[$(date)] finish multi-rename table conflict case"
}

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		echo "table_route_conflict_detection only supports mysql sink"
		return
	fi

	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	start_tidb_cluster --workdir "$WORK_DIR"
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME"
	SINK_URI="mysql://normal:123456@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"

	run_static_conflict_case
	run_create_table_conflict_case
	run_multi_rename_table_conflict_case

	cleanup_process "$CDC_BINARY"
}

trap 'stop_test "$WORK_DIR"' EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
