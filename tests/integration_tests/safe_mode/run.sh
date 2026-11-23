#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=${CDC_BINARY:-cdc.test}
SINK_TYPE=$1

function check_rows() {
	local actual
	actual=$(mysql -uroot -h"$DOWN_TIDB_HOST" -P"$DOWN_TIDB_PORT" -N -B -e \
		"SELECT GROUP_CONCAT(CONCAT(id, ':', a) ORDER BY id) FROM safe_mode.t") || return 1
	if [ "$actual" != "1:hello2,2:world2,3:multi,4:multi" ]; then
		echo "Unexpected downstream rows: '$actual'"
		return 1
	fi
}
export -f check_rows

function run_case() {
	local filter_type=$1
	local batch_enable=$2
	local changefeed_id="safe-mode-$filter_type-$batch_enable"
	local config="$WORK_DIR/$changefeed_id.toml"
	echo "Testing safe mode: delete filter=$filter_type, batch-dml-enable=$batch_enable"

	run_sql_file "$CUR/data/create_table.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql_file "$CUR/data/create_table.sql" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	# Seed only upstream, before the changefeed start timestamp. Every UPDATE
	# below must create a missing downstream row rather than affect zero rows.
	run_sql_file "$CUR/data/insert.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	cat >"$config" <<EOF
[filter]
rules = ["safe_mode.t"]
EOF
	if [ "$filter_type" != "none" ]; then
		cat >>"$config" <<EOF

[[filter.event-filters]]
matcher = ["safe_mode.t"]
EOF
		if [ "$filter_type" = "event" ]; then
			echo 'ignore-event = ["delete"]' >>"$config"
		else
			echo 'ignore-delete-value-expr = "id > 0"' >>"$config"
		fi
	fi
	cdc_cli_changefeed create --changefeed-id="$changefeed_id" --config="$config" \
		--sink-uri="mysql://normal:123456@$DOWN_TIDB_HOST:$DOWN_TIDB_PORT/?safe-mode=true&batch-dml-enable=$batch_enable"
	ensure 30 check_changefeed_state "http://$UP_PD_HOST_1:$UP_PD_PORT_1" "$changefeed_id" normal null ""
	local row_count
	row_count=$(mysql -uroot -h"$DOWN_TIDB_HOST" -P"$DOWN_TIDB_PORT" -N -B -e "SELECT COUNT(*) FROM safe_mode.t")
	[ "$row_count" = "0" ]

	run_sql_file "$CUR/data/update.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	if [ "$filter_type" = "none" ]; then
		run_sql "UPDATE safe_mode.t SET a = 'multi' WHERE id = 3;"
		run_sql "UPDATE safe_mode.t SET a = 'multi' WHERE id = 4;"
	else
		# Exercise multiple UPDATEs in one event on the DELETE filter path.
		run_sql "BEGIN; UPDATE safe_mode.t SET a = 'multi' WHERE id IN (3, 4); COMMIT;"
	fi
	do_retry 60 1 bash -c check_rows
	check_sync_diff "$WORK_DIR" "$CUR/conf/diff_config.toml"
	check_changefeed_state "http://$UP_PD_HOST_1:$UP_PD_PORT_1" "$changefeed_id" normal null ""
	cdc_cli_changefeed remove --changefeed-id="$changefeed_id"
}

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	start_tidb_cluster --workdir "$WORK_DIR"
	export GO_FAILPOINTS=''
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"
	for filter_type in none event expr; do
		for batch_enable in false true; do
			run_case "$filter_type" "$batch_enable"
		done
	done
	if grep -Eq '^panic:|\[PANIC\]' "$WORK_DIR/stdout.log" "$WORK_DIR/cdc.log"; then
		echo "CDC panicked while recovering missing rows in safe mode"
		exit 1
	fi
	cleanup_process "$CDC_BINARY"
}

trap 'stop_test "$WORK_DIR"' EXIT
run
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
