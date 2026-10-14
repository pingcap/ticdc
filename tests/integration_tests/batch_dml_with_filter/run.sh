#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=${CDC_BINARY:-cdc.test}
SINK_TYPE=$1

function check_rows() {
	local table=$1
	local expected=$2
	local actual
	actual=$(mysql -uroot -h"$DOWN_TIDB_HOST" -P"$DOWN_TIDB_PORT" -N -B -e \
		"SELECT GROUP_CONCAT(CONCAT_WS(':', id, biz_key, biz_status, payload) ORDER BY id SEPARATOR ',') FROM batch_dml_with_filter.$table") || return 1
	if [ "$actual" != "$expected" ]; then
		echo "Unexpected rows in $table: expected '$expected', got '$actual'"
		return 1
	fi
}
export -f check_rows

function run_case() {
	local safe_mode=$1
	local scenario=$2
	local table="${scenario}_${safe_mode}"
	local changefeed_id="batch-filter-${scenario//_/-}-$safe_mode"
	local config="$WORK_DIR/$table.toml"
	local create_table="CREATE TABLE batch_dml_with_filter.$table (
        id BIGINT PRIMARY KEY,
        biz_key VARCHAR(64) NOT NULL,
        biz_status TINYINT NOT NULL,
        payload VARCHAR(64),
        UNIQUE KEY uk_biz (biz_key, biz_status))"
	run_sql "$create_table" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "$create_table" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	cat >"$config" <<EOF
[filter]
rules = ["batch_dml_with_filter.$table"]

[[filter.event-filters]]
matcher = ["batch_dml_with_filter.$table"]
ignore-event = ["delete"]
EOF

	cdc_cli_changefeed create --changefeed-id="$changefeed_id" --config="$config" \
		--sink-uri="mysql://root@$DOWN_TIDB_HOST:$DOWN_TIDB_PORT/?batch-dml-enable=true&safe-mode=$safe_mode&worker-count=1"
	ensure 30 check_changefeed_state "http://$UP_PD_HOST_1:$UP_PD_PORT_1" "$changefeed_id" normal null ""

	# Synchronize a seed before writing the workload so unsafe-mode events are
	# newer than ReplicatingTs and do not take the recovery safe-mode path.
	run_sql "INSERT INTO batch_dml_with_filter.$table VALUES (99, 'retained', 0, 'kept')" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	do_retry 60 1 bash -c 'check_rows "$@"' _ "$table" "99:retained:0:kept"
	if [ "$scenario" = "update_insert" ]; then
		run_sql "INSERT INTO batch_dml_with_filter.$table VALUES (1, 'business', 1, 'before')" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
		do_retry 60 1 bash -c 'check_rows "$@"' _ "$table" "1:business:1:before,99:retained:0:kept"
	fi

	local log_start
	log_start=$(wc -l <"$WORK_DIR/cdc.log")
	# Reuse the same key in many separate transactions so normal batching sees
	# UPDATE -> INSERT or INSERT -> INSERT after DELETE filtering.
	local workload="$WORK_DIR/$table.sql"
	{
		echo "SET autocommit = 1;"
		for ((i = 0; i < 200; i++)); do
			if [ "$scenario" = "update_insert" ]; then
				echo "UPDATE batch_dml_with_filter.$table SET payload = 'updated' WHERE id = 1;"
				echo "DELETE FROM batch_dml_with_filter.$table WHERE id = 1;"
				echo "INSERT INTO batch_dml_with_filter.$table VALUES (1, 'business', 1, 'round');"
			else
				echo "INSERT INTO batch_dml_with_filter.$table VALUES (1, 'business', 1, 'first');"
				echo "DELETE FROM batch_dml_with_filter.$table WHERE id = 1;"
			fi
		done
		if [ "$scenario" = "update_insert" ]; then
			echo "DELETE FROM batch_dml_with_filter.$table WHERE id = 1;"
		fi
		echo "INSERT INTO batch_dml_with_filter.$table VALUES (1, 'business', 1, 'last'), (2, 'other', 0, 'other');"
	} >"$workload"
	mysql -uroot -h"$UP_TIDB_HOST" -P"$UP_TIDB_PORT" <"$workload" >"$WORK_DIR/$table.workload.log" 2>&1
	do_retry 60 1 bash -c 'check_rows "$@"' _ "$table" "1:business:1:last,2:other:0:other,99:retained:0:kept"

	if [ "$safe_mode" = "false" ]; then
		# Unsafe fallback must reach the downstream duplicate-key retry, whose
		# safe-mode regeneration must also handle the filtered event sequence.
		tail -n +"$((log_start + 1))" "$WORK_DIR/cdc.log" >"$WORK_DIR/$table.retry.log"
		grep -Fq "Meet Duplicate Entry Error, retry the dmls in safemode" "$WORK_DIR/$table.retry.log"
	fi

	# A distinct deleted row must remain downstream, and a later update must
	# still make progress after the fallback and any safe-mode retry.
	run_sql "DELETE FROM batch_dml_with_filter.$table WHERE id = 99;
        UPDATE batch_dml_with_filter.$table SET payload = 'continued' WHERE id = 2" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	do_retry 60 1 bash -c 'check_rows "$@"' _ "$table" "1:business:1:last,2:other:0:continued,99:retained:0:kept"
	check_changefeed_state "http://$UP_PD_HOST_1:$UP_PD_PORT_1" "$changefeed_id" normal null ""
	kill -0 "$CDC_PID"
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
	CDC_PID=$(get_cdc_pid "$CDC_HOST" "$CDC_PORT")
	for host_port in "$UP_TIDB_HOST:$UP_TIDB_PORT" "$DOWN_TIDB_HOST:$DOWN_TIDB_PORT"; do
		run_sql "CREATE DATABASE batch_dml_with_filter" "${host_port%:*}" "${host_port##*:}"
	done
	for safe_mode in false true; do
		for scenario in update_insert insert_insert; do
			run_case "$safe_mode" "$scenario"
		done
	done
	if grep -Eq '^panic:|\[PANIC\]' "$WORK_DIR/stdout.log" "$WORK_DIR/cdc.log"; then
		echo "CDC panicked while replicating filtered batch DML"
		exit 1
	fi
	cleanup_process "$CDC_BINARY"
}

trap 'stop_test "$WORK_DIR"' EXIT
run
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
