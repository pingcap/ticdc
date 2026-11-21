#!/bin/bash

set -euo pipefail

# This case checks that a multi-table RENAME is applied atomically.
#
# 1. Create t1..t20 upstream and wait until t*_routed replicate downstream.
# 2. Hang the MySQL sink, then RENAME t* TO s* upstream. The rename is
#    recorded in the redo log but must not reach the downstream.
# 3. Kill CDC, replay the redo log with `cdc redo apply`, and verify the 20
#    renames are replayed as one atomic statement: s*_routed = 20 and
#    t*_routed = 0.
# 4. Restart CDC and verify replication resumes.
# 5. RENAME s* TO r* upstream, poll that the downstream never exposes a
#    partial state, then kill and restart downstream. The retried rename
#    must still complete atomically.

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TABLE_COUNT=20
DB_NAME=rename_atomic
REDO_DIR="/tmp/tidb_cdc_test/$TEST_NAME/redo"
CHANGEFEED_ID=rename-atomic
REPLICATE_SECONDS=1

function cleanup_redo_dir() {
	rm -rf "$REDO_DIR"
	mkdir -p "$REDO_DIR"
}

function count_downstream_tables() {
	local pattern=$1

	mysql -uroot -h "$DOWN_TIDB_HOST" -P "$DOWN_TIDB_PORT" \
		--default-character-set utf8mb4 -NBe \
		"SELECT IFNULL(SUM(table_name REGEXP '${pattern}'), 0) FROM information_schema.tables WHERE table_schema='${DB_NAME}'"
}

function read_downstream_rename_state() {
	local old_pattern=$1
	local new_pattern=$2
	local old_count
	local new_count
	local counts

	# Read both counts in one statement so the assertion uses one downstream
	# snapshot. Two separate queries can straddle an atomic rename and report
	# old=20/new=20 even though the RENAME never exposed a partial state.
	counts=$(mysql -uroot -h "$DOWN_TIDB_HOST" -P "$DOWN_TIDB_PORT" \
		--default-character-set utf8mb4 -NBe \
		"SELECT IFNULL(SUM(table_name REGEXP '${old_pattern}'), 0),
		        IFNULL(SUM(table_name REGEXP '${new_pattern}'), 0)
		 FROM information_schema.tables WHERE table_schema='${DB_NAME}'")
	read -r old_count new_count <<<"$counts"

	printf '%s %s\n' "$old_count" "$new_count"
}

function assert_rename_state_is_atomic() {
	local old_count=$1
	local new_count=$2

	# The atomic rename must expose either all old tables or all new tables.
	if ! { [ "$old_count" = "$TABLE_COUNT" ] && [ "$new_count" = 0 ]; } &&
		! { [ "$old_count" = 0 ] && [ "$new_count" = "$TABLE_COUNT" ]; }; then
		echo "[$(date)] Failed, observed partially renamed state: old=$old_count new=$new_count" >&2
		exit 1
	fi
}

function assert_rename_is_atomic() {
	local old_pattern=$1
	local new_pattern=$2
	local state
	local old_count
	local new_count

	state=$(read_downstream_rename_state "$old_pattern" "$new_pattern")
	read -r old_count new_count <<<"$state"
	assert_rename_state_is_atomic "$old_count" "$new_count"
}

function wait_for_complete_rename() {
	local old_pattern=$1
	local new_pattern=$2
	local i
	local state
	local old_count
	local new_count

	for i in $(seq 1 60); do
		state=$(read_downstream_rename_state "$old_pattern" "$new_pattern")
		read -r old_count new_count <<<"$state"
		if [ "$new_count" = "$TABLE_COUNT" ] && [ "$old_count" = 0 ]; then
			echo "[$(date)] Downstream rename complete: old=${old_count}, new=${new_count}"
			return 0
		fi
		echo "[$(date)] Waiting for rename: old=${old_count}, new=${new_count}"
		assert_rename_state_is_atomic "$old_count" "$new_count"
		sleep 2
	done
	echo "[$(date)] Failed, timed out waiting for the RENAME to complete" >&2
	exit 1
}

function kill_downstream_tidb() {
	local pid
	pid=$(lsof -ti tcp:"$DOWN_TIDB_PORT" -sTCP:LISTEN || true)
	if [ -z "$pid" ]; then
		echo "[$(date)] Downstream TiDB is not running" >&2
		exit 1
	fi
	kill -9 $pid
	while lsof -ti tcp:"$DOWN_TIDB_PORT" -sTCP:LISTEN >/dev/null 2>&1; do
		sleep 0.1
	done
}

function start_downstream_tidb() {
	local source_config
	local config_file

	if [ "$NEXT_GEN" = 1 ]; then
		source_config="$WORK_DIR/downstream-tidb-$KEYSPACE_NAME.toml"
	else
		source_config="$WORK_DIR/tidb-config.toml"
	fi

	# Mirror randomGenSocketsConf from start_tidb_cluster_impl: the initial
	# TiDB process uses a random socket path and does not retain its config,
	# so the restarted process needs a fresh config with a unique socket.
	random_str=$(date '+%s%N')
	if [ "$(uname)" = "Darwin" ]; then
		random_str=$(head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \n')
	fi
	config_file="$WORK_DIR/tidb-config-$random_str.toml"

	# The initial TiDB process uses a random socket path. Build a new config
	# for the restarted process to avoid reusing that socket path. The socket
	# must be a root-level option, so prepend it instead of appending it.
	{
		echo "socket = \"/tmp/tidb-$random_str.sock\""
		sed '/^[[:space:]]*socket[[:space:]]*=/d' "$source_config"
	} >"$config_file"

	tidb-server \
		-P "$DOWN_TIDB_PORT" \
		-config "$config_file" \
		--store tikv \
		--path "${DOWN_PD_HOST}:${DOWN_PD_PORT}" \
		--status "$DOWN_TIDB_STATUS" \
		--log-file "$WORK_DIR/tidb_down_restart.log" >>"$WORK_DIR/tidb_down_restart.stdout" 2>&1 &
	check_tidb_health "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 60
}

function run() {
	# Only runs for the mysql sink.
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	cleanup_redo_dir

	echo "[$(date)] Starting TiDB cluster and CDC server"
	start_tidb_cluster --workdir "$WORK_DIR"
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"

	cdc_cli_changefeed create \
		--changefeed-id="$CHANGEFEED_ID" \
		--sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/" \
		--config="$CUR/conf/changefeed.toml"
	ensure 30 check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "$CHANGEFEED_ID" normal null ""

	echo "[$(date)] Creating baseline tables"
	run_sql_file "$CUR/data/create_tables.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	local baseline_count
	for _ in $(seq 1 60); do
		baseline_count=$(count_downstream_tables '^t[0-9]+_routed$')
		if [ "$baseline_count" = "$TABLE_COUNT" ]; then
			break
		fi
		sleep 2
	done
	if [ "$baseline_count" != "$TABLE_COUNT" ]; then
		echo "[$(date)] Failed, timed out waiting for the routed baseline tables" >&2
		exit 1
	fi

	echo "[$(date)] Hanging the MySQL sink and renaming upstream tables"
	cleanup_process "$(basename "$CDC_BINARY")"
	export GO_FAILPOINTS='github.com/pingcap/ticdc/pkg/sink/mysql/MySQLSinkExecDDLDelay=return("3600")'
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"

	run_sql_file "$CUR/data/rename_tables.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	# The sink is hung on the rename: it must not reach the downstream yet.
	sleep 5
	assert_rename_is_atomic '^t[0-9]+_routed$' '^s[0-9]+_routed$'
	local new_count
	new_count=$(count_downstream_tables '^s[0-9]+_routed$')
	if [ "$new_count" != 0 ]; then
		echo "[$(date)] Failed, the hung rename reached the downstream: new=$new_count" >&2
		exit 1
	fi

	echo "[$(date)] Waiting for the redo log to catch up"
	local storage_path="file://$REDO_DIR"
	local tmp_download_path="$WORK_DIR/cdc_data/redo/$CHANGEFEED_ID"
	local current_tso
	current_tso=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	ensure 60 check_redo_resolved_ts "$CHANGEFEED_ID" "$current_tso" "$storage_path" "$tmp_download_path/meta"

	echo "[$(date)] Killing CDC and applying the redo log"
	cleanup_process "$(basename "$CDC_BINARY")"
	export GO_FAILPOINTS=''
	"$CDC_BINARY" redo apply --enable-profiling=false \
		--tmp-dir="$tmp_download_path/apply" \
		--storage="$storage_path" \
		--sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/" \
		>"$WORK_DIR/redo_apply.log" 2>&1 &
	local redo_pid=$!
	while kill -0 "$redo_pid" 2>/dev/null; do
		assert_rename_is_atomic '^t[0-9]+_routed$' '^s[0-9]+_routed$'
		sleep 0.2
	done
	if ! wait "$redo_pid"; then
		echo "[$(date)] Redo apply failed" >&2
		cat "$WORK_DIR/redo_apply.log" >&2
		exit 1
	fi
	wait_for_complete_rename '^t[0-9]+_routed$' '^s[0-9]+_routed$'

	echo "[$(date)] Restarting CDC and verifying replication resumes"
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"
	run_sql "INSERT INTO $DB_NAME.s1 (id, value) VALUES (1, 100);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	echo "[$(date)] Waiting for replication to resume"
	ensure 30 "run_sql 'SELECT COUNT(*) AS row_count FROM ${DB_NAME}.s1_routed WHERE id = 1 AND value = 100;' \
		${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_contains 'row_count: 1'"

	echo "[$(date)] Renaming upstream tables again"
	run_sql_file "$CUR/data/rename_tables_2nd.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	for _ in $(seq 1 $((REPLICATE_SECONDS * 5))); do
		# The downstream must only expose fully renamed tables
		assert_rename_is_atomic '^s[0-9]+_routed$' '^r[0-9]+_routed$'
		sleep 0.2
	done

	echo "[$(date)] Killing and restarting downstream"
	kill_downstream_tidb
	start_downstream_tidb

	wait_for_complete_rename '^s[0-9]+_routed$' '^r[0-9]+_routed$'
	ensure 30 check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "$CHANGEFEED_ID" normal null ""
	echo "[$(date)] Live sink rename complete"
}

function on_exit() {
	stop_test "$WORK_DIR"
}

trap on_exit EXIT
run "$@"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
