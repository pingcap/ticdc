#!/bin/bash

set -euo pipefail

# This case checks that a replicated multi-table RENAME is applied atomically
# and survives a downstream TiDB restart.
#
# 1. Create the same t1..t20 baseline tables on both TiDB clusters.
# 2. Replicate t1..t20 TO s1..s20 for a few seconds.
# 3. Kill and restart downstream TiDB.
# 4. Poll the downstream s/t counts. A correct sink exposes only (20, 0) or
#    (0, 20); any mixed or partial count is a test failure.

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TABLE_COUNT=20
DB_NAME=rename_atomic
REPLICATE_SECONDS=1

function run() {
	# Only runs for the mysql sink.
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	echo "[$(date)] Starting TiDB cluster and CDC server"
	start_tidb_cluster --workdir "$WORK_DIR"
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"

	echo "[$(date)] Creating tables"
	run_sql_file "$CUR/data/create_tables.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql_file "$CUR/data/create_tables.sql" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"

	cdc_cli_changefeed create -c rename-atomic --sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"
	ensure 30 check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" rename-atomic normal null ""

	echo "[$(date)] Renaming upstream tables"
	run_sql_file "$CUR/data/rename_tables.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	for _ in $(seq 1 $((REPLICATE_SECONDS * 5))); do
		assert_no_partial_rename_state
		sleep 0.2
	done
	echo "[$(date)] Killing downstream TiDB after ${REPLICATE_SECONDS}s"
	kill_downstream_tidb
	echo "[$(date)] Restarting downstream TiDB"
	start_downstream_tidb

	wait_for_complete_rename
	ensure 30 check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" rename-atomic normal null ""
	echo "[$(date)] Downstream rename complete"
}

function count_downstream_tables() {
	local prefix=$1

	mysql -uroot -h "$DOWN_TIDB_HOST" -P "$DOWN_TIDB_PORT" \
		--default-character-set utf8mb4 -NBe \
		"SELECT IFNULL(SUM(table_name REGEXP '^${prefix}[0-9]+$'), 0) FROM information_schema.tables WHERE table_schema='${DB_NAME}'"
}

function assert_no_partial_rename_state() {
	local s_count
	local t_count

	t_count=$(count_downstream_tables t)
	s_count=$(count_downstream_tables s)

	if { [ "$t_count" != 0 ] && [ "$t_count" != "$TABLE_COUNT" ]; } ||
		{ [ "$s_count" != 0 ] && [ "$s_count" != "$TABLE_COUNT" ]; }; then
		echo "[$(date)] Failed, observed partially renamed state: old=$t_count new=$s_count" >&2
		exit 1
	fi
}

function wait_for_complete_rename() {
	local i
	local s_count
	local t_count

	for i in $(seq 1 60); do
		t_count=$(count_downstream_tables t)
		s_count=$(count_downstream_tables s)
		if [ "$s_count" = "$TABLE_COUNT" ] && [ "$t_count" = 0 ]; then
			echo "[$(date)] Downstream rename complete: old=${t_count}, new=${s_count}"
			return 0
		fi
		echo "[$(date)] Waiting for rename: old=${t_count}, new=${s_count}"
		assert_no_partial_rename_state
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

function on_exit() {
	stop_test "$WORK_DIR"
}

trap on_exit EXIT
run "$@"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
