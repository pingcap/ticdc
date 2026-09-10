#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare

WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

DB_NAME=split_table_balance_event_store
TABLE_NAME=t
CHANGEFEED_ID=test
API_ADDR=127.0.0.1:8300
SOURCE_ADDR=127.0.0.1:8301
EVENT_STORE_FAILPOINT=github.com/pingcap/ticdc/logservice/eventstore/SlowEventStoreWrite

workload_pid=""
failpoint_enabled=false

function cleanup() {
	if [ -n "$workload_pid" ]; then
		kill "$workload_pid" 2>/dev/null || true
		wait "$workload_pid" 2>/dev/null || true
	fi
	if [ "$failpoint_enabled" == "true" ]; then
		disable_failpoint --addr "$SOURCE_ADDR" --name "$EVENT_STORE_FAILPOINT" || true
	fi
	stop_test $WORK_DIR
}

function get_capture_id() {
	local addr=$1
	curl -fsS "http://${API_ADDR}/api/v2/captures" |
		jq -r --arg addr "$addr" '.items[] | select(.address == $addr) | .id' | head -n1
}

function get_table_counts() {
	local table_id=$1
	local source_id=$2
	curl -fsS "http://${API_ADDR}/api/v2/changefeeds/${CHANGEFEED_ID}/tables?keyspace=${KEYSPACE_NAME}" |
		jq -r --argjson table_id "$table_id" --arg source_id "$source_id" '
            [.items[] | .node_id as $node_id | .table_ids[] |
                select(. == $table_id) | {node_id: $node_id}] as $dispatchers |
            [($dispatchers | length),
             ([$dispatchers[] | select(.node_id == $source_id)] | length)] |
            @tsv'
}

function wait_for_split_table_on_source() {
	local table_id=$1
	local source_id=$2
	for ((i = 0; i < 60; i++)); do
		read -r total source < <(get_table_counts "$table_id" "$source_id")
		if [ "$total" -ge 4 ] && [ "$source" -eq "$total" ]; then
			echo "$total"
			return 0
		fi
		sleep 2
	done
	echo "split table dispatchers were not all scheduled on $SOURCE_ADDR" >&2
	return 1
}

function wait_for_redistribution() {
	local table_id=$1
	local source_id=$2
	local initial_count=$3
	for ((i = 0; i < 60; i++)); do
		read -r total source < <(get_table_counts "$table_id" "$source_id")
		if [ "$total" -eq "$initial_count" ] && [ "$source" -lt "$initial_count" ]; then
			echo "dispatcher distribution changed: source=$source, other=$((total - source))"
			return 0
		fi
		sleep 2
	done
	echo "dispatchers were not moved away from the EventStore-heavy node" >&2
	return 1
}

function generate_table_traffic() {
	while true; do
		mysql -h${UP_TIDB_HOST} -P${UP_TIDB_PORT} -uroot -N -s \
			-e "UPDATE ${DB_NAME}.${TABLE_NAME} SET payload=REPEAT(IF(LEFT(payload, 1)='x', 'y', 'x'), 65536), seq=seq+1;" \
			>/dev/null
		sleep 0.2
	done
}

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR

	local pd_addr=http://${UP_PD_HOST_1}:${UP_PD_PORT_1}
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --pd "$pd_addr" --logsuffix 0 --addr "$API_ADDR"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --pd "$pd_addr" --logsuffix 1 --addr "$SOURCE_ADDR"

	run_sql "CREATE DATABASE ${DB_NAME};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE ${DB_NAME};" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE TABLE ${DB_NAME}.${TABLE_NAME} (id INT PRIMARY KEY, seq BIGINT NOT NULL, payload MEDIUMTEXT NOT NULL);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE ${DB_NAME}.${TABLE_NAME} (id INT PRIMARY KEY, seq BIGINT NOT NULL, payload MEDIUMTEXT NOT NULL);" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "INSERT INTO ${DB_NAME}.${TABLE_NAME} VALUES (1000,0,''),(11000,0,''),(21000,0,''),(31000,0,''),(41000,0,''),(51000,0,''),(61000,0,''),(71000,0,'');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO ${DB_NAME}.${TABLE_NAME} VALUES (1000,0,''),(11000,0,''),(21000,0,''),(31000,0,''),(41000,0,''),(51000,0,''),(61000,0,''),(71000,0,'');" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "SPLIT TABLE ${DB_NAME}.${TABLE_NAME} BETWEEN (0) AND (80000) REGIONS 8;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	local start_ts
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	do_retry 5 3 cdc_cli_changefeed create --pd="$pd_addr" --start-ts="$start_ts" \
		--sink-uri="mysql://normal:123456@127.0.0.1:3306/" -c "$CHANGEFEED_ID" \
		--config="$CUR/conf/changefeed.toml"

	local table_id
	local source_id
	local initial_count
	table_id=$(get_table_id "$DB_NAME" "$TABLE_NAME")
	split_table_with_retry "$table_id" "$CHANGEFEED_ID" 10
	move_split_table_with_retry "$SOURCE_ADDR" "$table_id" "$CHANGEFEED_ID" 10
	source_id=$(get_capture_id "$SOURCE_ADDR")
	initial_count=$(wait_for_split_table_on_source "$table_id" "$source_id")

	# Delay each EventStore commit on the selected source node. This keeps the
	# write workers occupied and lets incoming events queue behind them.
	enable_failpoint --addr "$SOURCE_ADDR" --name "$EVENT_STORE_FAILPOINT" --expr "sleep(1000)"
	failpoint_enabled=true
	generate_table_traffic &
	workload_pid=$!

	wait_for_redistribution "$table_id" "$source_id" "$initial_count"

	kill "$workload_pid"
	wait "$workload_pid" 2>/dev/null || true
	workload_pid=""
	disable_failpoint --addr "$SOURCE_ADDR" --name "$EVENT_STORE_FAILPOINT"
	failpoint_enabled=false

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 60
	cleanup_process $CDC_BINARY
}

trap cleanup EXIT
run "$@"
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
