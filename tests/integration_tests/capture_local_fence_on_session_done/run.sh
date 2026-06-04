#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

DB_NAME="capture_local_fence_on_session_done"
TABLE_NAME="t"
CAPTURE1_ADDR="127.0.0.1:8300"
CAPTURE2_ADDR="127.0.0.1:8301"
HANG_FAILPOINT="github.com/pingcap/ticdc/pkg/sink/mysql/MySQLSinkHangLongTime"

function get_capture_id_by_addr() {
	local api_addr=$1
	local target_addr=$2
	curl -s "http://${api_addr}/api/v2/captures" |
		jq -r --arg addr "$target_addr" '.items[] | select(.address==$addr) | .id' | head -n1
}

function get_table_node_id() {
	local api_addr=$1
	local changefeed_id=$2
	local table_id=$3
	curl -s "http://${api_addr}/api/v2/changefeeds/${changefeed_id}/tables?keyspace=$KEYSPACE_NAME" |
		jq -r --argjson tid "$table_id" '.items[] | select(.table_ids | index($tid)) | .node_id' | head -n1
}

function wait_for_table_on_addr() {
	local api_addr=$1
	local changefeed_id=$2
	local table_id=$3
	local target_addr=$4

	for ((i = 0; i < 30; i++)); do
		local target_id
		target_id=$(get_capture_id_by_addr "$api_addr" "$target_addr")
		if [ -z "$target_id" ] || [ "$target_id" == "null" ]; then
			sleep 2
			continue
		fi

		local node_id
		node_id=$(get_table_node_id "$api_addr" "$changefeed_id" "$table_id")
		if [ "$node_id" == "$target_id" ]; then
			return 0
		fi
		sleep 2
	done

	echo "table $table_id not scheduled on $target_addr" >&2
	return 1
}

function revoke_capture_lease() {
	local capture_id=$1
	local capture_key="/tidb/cdc/default/__cdc_meta__/capture/${capture_id}"
	local lease
	lease=$(ETCDCTL_API=3 etcdctl get "$capture_key" -w json | grep -o 'lease":[0-9]*' | awk -F: '{print $2}')
	if [ -z "$lease" ]; then
		echo "failed to get etcd lease for capture $capture_id" >&2
		return 1
	fi

	local lease_hex
	lease_hex=$(printf '%x\n' "$lease")
	ETCDCTL_API=3 etcdctl lease revoke "$lease_hex"
}

function wait_for_downstream_row_count() {
	local expected=$1
	for ((i = 0; i < 30; i++)); do
		local count
		count=$(mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -N \
			-e "select count(*) from ${DB_NAME}.${TABLE_NAME};" 2>/dev/null || true)
		if [ "$count" == "$expected" ]; then
			return 0
		fi
		sleep 2
	done

	echo "downstream row count is not $expected" >&2
	return 1
}

function run() {
	# This case depends on the MySQL sink DML failpoint.
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --pd $pd_addr --logsuffix 1 --addr "$CAPTURE1_ADDR"
	export GO_FAILPOINTS="${HANG_FAILPOINT}=return(true)"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --pd $pd_addr --logsuffix 2 --addr "$CAPTURE2_ADDR"
	export GO_FAILPOINTS=''

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1&worker-count=1"

	run_sql "CREATE DATABASE ${DB_NAME};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE ${DB_NAME}.${TABLE_NAME} (id int primary key, v int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE ${DB_NAME};" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE TABLE ${DB_NAME}.${TABLE_NAME} (id int primary key, v int);" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	changefeed_id=$(cdc_cli_changefeed create --pd=$pd_addr --start-ts=$start_ts --sink-uri="$SINK_URI" | grep '^ID:' | head -n1 | awk '{print $2}')
	table_id=$(get_table_id "$DB_NAME" "$TABLE_NAME")

	move_table_with_retry "$CAPTURE2_ADDR" $table_id "$changefeed_id" 10
	wait_for_table_on_addr "$CAPTURE1_ADDR" "$changefeed_id" "$table_id" "$CAPTURE2_ADDR"

	capture2_id=$(get_capture_id_by_addr "$CAPTURE1_ADDR" "$CAPTURE2_ADDR")
	if [ -z "$capture2_id" ] || [ "$capture2_id" == "null" ]; then
		echo "failed to get capture id for $CAPTURE2_ADDR" >&2
		exit 1
	fi
	cdc2_pid=$(get_cdc_pid "127.0.0.1" "8301")

	run_sql "INSERT INTO ${DB_NAME}.${TABLE_NAME} VALUES (1, 10), (2, 20), (3, 30), (4, 40);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	ensure 30 "grep -Eq 'inject MySQLSinkHangLongTime' '$WORK_DIR/cdc2.log'"
	wait_for_downstream_row_count 0

	revoke_capture_lease "$capture2_id"

	ensure 15 "! ps -p $cdc2_pid"
	check_logs_contains $WORK_DIR "local fence triggered" 2
	check_logs_contains $WORK_DIR "dispatcher orchestrator local fence triggered" 2
	check_logs_contains $WORK_DIR "stopping dispatcher manager write path" 2
	check_logs_contains $WORK_DIR "server closed" 2

	run_sql "INSERT INTO ${DB_NAME}.${TABLE_NAME} VALUES (5, 50), (6, 60);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
