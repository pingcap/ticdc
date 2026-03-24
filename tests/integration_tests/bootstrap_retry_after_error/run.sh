#!/bin/bash

set -eu

# This integration test covers bootstrap failure handling after maintainer
# migration.
#
# Steps:
# 1. Start two TiCDC nodes and create a blackhole changefeed.
# 2. Trigger one real scheduling by moving a table from the current maintainer node
#    to the other node.
# 3. Kill the current maintainer so the surviving node becomes the new maintainer.
# 4. Enable a one-shot failpoint on the surviving node so its first bootstrap fails
#    while loading tables from schema store.
# 5. Restart the old node and verify the cluster stays healthy while the changefeed
#    transitions to failed with ErrSnapshotLostByGC.

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
MAX_RETRIES=20
CHECK_RETRIES=60

PD_ADDR="http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
CHANGEFEED_ID="bootstrap-retry-after-error-$RANDOM"
CDC_ADDRS=("127.0.0.1:8300" "127.0.0.1:8301")
FAILPOINT_NAME="github.com/pingcap/ticdc/logservice/schemastore/getAllPhysicalTablesGCFastFail"

function get_maintainer_addr() {
	local api_addr=$1
	curl -s --connect-timeout 1 --max-time 1 "http://${api_addr}/api/v2/changefeeds/${CHANGEFEED_ID}?keyspace=$KEYSPACE_NAME" | jq -r '.maintainer_addr'
}

function wait_for_maintainer_addr() {
	local api_addr=$1
	local maintainer_addr=""
	for ((i = 0; i < CHECK_RETRIES; i++)); do
		maintainer_addr=$(get_maintainer_addr "$api_addr")
		if [ -n "$maintainer_addr" ] && [ "$maintainer_addr" != "null" ]; then
			echo "$maintainer_addr"
			return 0
		fi
		sleep 1
	done
	echo "failed to get maintainer address" >&2
	return 1
}

function pick_other_addr() {
	local exclude=$1
	for addr in "${CDC_ADDRS[@]}"; do
		if [ "$addr" != "$exclude" ]; then
			echo "$addr"
			return 0
		fi
	done
	echo "failed to pick the other capture for $exclude" >&2
	return 1
}

function addr_to_logsuffix() {
	case "$1" in
	"127.0.0.1:8300") echo "0" ;;
	"127.0.0.1:8301") echo "1" ;;
	*)
		echo "unknown capture addr $1" >&2
		return 1
		;;
	esac
}

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	export GO_FAILPOINTS='github.com/pingcap/ticdc/maintainer/scheduler/StopBalanceScheduler=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300" --pd "$PD_ADDR"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301" --pd "$PD_ADDR"
	export GO_FAILPOINTS=''

	run_sql "CREATE DATABASE bootstrap_retry_after_error;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE bootstrap_retry_after_error.t1(id INT PRIMARY KEY, val INT);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	table_id=$(get_table_id "bootstrap_retry_after_error" "t1")

	cdc_cli_changefeed create --pd="$PD_ADDR" --sink-uri="blackhole://" -c "$CHANGEFEED_ID"

	maintainer_addr=$(wait_for_maintainer_addr "${CDC_ADDRS[0]}")
	other_addr=$(pick_other_addr "$maintainer_addr")
	maintainer_logsuffix=$(addr_to_logsuffix "$maintainer_addr")
	other_logsuffix=$(addr_to_logsuffix "$other_addr")

	check_coordinator_and_maintainer "$maintainer_addr" "$CHANGEFEED_ID" $CHECK_RETRIES
	query_dispatcher_count "$maintainer_addr" "$CHANGEFEED_ID" 2 $CHECK_RETRIES

	move_table_with_retry "$other_addr" $table_id "$CHANGEFEED_ID" 10

	enable_failpoint --addr "$other_addr" --name "$FAILPOINT_NAME" --expr "1*return(true)"

	maintainer_host=${maintainer_addr%:*}
	maintainer_port=${maintainer_addr#*:}
	maintainer_pid=$(get_cdc_pid "$maintainer_host" "$maintainer_port")
	if [ -z "$maintainer_pid" ] || [ "$maintainer_pid" == "null" ]; then
		echo "failed to get maintainer pid" >&2
		exit 1
	fi

	kill_cdc_pid "$maintainer_pid"

	ensure $MAX_RETRIES "check_logs_contains $WORK_DIR 'ErrSnapshotLostByGC' '$other_logsuffix'"

	export GO_FAILPOINTS='github.com/pingcap/ticdc/maintainer/scheduler/StopBalanceScheduler=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "${maintainer_logsuffix}-restart" --addr "$maintainer_addr" --pd "$PD_ADDR"
	export GO_FAILPOINTS=''

	ensure $MAX_RETRIES "get_cdc_pid 127.0.0.1 8300 >/dev/null"
	ensure $MAX_RETRIES "get_cdc_pid 127.0.0.1 8301 >/dev/null"
	ensure $MAX_RETRIES "check_changefeed_state $PD_ADDR $CHANGEFEED_ID failed ErrSnapshotLostByGC ''"

	cleanup_process $CDC_BINARY
	stop_tidb_cluster
}

trap 'stop_test $WORK_DIR' EXIT
run
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
