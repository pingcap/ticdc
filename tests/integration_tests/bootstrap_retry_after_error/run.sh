#!/bin/bash

set -eu

# This integration test reproduces the panic caused by a bootstrap retry after
# the first bootstrap round has already failed and consumed the cached
# bootstrap responses.
#
# Steps:
# 1. Start two TiCDC nodes and create a blackhole changefeed.
# 2. Move one table to the non-maintainer node so failover has real bootstrap state.
# 3. SIGSTOP the current maintainer so the surviving node becomes the new maintainer.
# 4. Enable a one-shot bootstrap failpoint on the surviving node.
# 5. As soon as the surviving node logs the first bootstrap error, SIGSTOP it,
#    restart the old node, then SIGCONT the surviving node.
# 6. Verify the retry hits the empty checkpointTs panic path on the surviving node.

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

function wait_for_log_pattern() {
	local log_file=$1
	local pattern=$2
	local retry_count=${3:-300}
	local sleep_seconds=${4:-0.1}

	for ((i = 0; i < retry_count; i++)); do
		if [ -f "$log_file" ] && grep -Eqs "$pattern" "$log_file"; then
			return 0
		fi
		sleep "$sleep_seconds"
	done

	echo "pattern '$pattern' not found in $log_file" >&2
	return 1
}

function check_panic_logs() {
	local work_dir=$1

	if ! ls "$work_dir"/cdc*.log "$work_dir"/stdout*.log >/dev/null 2>&1; then
		echo "expected panic logs not found in $work_dir" >&2
		return 1
	fi

	if ! grep -Eqs 'all bootstrap responses reported empty checkpointTs' "$work_dir"/cdc*.log; then
		echo "expected empty checkpointTs message not found in $work_dir/cdc*.log" >&2
		return 1
	fi

	if ! grep -Eqs 'cant not found the startTs from the bootstrap response|\\[PANIC\\]' "$work_dir"/cdc*.log; then
		echo "expected panic message not found in $work_dir/cdc*.log" >&2
		return 1
	fi

	if ! grep -Eqs 'panic:.*cant not found the startTs from the bootstrap response|cant not found the startTs from the bootstrap response' "$work_dir"/stdout*.log; then
		echo "expected panic stack not found in $work_dir/stdout*.log" >&2
		return 1
	fi
}

export -f check_panic_logs

function start_cdc_server_no_wait() {
	local work_dir=$1
	local log_suffix=$2
	local addr=$3
	local pd_addr=$4
	local cover_id=${5:-$$}
	local data_dir=${6:-"$work_dir/cdc_data${log_suffix}"}

	GO_FAILPOINTS= $CDC_BINARY -test.coverprofile="$OUT_DIR/cov.$TEST_NAME.$cover_id.out" server \
		--log-file "$work_dir/cdc${log_suffix}.log" \
		--log-level debug \
		--data-dir "$data_dir" \
		--cluster-id default \
		--addr "$addr" \
		--pd "$pd_addr" >>"$work_dir/stdout${log_suffix}.log" 2>&1 &
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
	restart_logsuffix="${maintainer_logsuffix}-restart"
	other_logsuffix=$(addr_to_logsuffix "$other_addr")
	other_host=${other_addr%:*}
	other_port=${other_addr#*:}
	other_pid=$(get_cdc_pid "$other_host" "$other_port")
	if [ -z "$other_pid" ] || [ "$other_pid" == "null" ]; then
		echo "failed to get target maintainer pid" >&2
		exit 1
	fi

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

	kill -STOP "$maintainer_pid"

	(
		stopped_other=0
		trap 'if [ "$stopped_other" -eq 1 ]; then kill -CONT "$other_pid" 2>/dev/null || true; fi' EXIT
		wait_for_log_pattern "$WORK_DIR/cdc${other_logsuffix}.log" 'load table from scheme store failed|ErrSnapshotLostByGC' 1500 0.02
		kill -STOP "$other_pid"
		stopped_other=1
		kill -KILL "$maintainer_pid" 2>/dev/null || true
		start_cdc_server_no_wait "$WORK_DIR" "$restart_logsuffix" "$maintainer_addr" "$PD_ADDR" "${maintainer_pid}-restart"
		wait_for_log_pattern "$WORK_DIR/cdc${restart_logsuffix}.log" "remote capture online.*${maintainer_addr}" 100 0.05
		kill -CONT "$other_pid"
		stopped_other=0
		trap - EXIT
	) &
	resume_watcher_pid=$!

	wait "$resume_watcher_pid"

	ensure $MAX_RETRIES "check_panic_logs $WORK_DIR"
	echo "panic reproduced in bootstrap retry path, stop here for debugging" >&2
	exit 1
}

trap 'stop_test $WORK_DIR' EXIT
run
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
