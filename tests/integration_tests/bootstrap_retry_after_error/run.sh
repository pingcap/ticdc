#!/bin/bash

set -eu

# This integration test covers bootstrap retry handling after an initial
# bootstrap failure.
#
# Steps:
# 1. Start one TiCDC node with a one-shot schema store failpoint.
# 2. Create a mysql sink changefeed and wait until bootstrap fails with
#    ErrSnapshotLostByGC.
# 3. Start a second TiCDC node to trigger node change / bootstrap retry.
# 4. Verify both TiCDC servers keep running and the changefeed remains failed
#    with ErrSnapshotLostByGC.

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$CUR/../../.." && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=20

PD_ADDR="http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
CHANGEFEED_ID="bootstrap-retry-after-error-$RANDOM"
SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
FAILPOINT_NAME="github.com/pingcap/ticdc/logservice/schemastore/getAllPhysicalTablesGCFastFail"

function ensure_failpoint_cdc_binary() {
	make -C "$REPO_ROOT" integration_test_build_fast
}

function check_api_ready() {
	local addr=$1
	local http_code

	http_code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
		"http://${addr}/api/v2/changefeeds?keyspace=$KEYSPACE_NAME" \
		--user ticdc:ticdc_secret)
	[ "$http_code" = "200" ]
}

function check_cdc_logs_contains() {
	local work_dir=$1
	local pattern=$2
	grep -Eq "$pattern" "$work_dir"/cdc*.log
}

export -f check_api_ready
export -f check_cdc_logs_contains

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	ensure_failpoint_cdc_binary

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" \
		--addr "127.0.0.1:8300" --pd "$PD_ADDR" \
		--failpoint "${FAILPOINT_NAME}=1*return(true)"

	run_sql "CREATE DATABASE bootstrap_retry_after_error;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE bootstrap_retry_after_error.t1(id INT PRIMARY KEY, val INT);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE bootstrap_retry_after_error;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE TABLE bootstrap_retry_after_error.t1(id INT PRIMARY KEY, val INT);" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	cdc_cli_changefeed create --pd="$PD_ADDR" --sink-uri="$SINK_URI" -c "$CHANGEFEED_ID"

	ensure $MAX_RETRIES "check_cdc_logs_contains $WORK_DIR 'ErrSnapshotLostByGC'"
	ensure $MAX_RETRIES "check_changefeed_state $PD_ADDR $CHANGEFEED_ID failed ErrSnapshotLostByGC ''"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301" --pd "$PD_ADDR"

	ensure $MAX_RETRIES "get_cdc_pid 127.0.0.1 8300 >/dev/null"
	ensure $MAX_RETRIES "get_cdc_pid 127.0.0.1 8301 >/dev/null"
	ensure $MAX_RETRIES "check_api_ready 127.0.0.1:8300"
	ensure $MAX_RETRIES "check_api_ready 127.0.0.1:8301"
	ensure $MAX_RETRIES "check_changefeed_state $PD_ADDR $CHANGEFEED_ID failed ErrSnapshotLostByGC ''"
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
