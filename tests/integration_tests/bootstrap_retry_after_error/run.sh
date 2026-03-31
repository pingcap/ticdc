#!/bin/bash

set -eu

# This integration test covers bootstrap retry handling after an initial
# bootstrap failure.
#
# Steps:
# 1. Start one TiCDC node with a schema store failpoint that keeps bootstrap
#    failing with ErrSnapshotLostByGC on the maintainer node.
# 2. Create a mysql sink changefeed and wait until bootstrap fails with
#    ErrSnapshotLostByGC.
# 3. Start a second TiCDC node immediately after the first bootstrap error so
#    the failed maintainer still observes node scheduling and processes another
#    bootstrap response.
# 4. Verify logs contain the retry path:
#    maintainer node changed -> bootstrap response -> handle bootstrap response.
# 5. Verify both TiCDC servers keep running and the changefeed remains failed
#    with ErrSnapshotLostByGC.

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$CUR/../../.." && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=20
FAILPOINT_BLOCK_BEFORE_STOP_CHANGEFEED="github.com/pingcap/ticdc/coordinator/BlockBeforeStopChangefeed"

PD_ADDR="http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
SINK_URI="mysql://normal:123456@127.0.0.1:3306/"

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

function check_node_change_triggers_bootstrap() {
	local work_dir=$1
	local file

	for file in "$work_dir"/cdc*.log; do
		if [ ! -f "$file" ]; then
			continue
		fi
		if awk '
			/maintainer node changed/ {
				nodeChanged = 1
				gotResp = 0
				handled = 0
			}
			nodeChanged && /maintainer received bootstrap response/ {
				gotResp = 1
			}
			nodeChanged && gotResp && /handle bootstrap response/ {
				handled = 1
				exit 0
			}
			END {
				exit handled ? 0 : 1
			}
		' "$file"; then
			return 0
		fi
	done

	return 1
}

export -f check_api_ready
export -f check_cdc_logs_contains
export -f check_node_change_triggers_bootstrap

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	export GO_FAILPOINTS='github.com/pingcap/ticdc/logservice/schemastore/getAllPhysicalTablesGCFastFail=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0"
	ensure $MAX_RETRIES "check_api_ready 127.0.0.1:8300"
	enable_failpoint --addr "127.0.0.1:8300" --name "$FAILPOINT_BLOCK_BEFORE_STOP_CHANGEFEED" --expr "pause"

	run_sql "CREATE DATABASE bootstrap_retry_after_error;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE bootstrap_retry_after_error.t1(id INT PRIMARY KEY, val INT);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE bootstrap_retry_after_error;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE TABLE bootstrap_retry_after_error.t1(id INT PRIMARY KEY, val INT);" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	cdc_cli_changefeed create --sink-uri="$SINK_URI" -c "test"

	ensure $MAX_RETRIES "check_cdc_logs_contains $WORK_DIR 'ErrSnapshotLostByGC'"

	# Start the second node without the schema-store failpoint. The retry still
	# fails because the maintainer is running on the first node, which keeps the
	# bootstrap error active while we verify node scheduling triggers another
	# bootstrap round.
	export GO_FAILPOINTS=''

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301" --pd "$PD_ADDR"

	ensure $MAX_RETRIES "check_node_change_triggers_bootstrap $WORK_DIR"
	disable_failpoint --addr "127.0.0.1:8300" --name "$FAILPOINT_BLOCK_BEFORE_STOP_CHANGEFEED"
	ensure $MAX_RETRIES "get_cdc_pid 127.0.0.1 8300 >/dev/null"
	ensure $MAX_RETRIES "get_cdc_pid 127.0.0.1 8301 >/dev/null"
	ensure $MAX_RETRIES "check_api_ready 127.0.0.1:8300"
	ensure $MAX_RETRIES "check_api_ready 127.0.0.1:8301"
	ensure $MAX_RETRIES "check_changefeed_state $PD_ADDR test failed ErrSnapshotLostByGC ''"
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
