#!/bin/bash

set -eu

# This integration test covers a bootstrap retry sequence after the first bootstrap
# round has already failed and consumed the cached bootstrap responses.
#
# Steps:
# 1. Start a single TiCDC node.
# 2. Enable a one-shot failpoint so the first maintainer bootstrap fails while
#    loading tables from schema store.
# 3. Create a blackhole changefeed and wait until the bootstrap failure is logged.
# 4. Start a second TiCDC node to trigger node change / bootstrap retry.
# 5. Verify the retry reaches the "empty checkpointTs" error path instead of panicking.

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
MAX_RETRIES=20

PD_ADDR="http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
CHANGEFEED_ID="bootstrap-retry-after-error"
FAILPOINT_NAME="github.com/pingcap/ticdc/logservice/schemastore/getAllPhysicalTablesGCFastFail"

function check_changefeed_failed() {
	local pd_addr=$1
	local changefeed_id=$2
	info=$(cdc_cli_changefeed query --pd=$pd_addr -c "$changefeed_id" -s | grep -v "Command to ticdc")
	state=$(echo "$info" | jq -r '.state')
	if [[ "$state" != "failed" ]]; then
		echo "changefeed state $state does not equal to failed"
		exit 1
	fi
}

export -f check_changefeed_failed

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300" --pd "$PD_ADDR"
	enable_failpoint --addr "127.0.0.1:8300" --name "$FAILPOINT_NAME" --expr "1*return(true)"

	cdc_cli_changefeed create --pd="$PD_ADDR" --sink-uri="blackhole://" -c "$CHANGEFEED_ID"

	ensure $MAX_RETRIES "check_logs_contains $WORK_DIR 'ErrSnapshotLostByGC' '0'"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301" --pd "$PD_ADDR"

	ensure $MAX_RETRIES "check_logs_contains $WORK_DIR 'maintainer node changed' '0'"
	ensure $MAX_RETRIES "check_logs_contains $WORK_DIR 'all bootstrap responses reported empty checkpointTs' '0'"
	ensure $MAX_RETRIES "get_cdc_pid 127.0.0.1 8300 >/dev/null"
	ensure $MAX_RETRIES "get_cdc_pid 127.0.0.1 8301 >/dev/null"
	ensure $MAX_RETRIES "check_changefeed_failed $PD_ADDR $CHANGEFEED_ID"


	cleanup_process $CDC_BINARY
	stop_tidb_cluster
}

trap 'stop_test $WORK_DIR' EXIT
run
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
