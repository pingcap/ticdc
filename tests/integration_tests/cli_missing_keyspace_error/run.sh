#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
SINK_TYPE=$1
CDC_BINARY=cdc

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		echo "skip ${TEST_NAME} for sink type ${SINK_TYPE}"
		return
	fi

	if [ "$NEXT_GEN" != "1" ]; then
		echo "skip ${TEST_NAME} because NEXT_GEN is disabled"
		return
	fi

	stop_tidb_cluster || true
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	start_tidb_cluster --workdir "$WORK_DIR"
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"

	set +e
	output=$(cdc cli --server "http://127.0.0.1:8300" changefeed pause --changefeed-id "missing-keyspace" 2>&1)
	exit_code=$?
	set -e

	if [ "$exit_code" -eq 0 ]; then
		echo "expected command to fail without --keyspace/-k, but it succeeded"
		exit 1
	fi

	if ! echo "$output" | grep -q "please specify --keyspace or -k"; then
		echo "expected guidance message not found in output:"
		echo "$output"
		exit 1
	fi

	if echo "$output" | grep -Eq "^Error:[[:space:]]*$"; then
		echo "unexpected empty error output:"
		echo "$output"
		exit 1
	fi
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
