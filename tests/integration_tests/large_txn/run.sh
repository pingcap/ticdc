#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR

	# Create database in upstream and downstream
	run_sql "CREATE DATABASE large_txn" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE large_txn" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	# Start CDC server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr

	# Create changefeed
	cdc_cli_changefeed create --sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/?transaction-atomicity=none"
}

trap 'stop_test $WORK_DIR' EXIT

# Only support MySQL sink for this test
if [ "$SINK_TYPE" == "mysql" ]; then
	prepare $*

	cd "$(dirname "$0")"
	set -euxo pipefail

	echo "[$(date)] Starting large transaction workload..."

	# Run the large transaction workload
	# 1000 rows per txn, 10 txns per type (insert, update, delete)
	GO111MODULE=on go run main.go \
		-dsn "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/large_txn" \
		--rows=1000 \
		--txns=10

	echo "[$(date)] Workload completed, verifying data consistency with CDC sync..."

	# Use sync_diff_inspector to verify data consistency
	check_sync_diff $WORK_DIR $CUR/diff_config.toml 200 3

	cleanup_process $CDC_BINARY
	echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
fi
