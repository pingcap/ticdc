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
	run_sql "CREATE DATABASE complex_txn" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE complex_txn" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# Start 2 CDC servers
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "cdc1"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "cdc2" --addr "127.0.0.1:8301"

	# Create changefeed
	cdc_cli_changefeed create --sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"
}

trap 'stop_test $WORK_DIR' EXIT
# Only support MySQL sink for complex transaction test
if [ "$SINK_TYPE" == "mysql" ]; then
	prepare $*

	cd "$(dirname "$0")"
	set -euxo pipefail

	echo "[$(date)] Starting complex transaction workload..."

	# Run the complex transaction workload
	GO111MODULE=on go run *.go \
		-dsn "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/complex_txn" \
		--concurrency=20 \
		--total-txns=200000 \
		--products=200 \
		--users=2000

	echo "[$(date)] Workload completed, verifying data consistency with CDC sync..."

	# Use sync_diff_inspector to verify data consistency
	# It will retry until data is consistent or timeout
	check_sync_diff $WORK_DIR $CUR/diff_config.toml 100 3

	cleanup_process $CDC_BINARY
	echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
fi
