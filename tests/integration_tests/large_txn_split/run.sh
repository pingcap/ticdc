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

	run_sql "CREATE DATABASE large_txn_split" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE large_txn_split" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	run_cdc_server \
		--workdir $WORK_DIR \
		--binary $CDC_BINARY \
		--addr "127.0.0.1:8300" \
		--pd $pd_addr \
		--config "$CUR/conf/server.toml"

	cdc_cli_changefeed create --sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/?transaction-atomicity=none"
}

trap 'stop_test $WORK_DIR' EXIT

if [ "$SINK_TYPE" == "mysql" ]; then
	prepare $*

	set -euxo pipefail

	echo "[$(date)] Starting large transaction split workload..."

	GO111MODULE=on go run "$CUR/../large_txn/main.go" \
		-dsn "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/large_txn_split" \
		--rows=2048 \
		--txns=1

	echo "[$(date)] Workload completed, verifying split path and data consistency..."

	$CUR/../_utils/check_logs_contains $WORK_DIR "scan interrupted inside a large txn"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 200 3

	cleanup_process $CDC_BINARY
	echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
fi
