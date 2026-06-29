#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
CDC_ADDR="127.0.0.1:18300"

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR

	run_sql "CREATE DATABASE large_txn_split" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE large_txn_split" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	run_cdc_server \
		--workdir $WORK_DIR \
		--binary $CDC_BINARY \
		--addr "$CDC_ADDR" \
		--pd $pd_addr \
		--config "$CUR/conf/server.toml"

	cdc_cli_changefeed create \
		--server "$CDC_ADDR" \
		--sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/?transaction-atomicity=none"
}

function generate_workload() {
	local sql_file=$1
	local rows=2048

	{
		echo "USE large_txn_split;"
		echo "CREATE TABLE IF NOT EXISTS large_txn_table (id INT AUTO_INCREMENT PRIMARY KEY, batch_id INT, data LONGTEXT);"

		echo "BEGIN;"
		for i in $(seq 1 "$rows"); do
			echo "INSERT INTO large_txn_table (batch_id, data) VALUES (0, REPEAT('a', 1024));"
		done
		echo "COMMIT;"

		echo "BEGIN;"
		echo "UPDATE large_txn_table SET data = REPEAT('b', 1024) WHERE batch_id = 0;"
		echo "COMMIT;"

		echo "BEGIN;"
		echo "DELETE FROM large_txn_table WHERE batch_id = 0;"
		echo "COMMIT;"

		echo "TRUNCATE TABLE large_txn_table;"
		echo "BEGIN;"
		for i in $(seq 1 "$rows"); do
			echo "INSERT INTO large_txn_table (batch_id, data) VALUES (1, REPEAT('c', 1024));"
		done
		echo "COMMIT;"
	} >"$sql_file"
}

function run_workload() {
	local sql_file=$1
	local workload_log=$WORK_DIR/workload.log

	if ! mysql -uroot -h${UP_TIDB_HOST} -P${UP_TIDB_PORT} --default-character-set utf8mb4 <"$sql_file" >"$workload_log" 2>&1; then
		cat "$workload_log"
		return 1
	fi
}

trap 'stop_test $WORK_DIR' EXIT

if [ "$SINK_TYPE" == "mysql" ]; then
	prepare $*

	set -euxo pipefail

	echo "[$(date)] Starting large transaction split workload..."

	workload_sql=$WORK_DIR/large_txn_split_workload.sql
	set +x
	generate_workload "$workload_sql"
	set -x
	run_workload "$workload_sql"

	echo "[$(date)] Workload completed, verifying split path and data consistency..."

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 200 3
	$CUR/../_utils/check_logs_contains $WORK_DIR "scan interrupted inside a large txn"

	cleanup_process $CDC_BINARY
	echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
fi
