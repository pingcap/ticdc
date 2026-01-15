#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4

function run() {
	# Validate sink type is mysql since this test is mysql specific
	if [ "$SINK_TYPE" != "mysql" ]; then
		echo "skip sink_hang test for $SINK_TYPE"
		return 0
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	run_sql_file $CUR/data/down_data.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_sql_file $CUR/data/up_ddl.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_sql "CREATE DATABASE sink_retry;"
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=sink_retry
	export GO_FAILPOINTS='github.com/pingcap/ticdc/pkg/sink/mysql/MySQLSinkTxnRandomError=25%return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1"
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"

	sleep 5

	run_sql "CREATE TABLE sink_retry.finish_mark_1 (a int primary key);"

	# write the same data in upstream and downstream to trigger duplicate entry error
	run_sql_file $CUR/data/up_data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	sleep 30
	check_table_exists "sink_retry.finish_mark_1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=sink_retry

	sleep 10
	run_sql "CREATE TABLE sink_retry.finish_mark_2 (a int primary key);"
	sleep 30
	check_table_exists "sink_retry.finish_mark_2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	check_logs_contains $WORK_DIR "Meet Duplicate Entry Error, retry the dmls in safemode"

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
