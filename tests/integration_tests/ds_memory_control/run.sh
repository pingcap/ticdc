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

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_sql "CREATE DATABASE ds_memory_control;"
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=ds_memory_control
	export GO_FAILPOINTS='github.com/pingcap/ticdc/utils/dynstream/PausePath=10%return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://normal:123456@127.0.0.1:3306"
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"

	sleep 60
	run_sql "CREATE TABLE ds_memory_control.finish_mark_1 (a int primary key);"
	check_table_exists "ds_memory_control.finish_mark_1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 180
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 180

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
	export GO_FAILPOINTS='github.com/pingcap/ticdc/utils/dynstream/PauseArea=10%return(true)'

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	go-ycsb run mysql -P $CUR/conf/workload-2 -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=ds_memory_control

	sleep 60
	run_sql "CREATE TABLE ds_memory_control.finish_mark_2 (a int primary key);"
	check_table_exists "ds_memory_control.finish_mark_2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300

	cleanup_process $CDC_BINARY
}

trap 'stop_tidb_cluster; collect_logs $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
