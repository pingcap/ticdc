#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# use canal-adapter to sync data from kafka to mysql,
# make sure that `canal-json` output can be consumed by the canal-adapter.
function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="kafka://127.0.0.1:9092/test?protocol=canal-json"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	run_sql "CREATE TABLE test.finish_mark1 (a int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.finish_mark1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 600

	run_sql_file $CUR/data/data_gbk.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE test.finish_mark2 (a int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.finish_mark2 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 600

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
