#!/bin/bash
# this test case is used to test whether non-able split tables can be handled correctly

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# No need to test kafka and storage sink.
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://root:@127.0.0.1:3306/"

	run_sql "use test;create table t1 (a int primary key, b int, unique key uk(b));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "use test;create table t1 (a int primary key, b int, unique key uk(b));" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "split table test.t1 between (1) and (100000) regions 50;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	cdc_cli_changefeed create --sink-uri="$SINK_URI" -c "test" --config="$CUR/conf/changefeed.toml"

	sleep 60 # sleep enough for try to split table
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 50

	query_dispatcher_count "127.0.0.1:8300" "test" 2 10 # table trigger + t1 (check whether t1 is split)

	cleanup_process $CDC_BINARY
	stop_tidb_cluster
}

function run_for_force_split() {
	# No need to test kafka and storage sink.
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://root:@127.0.0.1:3306/"

	run_sql "use test;create table t1 (a int primary key, b int, unique key uk(b));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "use test;create table t1 (a int primary key, b int, unique key uk(b));" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "split table test.t1 between (1) and (100000) regions 50;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	sleep 10

	cdc_cli_changefeed create --sink-uri="$SINK_URI" -c "test2" --config="$CUR/conf/changefeed2.toml"

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 50

	query_dispatcher_count "127.0.0.1:8300" "test2" 6 10 # table trigger + t1 (split to 5 regions)

	cleanup_process $CDC_BINARY
	stop_tidb_cluster
}

trap 'stop_tidb_cluster; collect_logs $WORK_DIR' EXIT
run $*
run_for_force_split $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
