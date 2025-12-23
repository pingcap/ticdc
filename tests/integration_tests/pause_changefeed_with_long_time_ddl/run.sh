#!/bin/bash
# this test case is used to test whether we can successfully pause a changefeed when writing a long-time-cost ddl.

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

	export GO_FAILPOINTS='github.com/pingcap/ticdc/pkg/sink/mysql/MySQLSinkExecDDLDelay=return("3600")'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://root:@127.0.0.1:3306/"
	cdc_cli_changefeed create --sink-uri="$SINK_URI" -c "test"

	run_sql "use test;create table t1 (a int primary key, b int, unique key uk(b));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	cdc_cli_changefeed pause -c "test"

	sleep 10

	ensure 10 "check_logs_contains $WORK_DIR 'changefeed maintainer closed'"
	check_table_not_exists test.t1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	cdc_cli_changefeed resume -c "test"

	cleanup_process $CDC_BINARY
	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	check_table_exists test.t1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	cleanup_process $CDC_BINARY
	stop_tidb_cluster
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
