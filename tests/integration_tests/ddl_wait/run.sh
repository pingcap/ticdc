#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# This test simulates DDL operations that take a long time.
# TiCDC blocks DDL operations until its state is not running, except for adding indexes.
# TiCDC also checks add index ddl state before execute a new DDL.
function run() {
	# No need to test kafka and storage sink.
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	cd $CUR
	GO111MODULE=on go run test.go

	TOPIC_NAME="ticdc-ddl-wait-test-$RANDOM"
	SINK_URI="mysql://root@127.0.0.1:3306/?read-timeout=300ms"

	changefeed_id="ddl-wait"
	cdc_cli_changefeed create --sink-uri="$SINK_URI" -c=${changefeed_id}

	run_sql "alter table test.t modify column col decimal(30,10);"
	run_sql "alter table test.t add index idx_col (col);"
	# The downstream add index DDL may finish quickly with fast reorg enabled,
	# so we need a short fixed-interval polling to avoid missing the running window.
	for i in $(seq 1 120); do
		run_sql 'SELECT JOB_ID FROM information_schema.ddl_jobs WHERE DB_NAME = "test" AND TABLE_NAME = "t" AND JOB_TYPE LIKE "add index%" AND (STATE = "running" OR STATE = "queueing") LIMIT 1;' \
			"${DOWN_TIDB_HOST}" "${DOWN_TIDB_PORT}" >/dev/null 2>&1 || true
		if check_contains 'JOB_ID:' >/dev/null 2>&1; then
			break
		fi
		sleep 0.5
	done
	check_contains 'JOB_ID:'
	run_sql "create table test.t_like like test.t;"
	run_sql "insert into test.t values (1, 1);"
	run_sql "create table test.finish_mark (a int primary key);"
	check_table_exists test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_table_exists test.t_like ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	run_sql "show index from test.t_like where Key_name='idx_col';" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "Key_name: idx_col"

	# ensure all dml / ddl related to test.t finish
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300

	ensure 10 "check_logs_contains $WORK_DIR 'DDL replicate success'"
	ensure 10 "check_logs_contains $WORK_DIR 'DDL is running downstream'"

	# indexes should be the same when CDC retries happened
	# ref: https://github.com/pingcap/tiflow/issues/12128
	run_sql "alter table test.t add index (col);"
	run_sql "alter table test.t add index (col);"
	sleep 3
	cleanup_process $CDC_BINARY
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	# make sure all tables are equal in upstream and downstream
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300
	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
