#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

if [ "$SINK_TYPE" != "mysql" ]; then
	echo "schema_store_ignore_ddl only runs with mysql sink"
	exit 0
fi

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
	cdc_cli_changefeed create --sink-uri="$SINK_URI" --config $CUR/conf/changefeed.toml

	run_sql "create database schema_store_ignore_ddl;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "create table schema_store_ignore_ddl.t_old(id int primary key, v int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "insert into schema_store_ignore_ddl.t_old values (1, 1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "schema_store_ignore_ddl.t_old" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	ensure 30 "run_sql 'select count(1) from schema_store_ignore_ddl.t_old;' ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_contains 'count(1): 1'"

	run_sql "rename table schema_store_ignore_ddl.t_old to schema_store_ignore_ddl.t_new;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "create table schema_store_ignore_ddl.finish_mark(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "schema_store_ignore_ddl.finish_mark" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	check_table_exists "schema_store_ignore_ddl.t_old" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_not_exists "schema_store_ignore_ddl.t_new" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
