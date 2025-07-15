#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# Validate sink type is mysql since this test is mysql specific
	if [ "$SINK_TYPE" != "mysql" ]; then
		echo "skip mysql_sink_encoding_test for $SINK_TYPE"
		return 0
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# Record TSO before creating tables to skip system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	
	# Create test database
	run_sql "CREATE DATABASE mysql_sink_encoding_test;"
	
	# Start CDC server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# Create changefeed with MySQL sink
	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"

	# Execute test SQL files
	echo "Executing DDL tests..."
	run_sql_file $CUR/data/ddl_tests.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	
	echo "Executing DML tests..."
	run_sql_file $CUR/data/dml_tests.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	
	echo "Executing large data volume tests..."
	run_sql_file $CUR/data/large_volume_tests.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Wait for tables to be created in downstream
	sleep 30
	
	# Check that all test tables exist in downstream
	check_table_exists "mysql_sink_encoding_test.basic_types" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.complex_types" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.large_volume" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.multi_pk_table" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.composite_pk_table" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.string_composite_pk" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.mixed_pk_table" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.auto_increment_table" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.unique_constraints_table" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.parent_table" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.child_table" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.indexed_table" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.charset_collation_test" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.default_values_test" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.check_constraints_test" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "mysql_sink_encoding_test.finish_mark" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60

	# Verify data consistency between upstream and downstream
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>" 