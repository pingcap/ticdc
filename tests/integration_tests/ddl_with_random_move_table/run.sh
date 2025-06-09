#!/bin/bash
# This test is aimed to test the ddl execution when the table is scheduled to be moved.
# 1. we start three TiCDC servers, and create 100 tables.
# 2. one thread we execute ddl randomly(including create table, drop table, rename table, truncate table, recover table, add column, drop column)
# 3. one thread we execute dmls, and insert data to these 100 tables.
# 4. one thread we randomly move the tables to other nodes.
# finally, we check the data consistency between the upstream and downstream.

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
check_time=60

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "2" --addr "127.0.0.1:8302"

	TOPIC_NAME="ticdc-failover-ddl-test-mix-$RANDOM"
	SINK_URI="mysql://root@127.0.0.1:3306/"
	do_retry 5 3 run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c "test"
}

function create_tables() {
	## normal tables
	for i in {1..10}; do
		echo "Creating table table_$i..."
		run_sql "CREATE TABLE IF NOT EXISTS test.table_$i (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
}

function execute_ddls() {
	while true; do
		table_num=$((RANDOM % 10 + 1))
		table_name="table_$table_num"

		case $((RANDOM % 5)) in
		0)
			echo "DDL: Dropping and recreating $table_name..."
			run_sql "DROP TABLE IF EXISTS test.$table_name;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			sleep 0.5
			run_sql "CREATE TABLE IF NOT EXISTS test.$table_name (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			;;
		1)
			echo "DDL: Dropping and recovering $table_name..."
			run_sql "DROP TABLE IF EXISTS test.$table_name;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			sleep 0.5
			run_sql "RECOVER TABLE test.$table_name;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			;;
		2)
			echo "DDL: Truncating $table_name..."
			run_sql "TRUNCATE TABLE test.$table_name;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			;;
		3)
			echo "DDL: Renaming $table_name..."
			new_table_name="table_$(($table_num + 100))"
			run_sql "RENAME TABLE test.$table_name TO test.$new_table_name;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			sleep 0.5
			run_sql "RENAME TABLE test.$new_table_name TO test.$table_name;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			;;
		4)
			echo "DDL: Adding column to $table_name..."
			run_sql "ALTER TABLE test.$table_name ADD COLUMN new_col INT;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			sleep 0.5
			run_sql "ALTER TABLE test.$table_name DROP COLUMN new_col;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			;;
		esac

		sleep 1
	done
}

function execute_dml() {
	table_name="table_$1"
	echo "DML: Inserting data into $table_name..."
	while true; do
		run_sql_ignore_error "INSERT INTO test.$table_name (data) VALUES ('$(date +%s)');" ${UP_TIDB_HOST} ${UP_TIDB_PORT} || true
	done
}

function move_table() {
	while true; do
		table_num=$((RANDOM % 10 + 1))
		table_name="table_$table_num"
		port=$((RANDOM % 3 + 8300))

		# move table to a random node
		table_id=$(get_table_id "test" "$table_name")
		move_table_with_retry "127.0.0.1:$port" $table_id "test" 10 || true
		sleep 1
	done
}

main() {
	prepare

	create_tables
	execute_ddls &
	NORMAL_TABLE_DDL_PID=$!

	# do execute dml for 100 tables, and store the pid for each thread
	declare -a pids=()

	for i in {1..10}; do
		execute_dml $i &
		pids+=("$!")
	done

	move_table &
	MOVE_TABLE_PID=$!

	sleep 500

	kill -9 $NORMAL_TABLE_DDL_PID ${pids[@]} $MOVE_TABLE_PID
	# wait for all dml threads to finish

	sleep 10

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 500

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
main
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
