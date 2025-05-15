#!/bin/bash
# This test is aimed to test the ddl execution for split tables when the table is sometimes merging, sometimes splitting.
# 1. we start two TiCDC servers, and create a table with some data and multiple regions.
# 2. we enable the split table param, and start a changefeed.
# 2. one thread we execute ddl randomly(including add column, drop column, rename table, add index, drop index)
# 3. one thread we execute dmls, and insert data to these table.
# 4. one thread we randomly merge table and then split table.
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

	export GO_FAILPOINTS='github.com/pingcap/ticdc/maintainer/scheduler/StopBalanceScheduler=return(true);github.com/pingcap/ticdc/maintainer/scheduler/StopSplitScheduler=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300"

	run_sql_file $CUR/data/pre.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/pre.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 20

	# make node0 to be maintainer
	sleep 10 
	export GO_FAILPOINTS='github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockorWaitBeforeWrite=pause'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301"

	SINK_URI="mysql://root@127.0.0.1:3306/"
	do_retry 5 3 run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c "test" --config="$CUR/conf/changefeed.toml"
}

function execute_ddls() {
	while true; do
		table_num=$((RANDOM % 5 + 1))
		table_name="table_$table_num"

		case $((RANDOM % 3)) in
		0)
			echo "DDL: Adding index and dropping index in $table_name..."
			run_sql "CREATE INDEX idx_data ON test.$table_name (data);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			sleep 0.5
			run_sql "DROP INDEX idx_data ON test.$table_name;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			;;
		1)
			echo "DDL: Renaming $table_name..."
			new_table_name="table_$(($table_num + 100))"
			run_sql "RENAME TABLE test.$table_name TO test.$new_table_name;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			sleep 0.5
			run_sql "RENAME TABLE test.$new_table_name TO test.$table_name;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			;;
		2)
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

function merge_and_split_table() {
	while true; do
		table_num=$((RANDOM % 5 + 1))
		table_name="table_$table_num"

		# move table to a random node
		table_id=$(get_table_id "test" "$table_name")
		merge_table_with_retry $table_id "test" 10 || true
		sleep 2
        split_table_with_retry $table_id "test" 10 || true
	done
}

# main() {
# 	prepare

# 	execute_ddls &
# 	NORMAL_TABLE_DDL_PID=$!

# 	declare -a pids=()

# 	for i in {1..5}; do
# 		execute_dml $i &
# 		pids+=("$!")
# 	done

# 	merge_and_split_table &
# 	MERGE_AND_SPLIT_TABLE_PID=$!

# 	sleep 500

# 	kill -9 $NORMAL_TABLE_DDL_PID ${pids[@]} $MERGE_AND_SPLIT_TABLE_PID

# 	sleep 10

# 	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 100

# 	cleanup_process $CDC_BINARY
# }

main() {
	echo "hello"
	prepare

	# move the table to node 2
	table_id=$(get_table_id "test" "table_1")
	move_split_table_with_retry "127.0.0.1:8301" $table_id "test" 10 || true

	run_sql "ALTER TABLE test.table_1 ADD COLUMN new_col INT;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_ignore_error "INSERT INTO test.table_1 (data) VALUES ('$(date +%s)');" ${UP_TIDB_HOST} ${UP_TIDB_PORT} || true

	# 等到都上报了，通知下游来写 ddl 并卡住
	sleep 10

	## merge and split make the dispatcher id changed
	merge_table_with_retry $table_id "test" 10 || true
	sleep 10
	split_table_with_retry $table_id "test" 10 || true


	# restart node2 to disable failpoint
	cdc_pid_1=$(ps aux | grep cdc | grep 8301 | awk '{print $2}')
	kill_cdc_pid $cdc_pid_1
	sleep 5
	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1-1" --addr "127.0.0.1:8301"

	sleep 20

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 20

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
main
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
