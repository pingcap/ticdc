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
source $CUR/../_utils/execute_mixed_dml
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

	TOPIC_NAME="ticdc-ddl-with-random-move-table-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://root:@127.0.0.1:3306/" ;;
	esac
	do_retry 5 3 cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c "test" --config="$CUR/conf/$1.toml"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac
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
	execute_mixed_dml "$table_name" "${UP_TIDB_HOST}" "${UP_TIDB_PORT}"
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

function move_table_consistent() {
	while true; do
		table_num=$((RANDOM % 10 + 1))
		table_name="table_$table_num"
		port=$((RANDOM % 3 + 8300))

		# move table to a random node
		table_id=$(get_table_id "test" "$table_name")
		move_table_with_retry "127.0.0.1:$port" $table_id "test" 10 1 || true
		sleep 1
	done
}

main() {
	prepare changefeed

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

main_with_consistent() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	prepare consistent_changefeed

	create_tables
	execute_ddls &
	NORMAL_TABLE_DDL_PID=$!

	# do execute dml for 100 tables, and store the pid for each thread
	declare -a pids=()

	for i in {1..10}; do
		execute_dml $i &
		pids+=("$!")
	done

	move_table_consistent &
	MOVE_TABLE_PID=$!

	sleep 500

	kill -9 $NORMAL_TABLE_DDL_PID ${pids[@]} $MOVE_TABLE_PID
	# to ensure row changed events have been replicated to TiCDC
	sleep 20

	if ((RANDOM % 2)); then
		# For rename table, modify column ddl, drop column, drop index and drop table ddl, the struct of table is wrong when appling snapshot.
		# see https://github.com/pingcap/tidb/issues/63464.
		# So we can't check sync_diff with snapshot.
		changefeed_id="test"
		storage_path="file://$WORK_DIR/redo"
		tmp_download_path=$WORK_DIR/cdc_data/redo/$changefeed_id
		current_tso=$(run_cdc_cli_tso_query $UP_PD_HOST_1 $UP_PD_PORT_1)
		ensure 100 check_redo_resolved_ts $changefeed_id $current_tso $storage_path $tmp_download_path/meta
		cleanup_process $CDC_BINARY

		cdc redo apply --log-level debug --tmp-dir="$tmp_download_path/apply" \
			--storage="$storage_path" \
			--sink-uri="mysql://normal:123456@127.0.0.1:3306/" >$WORK_DIR/cdc_redo.log
		check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 100
	else
		check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300
		cleanup_process $CDC_BINARY
	fi
}

trap 'stop_tidb_cluster; collect_logs $WORK_DIR' EXIT
main
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
stop_tidb_cluster
main_with_consistent
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run consistent test case $TEST_NAME success! >>>>>>"
