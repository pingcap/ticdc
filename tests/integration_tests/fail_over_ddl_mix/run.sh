#!/bin/bash
# This test case is going to test the situation with dmls, ddls and random server down.
# The test case is as follows:
# 1. Start two ticdc servers.
# 2. Create 10 tables. And then randomly exec the ddls:
#    such as: drop table, and then create table
#             drop table, and then recover table
#             truncate table
# 3. Simultaneously we exec the dmls, continues insert data to these 10 tables.
# 4. Furthermore, we will randomly kill the ticdc server, and then restart it.
# 5. We execute these threads for a time, and then check the data consistency between the upstream and downstream.

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

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301"

	TOPIC_NAME="ticdc-failover-ddl-test-mix-$RANDOM"
	SINK_URI="mysql://root@127.0.0.1:3306/"

	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=3" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	do_retry 5 3 cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c "test" --config="$CUR/conf/$1.toml"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac
}

function create_tables() {
	## normal tables
	for i in {1..5}; do
		echo "Creating table table_$i..."
		run_sql "CREATE TABLE IF NOT EXISTS test.table_$i (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	## partition tables
	# for i in {6..10}; do
	# 	echo "Creating partition table_$i..."
	# 	run_sql "CREATE TABLE IF NOT EXISTS test.table_$i (
	# 		id INT AUTO_INCREMENT PRIMARY KEY,
	# 		data VARCHAR(255)
	# 	)
	# 	PARTITION BY RANGE (id) (
	# 		PARTITION p0 VALUES LESS THAN (200),
	# 		PARTITION p1 VALUES LESS THAN (1000),
	# 		PARTITION p2 VALUES LESS THAN MAXVALUE);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# done
}

function execute_ddl_for_normal_tables() {
	while true; do
		table_num=$((RANDOM % 5 + 1))
		table_name="table_$table_num"

		case $((RANDOM % 3)) in
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
		esac

		sleep 1
	done
}

# TODO: support partition test after support
function execute_ddl_for_partition_tables() {
	while true; do
		table_num=$((RANDOM % 5 + 6))
		table_name="table_$table_num"

		case $((RANDOM % 4)) in
		0)
			echo "DDL: Dropping And creating partition $table_name..."
			run_sql "ALTER TABLE test.$table_name DROP PARTITION p2;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			sleep 0.5
			run_sql "ALTER TABLE test.$table_name ADD PARTITION (PARTITION p2 VALUES LESS THAN MAXVALUE);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			;;
		1)
			echo "DDL: Truncating partition $table_name..."
			run_sql "ALTER TABLE test.$table_name TRUNCATE PARTITION p0;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			run_sql "ALTER TABLE test.$table_name TRUNCATE PARTITION p1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			run_sql "ALTER TABLE test.$table_name TRUNCATE PARTITION p2;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			;;
		2)
			echo "DDL: Removing and do partition $table_name..."
			run_sql "ALTER TABLE test.$table_name REMOVE PARTITIONING;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			sleep 0.5
			run_sql "ALTER TABLE test.$table_name PARTITION BY RANGE (id) (
				PARTITION p0 VALUES LESS THAN (200),
				PARTITION p1 VALUES LESS THAN (500),
				PARTITION p2 VALUES LESS THAN MAXVALUE);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			;;
		3)
			echo "DDL: Exchange Partition With normal Table $table_name..."
			exchange_table=$((RANDOM % 5 + 1))
			exchange_table_name="table_$exchange_table"
			run_sql_ignore_error "ALTER TABLE test.$table_name EXCHANGE PARTITION p0 with table test.$exchange_table_name" ${UP_TIDB_HOST} ${UP_TIDB_PORT} || true
			echo "Exchange Partition Finished"
			;;
		4)
			echo "DDL: REORGANIZE partition $table_name..."
			run_sql "ALTER TABLE test.$table_name REORGANIZE PARTITION p0 INTO (PARTITION p00 VALUES LESS THAN (50), PARTITION p01 VALUES LESS THAN (100), PARTITION p02 VALUES LESS THAN (200))" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			sleep 0.5
			run_sql "ALTER TABLE test.$table_name REORGANIZE PARTITION p00, p01, p02 INTO (PARTITION p0 VALUES LESS THAN (200))" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			;;
		esac
		sleep 1
	done
}

function execute_dml() {
	table_name="table_$1"
	execute_mixed_dml "$table_name" "${UP_TIDB_HOST}" "${UP_TIDB_PORT}"
}

function kill_server() {
	for count in {1..10}; do
		case $((RANDOM % 2)) in
		0)
			cdc_pid_1=$(pgrep -f "$CDC_BINARY.*--addr 127.0.0.1:8300")
			if [ -z "$cdc_pid_1" ]; then
				continue
			fi
			kill_cdc_pid $cdc_pid_1

			sleep 15
			run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-$count" --addr "127.0.0.1:8300"
			;;
		1)
			cdc_pid_2=$(pgrep -f "$CDC_BINARY.*--addr 127.0.0.1:8301")
			if [ -z "$cdc_pid_2" ]; then
				continue
			fi
			kill_cdc_pid $cdc_pid_2

			sleep 15
			run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1-$count" --addr "127.0.0.1:8301"
			;;
		esac
		sleep 15
	done
}

main() {
	prepare changefeed

	create_tables
	execute_ddl_for_normal_tables &
	NORMAL_TABLE_DDL_PID=$!
	# execute_ddl_for_partition_tables &
	# PARTITION_TABLE_DDL_PID=$!

	execute_dml 1 &
	DML_PID_1=$!
	execute_dml 2 &
	DML_PID_2=$!
	execute_dml 3 &
	DML_PID_3=$!
	execute_dml 4 &
	DML_PID_4=$!
	execute_dml 5 &
	DML_PID_5=$!
	# execute_dml 6 &
	# DML_PID_6=$!
	# execute_dml 7 &
	# DML_PID_7=$!
	# execute_dml 8 &
	# DML_PID_8=$!
	# execute_dml 9 &
	# DML_PID_9=$!
	# execute_dml 10 &
	# DML_PID_10=$!

	kill_server

	sleep 10

	# kill -9 $NORMAL_TABLE_DDL_PID $PARTITION_TABLE_DDL_PID $DML_PID_1 $DML_PID_2 $DML_PID_3 $DML_PID_4 $DML_PID_5 $DML_PID_6 $DML_PID_7 $DML_PID_8 $DML_PID_9 $DML_PID_10
	kill -9 $NORMAL_TABLE_DDL_PID $DML_PID_1 $DML_PID_2 $DML_PID_3 $DML_PID_4 $DML_PID_5

	sleep 10

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 500

	checkpoint1=$(cdc cli changefeed query -c "test" 2>&1 | grep -v "Command to ticdc" | jq '.checkpoint_tso')
	sleep 20
	checkpoint2=$(cdc cli changefeed query -c "test" 2>&1 | grep -v "Command to ticdc" | jq '.checkpoint_tso')

	if [[ "$checkpoint1" -eq "$checkpoint2" ]]; then
		echo "checkpoint is not changed"
		exit 1
	fi

	cleanup_process $CDC_BINARY
}

main_with_consistent() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	prepare consistent_changefeed

	create_tables
	execute_ddl_for_normal_tables &
	NORMAL_TABLE_DDL_PID=$!
	# execute_ddl_for_partition_tables &
	# PARTITION_TABLE_DDL_PID=$!

	execute_dml 1 &
	DML_PID_1=$!
	execute_dml 2 &
	DML_PID_2=$!
	execute_dml 3 &
	DML_PID_3=$!
	execute_dml 4 &
	DML_PID_4=$!
	execute_dml 5 &
	DML_PID_5=$!
	# execute_dml 6 &
	# DML_PID_6=$!
	# execute_dml 7 &
	# DML_PID_7=$!
	# execute_dml 8 &
	# DML_PID_8=$!
	# execute_dml 9 &
	# DML_PID_9=$!
	# execute_dml 10 &
	# DML_PID_10=$!

	kill_server

	sleep 10

	# kill -9 $NORMAL_TABLE_DDL_PID $PARTITION_TABLE_DDL_PID $DML_PID_1 $DML_PID_2 $DML_PID_3 $DML_PID_4 $DML_PID_5 $DML_PID_6 $DML_PID_7 $DML_PID_8 $DML_PID_9 $DML_PID_10
	kill -9 $NORMAL_TABLE_DDL_PID $DML_PID_1 $DML_PID_2 $DML_PID_3 $DML_PID_4 $DML_PID_5

	cleanup_process $CDC_BINARY
	# to ensure row changed events have been replicated to TiCDC
	sleep 10
	changefeed_id="test"
	storage_path="file://$WORK_DIR/redo"
	tmp_download_path=$WORK_DIR/cdc_data/redo/$changefeed_id
	rts=$(cdc redo meta --storage="$storage_path" --tmp-dir="$tmp_download_path" | grep -oE "resolved-ts:[0-9]+" | awk -F: '{print $2}')

	sed "s/<placeholder>/$rts/g" $CUR/conf/consistent_diff_config.toml >$WORK_DIR/consistent_diff_config.toml

	cat $WORK_DIR/consistent_diff_config.toml
	cdc redo apply --tmp-dir="$tmp_download_path/apply" --storage="$storage_path" --sink-uri="mysql://normal:123456@127.0.0.1:3306/" >$WORK_DIR/cdc_redo.log
	check_sync_diff $WORK_DIR $WORK_DIR/consistent_diff_config.toml
}

trap stop_tidb_cluster EXIT
main
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
stop_tidb_cluster
# FIXME: refactor redo apply
# main_with_consistent
# check_logs $WORK_DIR
# echo "[$(date)] <<<<<< run consistent test case $TEST_NAME success! >>>>>>"
