#!/bin/bash
# This test case is going to test the situation with dmls, ddls and random server down.
# The test case is as follows:
# 1. Start two ticdc servers.
# 2. Create 10 tables. And then randomly exec the ddls:
#    such as: drop table, and then create table
#             drop table, and then recover table
#             truncate table
# 3. Simultaneously we exec the dmls, continues insert data to these 10 tables.
# 4. Besides, we enable the syncpoint.
# 5. Furthermore, we will randomly kill the ticdc server, and then restart it.
# 6. We execute these threads for a time, and then check the data consistency between the upstream and downstream.

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

	TOPIC_NAME="ticdc-failover-ddl-test-mix-with-syncpoint-$RANDOM"
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
	do_retry 5 3 run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c "test" --config="$CUR/conf/changefeed.toml"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac
}

function create_tables() {
	for i in {1..5}; do
		echo "Creating table table_$i..."
		run_sql "CREATE TABLE IF NOT EXISTS test.table_$i (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
}

function execute_ddl() {
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

function execute_dml() {
	table_name="table_$1"
	echo "DML: Inserting data into $table_name..."
	while true; do
		run_sql_ignore_error "INSERT INTO test.$table_name (data) VALUES ('$(date +%s)');" ${UP_TIDB_HOST} ${UP_TIDB_PORT} || true
	done
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

			sleep 10
			run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-$count" --addr "127.0.0.1:8300"
			;;
		1)
			cdc_pid_2=$(pgrep -f "$CDC_BINARY.*--addr 127.0.0.1:8301")
			if [ -z "$cdc_pid_2" ]; then
				continue
			fi
			kill_cdc_pid $cdc_pid_2

			sleep 10
			run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1-$count" --addr "127.0.0.1:8301"
			;;
		esac
		sleep 15
	done
}

main() {
	prepare

	create_tables
	execute_ddl &
	DDL_PID=$!

	# 启动 DML 线程
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

	kill_server

	sleep 15

	kill -9 $DDL_PID $DML_PID_1 $DML_PID_2 $DML_PID_3 $DML_PID_4 $DML_PID_5

	sleep 15

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 500

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
main
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
