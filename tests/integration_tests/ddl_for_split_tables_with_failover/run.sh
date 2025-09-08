#!/bin/bash
# This test is aimed to test the ddl execution for split tables when server may crash.
# 1. we start two TiCDC servers, and create a table with some data and multiple regions.
# 2. we enable the split table param, and start a changefeed.
# 2. one thread we execute ddl randomly(including add column, drop column, rename table, add index, drop index)
# 3. one thread we execute dmls, and insert data to these table.
# 4. one thread randomly kill the server, and then restart it.
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

	run_sql_file $CUR/data/pre.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	TOPIC_NAME="ticdc-ddl-split-table-with-failover-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://root:@127.0.0.1:3306/" ;;
	esac
	do_retry 5 3 run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c "test" --config="$CUR/conf/changefeed.toml"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac
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
	execute_mixed_dml "$table_name" "${UP_TIDB_HOST}" "${UP_TIDB_PORT}"
}

function kill_server() {
	count=1
	while true; do
		case $((RANDOM % 3)) in
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
		count=$((count + 1))
		sleep 15
	done
}

main() {
	prepare

	execute_ddls &
	NORMAL_TABLE_DDL_PID=$!

	declare -a pids=()

	for i in {1..5}; do
		execute_dml $i &
		pids+=("$!")
	done

	kill_server &
	KILL_SERVER_PID=$!

	sleep 500

	kill -9 $NORMAL_TABLE_DDL_PID ${pids[@]} $KILL_SERVER_PID

	sleep 10

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 100

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
main
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
