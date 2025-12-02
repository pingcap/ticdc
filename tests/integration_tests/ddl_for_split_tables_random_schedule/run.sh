#!/bin/bash
# This test is aimed to test the schedule process for split tables and non-split tables with dml
# 1. we start three TiCDC servers, and create 5 split tables and 5 non-split tables.
# 2. we enable the split table param, and start a changefeed.
# 3. we execute dmls to these table.
# finally, we check the data consistency between the upstream and downstream, and final dispatchers count of these tables,
# to make sure the schedule process is correct.

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
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "2" --addr "127.0.0.1:8302"

	run_sql_file $CUR/data/pre.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	TOPIC_NAME="ticdc-ddl-split-table-with-random-schedule-$RANDOM"
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
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" "$CUR/conf/$1.toml" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "$CUR/conf/$1.toml" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI --config "$CUR/conf/$1.toml" ;;
	esac
}

function execute_dml() {
	table_name="table_$1"
	execute_mixed_dml "$table_name" "${UP_TIDB_HOST}" "${UP_TIDB_PORT}"
}

main() {
	prepare changefeed

	declare -a pids=()

	for i in {1..10}; do
		execute_dml $i &
		pids+=("$!")
	done

	sleep 200

	kill -9 ${pids[@]}

	sleep 60

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 100

	query_dispatcher_count "127.0.0.1:8300" "test" 36 100 le # 6 * 5 + 5 + 1

	cdc_pid_1=$(get_cdc_pid 127.0.0.1 8300)
	if [ -z "$cdc_pid_1" ]; then
		echo "ERROR: cdc server 1 is not running"
		exit 1
	fi
	kill_cdc_pid $cdc_pid_1

	sleep 60

	query_dispatcher_count "127.0.0.1:8301" "test" 26 100 le # 4 * 5 + 5 + 1

	cleanup_process $CDC_BINARY
}

main_with_consistent() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	prepare consistent_changefeed

	declare -a pids=()

	for i in {1..10}; do
		execute_dml $i &
		pids+=("$!")
	done

	sleep 200

	kill -9 ${pids[@]}

	sleep 60

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 100

	query_dispatcher_count "127.0.0.1:8300" "test" 36 100 le 1 # 6 * 5 + 5 + 1

	cdc_pid_1=$(get_cdc_pid 127.0.0.1 8300)
	if [ -z "$cdc_pid_1" ]; then
		echo "ERROR: cdc server 1 is not running"
		exit 1
	fi
	kill_cdc_pid $cdc_pid_1

	sleep 60

	query_dispatcher_count "127.0.0.1:8301" "test" 26 100 le 1 # 4 * 5 + 5 + 1

	cleanup_process $CDC_BINARY
}

trap 'stop_tidb_cluster; collect_logs $WORK_DIR' EXIT
main
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
stop_tidb_cluster
main_with_consistent
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run consistent test case $TEST_NAME success! >>>>>>"
