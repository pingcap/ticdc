#!/bin/bash
# This test is designed to verify system stability when randomly dropping remote messages.
# Test procedure:
# 1. Start 3 CDC nodes with failpoint enabled to randomly drop 10% of remote messages
# 2. Create a changefeed and continuously execute mixed DML operations on 10 tables
# 3. After 100 seconds, kill one CDC node and restart it after 20 seconds
# 4. Continue running for another 200 seconds (total 320 seconds)
# 5. Check data consistency between upstream and downstream

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
source $CUR/../_utils/execute_mixed_dml
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
TABLE_COUNT=10
KILL_DELAY=100     # Kill node after 100 seconds
RESTART_DELAY=20   # Restart node after 20 seconds
TEST_DURATION=320  # Total duration: 100 + 20 + 200 = 320 seconds

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_sql_file $CUR/data/pre.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Enable failpoint to randomly drop 10% of remote messages
	export GO_FAILPOINTS='github.com/pingcap/ticdc/pkg/messaging/InjectDropRemoteMessage=10%return(true)'

	# Start 3 CDC servers
	for i in $(seq 1 $CDC_COUNT); do
		run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "$i" --addr "127.0.0.1:830${i}"
	done

	TOPIC_NAME="ticdc-random-drop-message-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --server="127.0.0.1:8301"

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac
}

function execute_dml() {
	local table_id=$1
	execute_mixed_dml "test_table_${table_id}" "${UP_TIDB_HOST}" "${UP_TIDB_PORT}"
}

function kill_and_restart_node() {
	# Wait for KILL_DELAY seconds before killing a node
	echo "[$(date)] Waiting $KILL_DELAY seconds before killing CDC node..."
	sleep $KILL_DELAY

	# Kill the second CDC node (127.0.0.1:8302)
	echo "[$(date)] Killing CDC node at 127.0.0.1:8302..."
	cdc_pid=$(pgrep -f "$CDC_BINARY.*--addr 127.0.0.1:8302")
	if [ -n "$cdc_pid" ]; then
		kill_cdc_pid $cdc_pid
		echo "[$(date)] CDC node killed, PID: $cdc_pid"
	else
		echo "[$(date)] CDC node not found, skipping kill"
	fi

	# Wait for RESTART_DELAY seconds before restarting
	echo "[$(date)] Waiting $RESTART_DELAY seconds before restarting CDC node..."
	sleep $RESTART_DELAY

	# Restart the CDC node
	echo "[$(date)] Restarting CDC node at 127.0.0.1:8302..."
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "2-restart" --addr "127.0.0.1:8302"
	echo "[$(date)] CDC node restarted"
}

main() {
	prepare

	# Start continuous DML operations for all 10 tables in background
	declare -a dml_pids=()
	for i in $(seq 1 $TABLE_COUNT); do
		execute_dml $i &
		dml_pids+=("$!")
		echo "[$(date)] Started DML operations for test_table_$i, PID: $!"
	done

	# Kill and restart one CDC node in background
	kill_and_restart_node &
	KILL_RESTART_PID=$!

	# Run test for the specified duration
	echo "[$(date)] Test will run for $TEST_DURATION seconds..."
	sleep $TEST_DURATION

	# Stop background processes
	echo "[$(date)] Stopping background processes..."
	kill -9 ${dml_pids[@]} $KILL_RESTART_PID 2>/dev/null || true

	# Wait for data to be fully synchronized
	echo "[$(date)] Waiting 10 seconds for data synchronization..."
	sleep 10

	# Check data consistency
	echo "[$(date)] Checking data consistency..."
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 100

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
main
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
