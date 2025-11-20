#!/bin/bash
# this test case is used to test the multi changefeeds in multi servers
# to check whether the changefeeds can be scheduling normally.

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
CHANGEFEED_COUNT=6

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	# Start first CDC server (single node)
	echo "Starting first CDC server..."
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300"

	# Create sink URI based on sink type
	TOPIC_NAME="ticdc-autorandom-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	# Create 6 changefeeds using the existing configs
	changefeed_ids=()
	for i in $(seq 1 $CHANGEFEED_COUNT); do
		config_file="$CUR/conf/changefeed$i.toml"
		cdc_cli_changefeed create --sink-uri="$SINK_URI" --config="$config_file" -c "test$i"
	done

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	# Create databases and tables for each changefeed
	for i in $(seq 1 $CHANGEFEED_COUNT); do
		run_sql "CREATE DATABASE test$i;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		run_sql "CREATE TABLE test$i.t$i (id int primary key auto_increment, data varchar(255), created_at timestamp DEFAULT CURRENT_TIMESTAMP)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	# Start data writing in background
	echo "Starting data writing process..."
	(
		round=0
		while true; do
			((round++))
			for i in $(seq 1 $CHANGEFEED_COUNT); do
				# Insert data into each table
				run_sql "INSERT INTO test$i.t$i (data) VALUES ('round_${round}_data_$i'), ('round_${round}_extra_$i')" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			done
		done
	) &
	DATA_WRITER_PID=$!

	# Start second CDC server (second node)
	echo "Starting second CDC server..."
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8301"

	# Wait for coordinator to balance changefeeds
	sleep 15

	# Start third CDC server (third node)
	echo "Starting third CDC server..."
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8302"

	# Wait for coordinator to rebalance changefeeds across 3 nodes
	sleep 20

	# Continue writing data for a while with 3 nodes
	sleep 90

	# Stop one CDC server (simulate node failure)
	echo "Stopping one CDC server to simulate node failure..."
	# Find and kill one of the CDC processes (not the first one)
	CDC_PIDS=$(get_cdc_pid 127.0.0.1 8301)
	if [ ! -z "$CDC_PIDS" ]; then
		FIRST_PID=$(echo $CDC_PIDS | awk '{print $1}')
		kill $FIRST_PID
		echo "Killed CDC server with PID: $FIRST_PID"
	fi

	# Wait for coordinator to rebalance changefeeds to remaining 2 nodes
	sleep 30

	# Stop data writing
	kill $DATA_WRITER_PID 2>/dev/null || true

	# Wait for all data to be processed
	sleep 30

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 500
	cleanup_process $CDC_BINARY
}

trap 'stop_tidb_cluster; collect_logs $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
