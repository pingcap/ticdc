#!/bin/bash
# This case is used to test the migration from old architecture to new architecture
# The test will create changefeed with old arch, write some data, then switch to new arch,
# continue writing data, and finally check data consistency.

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	# Remove TICDC_NEWARCH to start with old architecture
	echo "Starting with old arch"
	unset TICDC_NEWARCH

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	cdc_pid_old=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')

	TOPIC_NAME="ticdc-migration-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	# Create changefeed with old architecture
	do_retry 5 3 run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	# Create test database and tables
	run_sql "DROP DATABASE IF EXISTS migration_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE migration_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE migration_test.t1 (id INT AUTO_INCREMENT PRIMARY KEY, val INT, col0 INT NOT NULL);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE migration_test.t2 (id INT AUTO_INCREMENT PRIMARY KEY, val INT, col0 INT NOT NULL);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Write initial data with old architecture
	echo "Writing initial data with old arch..."
	for i in {1..50}; do
		run_sql "INSERT INTO migration_test.t1 (val, col0) VALUES ($i, $i);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		run_sql "INSERT INTO migration_test.t2 (val, col0) VALUES ($i, $i);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	sleep 5

	# Stop old architecture CDC server
	echo "Stopping old arch CDC server..."
	kill_cdc_pid $cdc_pid_old
	sleep 3

	# Switch to new architecture
	echo "Switching to new arch..."
	export TICDC_NEWARCH=true

	# Start new architecture CDC server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "new-arch"
	cdc_pid_new=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')

	# Wait for new server to be ready
	sleep 5

	# Continue writing data with new architecture
	echo "Writing more data with new arch..."
	for i in {51..100}; do
		run_sql "INSERT INTO migration_test.t1 (val, col0) VALUES ($i, $i);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		run_sql "INSERT INTO migration_test.t2 (val, col0) VALUES ($i, $i);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	# Create finish mark table
	run_sql "CREATE TABLE migration_test.finish_mark (id INT AUTO_INCREMENT PRIMARY KEY, val INT DEFAULT 0, col0 INT NOT NULL);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Wait for synchronization
	sleep 15

	# Check data consistency
	echo "Checking data consistency..."
	check_table_exists migration_test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY

	echo "test_old_arch_compatibility completed successfully!"
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
