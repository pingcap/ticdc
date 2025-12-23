#!/bin/bash
# This case is used to test the migration from old architecture to new architecture.
# It now covers both a basic upgrade flow and a scheduler-specific upgrade flow.

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
BASE_WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

reset_workdir() {
	local case_name=$1
	WORK_DIR=$BASE_WORK_DIR/$case_name
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
}

prepare_sink() {
	local scenario=$1
	TOPIC_NAME="ticdc-migration-test-${scenario}-$RANDOM"
	case $SINK_TYPE in
	kafka)
		SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760"
		run_kafka_consumer "$WORK_DIR" "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" "$CUR/conf/${scenario}_changefeed.toml"
		;;
	storage)
		SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		run_storage_consumer "$WORK_DIR" "$SINK_URI" "$CUR/conf/${scenario}_changefeed.toml" ""
		;;
	pulsar)
		run_pulsar_cluster "$WORK_DIR" normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		run_pulsar_consumer --upstream-uri "$SINK_URI" --config "$CUR/conf/${scenario}_changefeed.toml"
		;;
	*)
		SINK_URI="mysql://root@127.0.0.1:3306/"
		;;
	esac
}

run_basic_upgrade_case() {
	reset_workdir "basic"

	start_tidb_cluster --workdir "$WORK_DIR"
	cd "$WORK_DIR"

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	echo "Starting basic upgrade case with old arch"
	unset TICDC_NEWARCH

	run_cdc_server --workdir "$WORK_DIR" --binary $CDC_BINARY
	cdc_pid_old=$(get_cdc_pid "$CDC_HOST" "$CDC_PORT")

	prepare_sink "basic"

	do_retry 5 3 cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"

	run_sql "DROP DATABASE IF EXISTS migration_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE migration_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE migration_test.t1 (id INT AUTO_INCREMENT PRIMARY KEY, val INT, col0 INT NOT NULL);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE migration_test.t2 (id INT AUTO_INCREMENT PRIMARY KEY, val INT, col0 INT NOT NULL);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	echo "Writing initial data with old arch..."
	for i in {1..50}; do
		run_sql "INSERT INTO migration_test.t1 (val, col0) VALUES ($i, $i);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		run_sql "INSERT INTO migration_test.t2 (val, col0) VALUES ($i, $i);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	sleep 5

	echo "Stopping old arch CDC server..."
	kill_cdc_pid $cdc_pid_old
	sleep 3

	echo "Switching to new arch..."
	export TICDC_NEWARCH=true

	run_cdc_server --workdir "$WORK_DIR" --binary $CDC_BINARY --logsuffix "new-arch"

	sleep 5

	echo "Writing more data with new arch..."
	for i in {51..100}; do
		run_sql "INSERT INTO migration_test.t1 (val, col0) VALUES ($i, $i);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		run_sql "INSERT INTO migration_test.t2 (val, col0) VALUES ($i, $i);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	run_sql "CREATE TABLE migration_test.finish_mark (id INT AUTO_INCREMENT PRIMARY KEY, val INT DEFAULT 0, col0 INT NOT NULL);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	sleep 15

	echo "Checking data consistency for basic case..."
	check_table_exists migration_test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff "$WORK_DIR" "$CUR/conf/diff_config.toml"

	cleanup_process $CDC_BINARY
	stop_tidb_cluster
	export TICDC_NEWARCH=true

	echo "Basic upgrade case completed successfully!"
}

run_scheduler_upgrade_case() {
	reset_workdir "scheduler"

	start_tidb_cluster --workdir "$WORK_DIR"
	cd "$WORK_DIR"

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_sql_file "$CUR/data/scheduler_pre.sql" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file "$CUR/data/scheduler_pre.sql" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	echo "Starting scheduler upgrade case with old arch"
	unset TICDC_NEWARCH

	run_cdc_server --workdir "$WORK_DIR" --binary $CDC_BINARY
	cdc_pid_old=$(get_cdc_pid "$CDC_HOST" "$CDC_PORT")

	prepare_sink "scheduler"

	changefeedid="scheduler-upgrade"
	do_retry 5 3 cdc_cli_changefeed create \
		--start-ts=$start_ts \
		--sink-uri="$SINK_URI" \
		--config="$CUR/conf/scheduler_changefeed.toml" \
		-c $changefeedid

	check_sync_diff "$WORK_DIR" "$CUR/conf/scheduler_diff_config.toml"

	echo "Pausing changefeed before upgrade..."
	cdc_cli_changefeed pause -c $changefeedid
	sleep 3

	echo "Stopping old arch CDC server for scheduler case..."
	kill_cdc_pid $cdc_pid_old
	sleep 3

	echo "Switching scheduler case to new arch..."
	export TICDC_NEWARCH=true

	run_cdc_server --workdir "$WORK_DIR" --binary $CDC_BINARY --logsuffix "new-arch"

	sleep 5

	echo "Resuming changefeed after upgrade..."
	cdc_cli_changefeed resume -c $changefeedid

	sleep 10

	changefeed_cfg=$(cdc_cli_changefeed query -c $changefeedid | grep -v "Command to ticdc")
	region_count_per_span=$(echo "$changefeed_cfg" | jq -r '.config.scheduler.region_count_per_span')
	if [ "$region_count_per_span" = "0" ] || [ "$region_count_per_span" = "null" ]; then
		echo "region_count_per_span should be non-zero after upgrade but got $region_count_per_span"
		echo "$changefeed_cfg"
		exit 1
	fi

	echo "Writing data after resume..."
	for i in $(seq 1 200); do
		run_sql "INSERT INTO scheduler_upgrade.big_table (id, val) VALUES ($i, $i);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	sleep 20

	echo "Checking data consistency for scheduler case..."
	check_table_exists scheduler_upgrade.big_table ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff "$WORK_DIR" "$CUR/conf/scheduler_diff_config.toml"

	cleanup_process $CDC_BINARY
	stop_tidb_cluster
	export TICDC_NEWARCH=true

	echo "Scheduler upgrade case completed successfully!"
}

run() {
	if [ "$NEXT_GEN" = 1 ]; then
		exit 0
	fi

	run_basic_upgrade_case
	run_scheduler_upgrade_case
}

trap 'stop_test $BASE_WORK_DIR' EXIT
run "$@"
check_logs $BASE_WORK_DIR/basic
check_logs $BASE_WORK_DIR/scheduler
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
