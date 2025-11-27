#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# Function to start CDC server
function start_cdc_server() {
	local suffix=$1
	echo "Starting CDC server with suffix: $suffix"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix $suffix --addr "127.0.0.1:8300"
}

# Function to generate DDL workload (table creation only)
function generate_ddl_workload() {
	local duration=$1
	local table_counter=1
	local end_time=$(($(date +%s) + duration))

	echo "Starting DDL workload for $duration seconds..."

	while [ $(date +%s) -lt $end_time ]; do
		table_name="test_table_$table_counter"

		# Create table (this triggers new dispatcher creation - the race condition target)
		echo "Creating table $table_name"
		run_sql "CREATE TABLE checkpoint_race_test.$table_name (
			id INT PRIMARY KEY AUTO_INCREMENT,
			data INT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

		# Store table name for data insertion thread
		echo "$table_name" >>"$WORK_DIR/created_tables.txt"

		table_counter=$((table_counter + 1))

		# Short interval between CREATE TABLE operations
		sleep 0.5
	done

	echo "DDL workload completed with $((table_counter - 1)) tables created"
}

# Function to insert data into created tables
function generate_data_insertion() {
	local duration=$1
	local end_time=$(($(date +%s) + duration))

	echo "Starting data insertion workload for $duration seconds..."

	while [ $(date +%s) -lt $end_time ]; do
		# Check if there are created tables to insert data into
		if [ -f "$WORK_DIR/created_tables.txt" ]; then
			# Get a random table from created tables
			table_name=$(shuf -n 1 "$WORK_DIR/created_tables.txt" 2>/dev/null || true)

			if [ -n "$table_name" ]; then
				# Insert more data per table (20 records instead of 5)
				for i in {1..20}; do
					run_sql "INSERT INTO checkpoint_race_test.$table_name (data) VALUES ($((RANDOM % 1000)));" ${UP_TIDB_HOST} ${UP_TIDB_PORT} || true
					# Track the data insertion for verification
					run_sql "INSERT INTO checkpoint_race_test.data_tracking (table_name, data_value) VALUES ('$table_name', $((RANDOM % 1000)));" ${UP_TIDB_HOST} ${UP_TIDB_PORT} || true
				done
			fi
		fi

		# Brief pause before next insertion batch
		sleep 0.2
	done

	echo "Data insertion workload completed"
}

# Function to simulate crashes
function simulate_crashes() {
	local duration=$1
	local crash_count=0
	local end_time=$(($(date +%s) + duration))

	echo "Starting crash simulation for $duration seconds..."

	while [ $(date +%s) -lt $end_time ]; do
		# Wait for some activity before crashing
		sleep $((2 + RANDOM % 3))

		echo "Simulating crash #$((crash_count + 1))"

		# Kill CDC processes
		cdc_pid_1=$(get_cdc_pid 127.0.0.1 8300)
		if [ -z "$cdc_pid_1" ]; then
			continue
		fi
		kill_cdc_pid $cdc_pid_1

		sleep 1

		# Restart CDC
		crash_count=$((crash_count + 1))
		start_cdc_server "restart_$crash_count"

		# Wait a bit for stabilization
		sleep 2
	done

	echo "Crash simulation completed with $crash_count crashes"
}

# This test verifies the checkpoint race condition fix by:
# 1. Creating frequent CREATE TABLE operations to trigger new dispatcher creation
# 2. Inserting data immediately after table creation (critical race window)
# 3. Simulating frequent CDC crashes during DDL processing
# 4. Verifying no data loss occurs after restarts
function run() {
	# This test focuses on the maintainer logic, works with all sink types
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	# Main test execution
	echo "=== Starting checkpoint race condition stress test ==="

	# Start initial CDC server
	start_cdc_server "initial"

	# Wait for initial stabilization
	sleep 5

	TOPIC_NAME="ticdc-checkpoint-race-ddl-crash-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=3" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://root@127.0.0.1:3306/" ;;
	esac

	do_retry 5 3 cdc_cli_changefeed create --sink-uri="$SINK_URI" -c "test"

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	# Create test database
	run_sql "CREATE DATABASE checkpoint_race_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE checkpoint_race_test.data_tracking (
		id INT PRIMARY KEY AUTO_INCREMENT,
		table_name VARCHAR(50),
		data_value INT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Run concurrent workloads with crash simulation
	test_duration=120 # 120 seconds of intensive testing

	# Initialize created tables file
	rm -f "$WORK_DIR/created_tables.txt"
	touch "$WORK_DIR/created_tables.txt"

	echo "Starting concurrent workloads..."
	# DDL workload: CREATE TABLE (race condition target)
	generate_ddl_workload $test_duration &
	DDL_PID=$!

	# Data insertion workload: Insert data into created tables (separate thread)
	generate_data_insertion $test_duration &
	DATA_PID=$!

	simulate_crashes $test_duration &
	CRASH_PID=$!

	# Wait for all background jobs to complete
	wait $DDL_PID
	wait $DATA_PID
	wait $CRASH_PID

	echo "All workloads completed"

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 100

	echo "=== Checkpoint race condition test completed successfully ==="
}

trap 'stop_tidb_cluster; collect_logs $WORK_DIR' EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
