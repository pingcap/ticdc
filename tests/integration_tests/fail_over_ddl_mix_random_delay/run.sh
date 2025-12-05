#!/bin/bash
# This test case mixes heavy DML traffic, random DDLs and CDC failover.
# Compared with fail_over_ddl_mix, this version randomly injects
# MySQLSinkExecDDLDelay failpoints (1~20s) and performs occasional CDC restarts
# while verifying data consistency at the end.

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
source $CUR/../_utils/execute_mixed_dml
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
TEST_DURATION=${TEST_DURATION:-300}

function start_cdc_instance() {
	local logsuffix=$1
	local addr=$2

	export GO_FAILPOINTS="github.com/pingcap/ticdc/pkg/sink/mysql/MySQLSinkExecDDLDelay=return(\"$((RANDOM % 5 + 1))\")"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "$logsuffix" --addr "$addr"
}

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	start_cdc_instance "0" "127.0.0.1:8300"
	start_cdc_instance "1" "127.0.0.1:8301"

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
	do_retry 5 3 cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c "test" --config="$CUR/conf/$1.toml"
}

function create_tables() {
	for i in {1..5}; do
		echo "Creating table table_$i..."
		run_sql "CREATE TABLE IF NOT EXISTS test.table_$i (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
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

		sleep 10
	done
}

function execute_dml() {
	table_name="table_$1"
	execute_mixed_dml "$table_name" "${UP_TIDB_HOST}" "${UP_TIDB_PORT}"
}

function random_failover_loop() {
	local duration=$1
	local end_time=$(($(date +%s) + duration))
	local restart_round=0

	while [ $(date +%s) -lt $end_time ]; do
		sleep $((RANDOM % 10 + 5))
		if ((RANDOM % 2 == 0)); then
			continue
		fi

		local target_port=8300
		local suffix="0"
		if ((RANDOM % 2 == 1)); then
			target_port=8301
			suffix="1"
		fi

		local cdc_pid=$(get_cdc_pid 127.0.0.1 $target_port)
		if [ -n "$cdc_pid" ]; then
			echo "Failover: killing CDC on port $target_port"
			kill_cdc_pid $cdc_pid
		fi

		sleep $((RANDOM % 5 + 3))
		restart_round=$((restart_round + 1))
		start_cdc_instance "$suffix-restart-$restart_round" "127.0.0.1:$target_port"
		sleep $((RANDOM % 5 + 3))
	done
}

function stop_background_jobs() {
	for pid in "$@"; do
		if [ -n "$pid" ] && kill -0 $pid 2>/dev/null; then
			kill $pid 2>/dev/null || true
			wait $pid 2>/dev/null || true
		fi
	done
}

function main() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		echo "Skip test since MySQL sink is required for failpoint coverage"
		return
	fi

	prepare changefeed

	create_tables
	execute_ddl_for_normal_tables &
	DDL_PID=$!

	DML_PIDS=()
	for i in {1..5}; do
		execute_dml $i &
		DML_PIDS+=($!)
	done

	random_failover_loop $TEST_DURATION &
	FAILOVER_PID=$!

	sleep $TEST_DURATION

	stop_background_jobs $DDL_PID ${DML_PIDS[@]}
	wait $FAILOVER_PID 2>/dev/null || true

	sleep 30

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 500

	checkpoint1=$(cdc_cli_changefeed query -c "test" 2>&1 | grep -v "Command to ticdc" | jq '.checkpoint_tso')
	sleep 20
	checkpoint2=$(cdc_cli_changefeed query -c "test" 2>&1 | grep -v "Command to ticdc" | jq '.checkpoint_tso')

	if [[ "$checkpoint1" -eq "$checkpoint2" ]]; then
		echo "checkpoint is not changed"
		exit 1
	fi

	export GO_FAILPOINTS=''

	cleanup_process $CDC_BINARY
}

function main_with_consistent() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		echo "Skip consistent test since MySQL sink is required for failpoint coverage"
		return
	fi

	prepare consistent_changefeed

	create_tables
	execute_ddl_for_normal_tables &
	DDL_PID=$!

	DML_PIDS=()
	for i in {1..5}; do
		execute_dml $i &
		DML_PIDS+=($!)
	done

	random_failover_loop $TEST_DURATION &
	FAILOVER_PID=$!

	sleep $TEST_DURATION

	stop_background_jobs $DDL_PID ${DML_PIDS[@]}
	wait $FAILOVER_PID 2>/dev/null || true

	# Wait to make sure replication catches up before checking redo progress.
	sleep 60

	if ((RANDOM % 2)); then
		# For rename table, modify column ddl, drop column, drop index and drop table ddl, the struct of table is wrong when appling snapshot.
		# see https://github.com/pingcap/tidb/issues/63464.
		# So we can't check sync_diff with snapshot.
		changefeed_id="test"
		storage_path="file://$WORK_DIR/redo"
		tmp_download_path=$WORK_DIR/cdc_data/redo/$changefeed_id
		current_tso=$(run_cdc_cli_tso_query $UP_PD_HOST_1 $UP_PD_PORT_1)
		ensure 50 check_redo_resolved_ts $changefeed_id $current_tso $storage_path $tmp_download_path/meta
		cleanup_process $CDC_BINARY

		cdc redo apply --log-level debug --tmp-dir="$tmp_download_path/apply" \
			--storage="$storage_path" \
			--sink-uri="mysql://normal:123456@127.0.0.1:3306/" >$WORK_DIR/cdc_redo.log
		check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300
	else
		sleep 30
		check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 1000
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
