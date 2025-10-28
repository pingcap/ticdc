#!/bin/bash
# This test case is going to test the situation with dmls, ddls and random server down.
# The test case is as follows:
# 1. Start two ticdc servers and inject failepoint EventDispatcherExecDDLFailed
# 2. Create 5 tables. And then randomly exec the ddls:
#    such as: rename table
#             add column, and then drop column
# 3. one thread we execute dmls, and insert data to these table.
# 4. Furthermore, we will randomly kill the ticdc server, and then restart it.
# 5. We execute these threads for a time, and then check the data consistency between the upstream and downstream.

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

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	export GO_FAILPOINTS='github.com/pingcap/ticdc/downstreamadapter/dispatcher/EventDispatcherExecDDLFailed=10%return(true)'

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301"

	TOPIC_NAME="ticdc-consistent-replicate-fail-over-ddl-$RANDOM"
	SINK_URI="mysql://root@127.0.0.1:3306/"

	do_retry 5 3 cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c "test" --config="$CUR/conf/changefeed.toml"
}

function create_tables() {
	## normal tables
	for i in {1..5}; do
		echo "Creating table table_$i..."
		run_sql "CREATE TABLE IF NOT EXISTS test.table_$i (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
}

function execute_ddls() {
	while true; do
		table_num=$((RANDOM % 5 + 1))
		table_name="table_$table_num"

		case $((RANDOM % 2)) in
		0)
			echo "DDL: Renaming $table_name..."
			new_table_name="table_$(($table_num + 100))"
			run_sql "RENAME TABLE test.$table_name TO test.$new_table_name;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			sleep 0.5
			run_sql "RENAME TABLE test.$new_table_name TO test.$table_name;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			;;
		1)
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
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	prepare

	create_tables
	execute_ddl_for_normal_tables &
	NORMAL_TABLE_DDL_PID=$!

	declare -a pids=()

	for i in {1..5}; do
		execute_dml $i &
		pids+=("$!")
	done

	kill_server

	sleep 10

	kill -9 $NORMAL_TABLE_DDL_PID ${pids[@]}

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
	# to ensure row changed events have been replicated to TiCDC
	sleep 10
	changefeed_id="test"
	storage_path="file://$WORK_DIR/redo"
	tmp_download_path=$WORK_DIR/cdc_data/redo/$changefeed_id
	rts=$(cdc redo meta --storage="$storage_path" --tmp-dir="$tmp_download_path" | grep -oE "resolved-ts:[0-9]+" | awk -F: '{print $2}')

	sed "s/<placeholder>/$rts/g" $CUR/conf/diff_config.toml >$WORK_DIR/diff_config.toml

	cat $WORK_DIR/diff_config.toml
	cdc redo apply --tmp-dir="$tmp_download_path/apply" --storage="$storage_path" --sink-uri="mysql://normal:123456@127.0.0.1:3306/" >$WORK_DIR/cdc_redo.log
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml
}

trap stop_tidb_cluster EXIT
main
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
