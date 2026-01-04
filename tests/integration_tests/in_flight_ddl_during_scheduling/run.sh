#!/bin/bash
#
# This test verifies that moving a dispatcher/merge a dispatcher during an in-flight multi-table DDL barrier
# does not cause the moved dispatcher to miss the DDL, and that the recreated dispatcher starts
# from (blockTs-1) with skipDMLAsStartTs enabled.

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare

WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

DB_NAME="ddl_move"
TABLE_1="t1"
TABLE_2="t2"
TABLE_1_NEW="t1_new"
TABLE_2_NEW="t2_new"
CHANGEFEED_ID="test"

deployDiffConfig() {
	cat >$WORK_DIR/diff_config.toml <<EOF
check-thread-count = 4
export-fix-sql = true
check-struct-only = false

[task]
    output-dir = "$WORK_DIR/sync_diff/output"
    source-instances = ["mysql1"]
    target-instance = "tidb0"
    target-check-tables = ["${DB_NAME}.*"]

[data-sources]
[data-sources.mysql1]
    host = "127.0.0.1"
    port = 4000
    user = "root"
    password = ""

[data-sources.tidb0]
    host = "127.0.0.1"
    port = 3306
    user = "root"
    password = ""
EOF
}

run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		echo "only mysql sink is supported"
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	export GO_FAILPOINTS='github.com/pingcap/ticdc/maintainer/scheduler/StopBalanceScheduler=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300"

	SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1"
	cat >$WORK_DIR/changefeed.toml <<EOF
[scheduler]
enable-table-across-nodes = true
region-threshold=1
region-count-per-span=10
EOF
	do_retry 5 3 cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c "$CHANGEFEED_ID" --config="$WORK_DIR/changefeed.toml"

	run_sql "DROP DATABASE IF EXISTS ${DB_NAME};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE ${DB_NAME};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE ${DB_NAME};" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_sql "CREATE TABLE ${DB_NAME}.${TABLE_1} (id INT PRIMARY KEY, v INT);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE ${DB_NAME}.${TABLE_2} (id INT PRIMARY KEY, v INT);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO ${DB_NAME}.${TABLE_1} VALUES (1, 1), (2, 2), (3, 3);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO ${DB_NAME}.${TABLE_2} VALUES (1, 10), (2, 20), (3, 30);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_table_exists "${DB_NAME}.${TABLE_1}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60
	check_table_exists "${DB_NAME}.${TABLE_2}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60

	table_1_id=$(get_table_id "$DB_NAME" "$TABLE_1")

	# Restart node0 to enable failpoints:
	# - StopBalanceScheduler: keep the tables on node0 until we explicitly move them.
	# - BlockOrWaitBeforeWrite: block DDL writing on the table-trigger dispatcher.
	cdc_pid_0=$(get_cdc_pid "$CDC_HOST" "$CDC_PORT")
	kill_cdc_pid $cdc_pid_0

	export GO_FAILPOINTS='github.com/pingcap/ticdc/maintainer/scheduler/StopBalanceScheduler=return(true);github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockOrWaitBeforeWrite=pause'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-1" --addr "127.0.0.1:8300"
	check_coordinator_and_maintainer "127.0.0.1:8300" "$CHANGEFEED_ID" 60

	# Start node1 for moving the table.
	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301"

	# Execute a multi-table DDL to trigger the barrier and wait for the writer action.
	run_sql "RENAME TABLE ${DB_NAME}.${TABLE_1} TO ${DB_NAME}.${TABLE_1_NEW}, ${DB_NAME}.${TABLE_2} TO ${DB_NAME}.${TABLE_2_NEW};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	ensure 60 "grep \"pending event get the action\" $WORK_DIR/cdc0-1.log | grep -q \"innerAction=0\""
	ddl_ts=$(grep "pending event get the action" $WORK_DIR/cdc0-1.log | grep "innerAction=0" | head -n 1 | grep -oE 'pendingEventCommitTs[^0-9]*[0-9]+' | head -n 1 | grep -oE '[0-9]+' || true)
	if [ -z "$ddl_ts" ]; then
		echo "failed to extract DDL commitTs from logs"
		exit 1
	fi
	echo "ddl_ts: $ddl_ts"
	expected_start_ts=$((ddl_ts - 1))

	move_table_with_retry "127.0.0.1:8301" $table_1_id "$CHANGEFEED_ID" 10

	# The moved dispatcher must start from (ddl_ts - 1) and enable skipDMLAsStartTs.
	ensure 60 "grep \"new dispatcher created\" $WORK_DIR/cdc1.log | grep -q \"tableID: ${table_1_id}\""
	dispatcher_line=$(grep "new dispatcher created" $WORK_DIR/cdc1.log | grep "tableID: ${table_1_id}" | tail -n 1)
	dispatcher_start_ts=$(echo "$dispatcher_line" | grep -oE 'startTs[^0-9]*[0-9]+' | tail -n 1 | grep -oE '[0-9]+' || true)
	dispatcher_skip_dml=$(echo "$dispatcher_line" | grep -oE 'skipDMLAsStartTs[^a-zA-Z]*(true|false)' | tail -n 1 | grep -oE '(true|false)' | tail -n 1 || true)

	if [ "$dispatcher_start_ts" != "$expected_start_ts" ]; then
		echo "unexpected dispatcher startTs, got: $dispatcher_start_ts, want: $expected_start_ts"
		exit 1
	fi
	if [ "$dispatcher_skip_dml" != "true" ]; then
		echo "unexpected skipDMLAsStartTs, got: $dispatcher_skip_dml, want: true"
		exit 1
	fi

	cdc_pid_0=$(get_cdc_pid "127.0.0.1" "8300")
	kill_cdc_pid $cdc_pid_0
	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-2" --addr "127.0.0.1:8300"

	# Wait for DDL to finish downstream.
	check_table_exists "${DB_NAME}.${TABLE_1_NEW}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
	check_table_exists "${DB_NAME}.${TABLE_2_NEW}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
	check_table_not_exists "${DB_NAME}.${TABLE_1}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
	check_table_not_exists "${DB_NAME}.${TABLE_2}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120

	# Verify DML after DDL is replicated correctly.
	run_sql "INSERT INTO ${DB_NAME}.${TABLE_1_NEW} VALUES (4, 4);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "UPDATE ${DB_NAME}.${TABLE_1_NEW} SET v = v + 1 WHERE id = 1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO ${DB_NAME}.${TABLE_2_NEW} VALUES (4, 40);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	ensure 60 "mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -N -s -e \"SELECT v FROM ${DB_NAME}.${TABLE_1_NEW} WHERE id=1\" | grep -q '^2$'"
	ensure 60 "mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -N -s -e \"SELECT COUNT(*) FROM ${DB_NAME}.${TABLE_1_NEW}\" | grep -q '^4$'"
	ensure 60 "mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -N -s -e \"SELECT COUNT(*) FROM ${DB_NAME}.${TABLE_2_NEW}\" | grep -q '^4$'"

	# Merge the split table when a single-table DDL is in-flight.
	# The merged dispatcher must start from (ddl_ts - 1) and enable skipDMLAsStartTs to safely replay the DDL at ddl_ts.
	run_sql "SPLIT TABLE ${DB_NAME}.${TABLE_1_NEW} BETWEEN (1) AND (100000) REGIONS 20;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	split_table_with_retry $table_1_id "$CHANGEFEED_ID" 10
	query_dispatcher_count "127.0.0.1:8300" "$CHANGEFEED_ID" 3 100 ge

	export GO_FAILPOINTS='github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockOrWaitBeforeWrite=pause;github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockOrWaitBeforePass=pause'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "2" --addr "127.0.0.1:8302"
	move_split_table_with_retry "127.0.0.1:8302" $table_1_id "$CHANGEFEED_ID" 10

	merge_log=$WORK_DIR/cdc2.log
	touch $merge_log
	merge_log_offset=$(wc -l <"$merge_log")
	run_sql "ALTER TABLE ${DB_NAME}.${TABLE_1_NEW} ADD COLUMN c_merge INT DEFAULT 0;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	ensure 60 "tail -n +$((merge_log_offset + 1)) \"$merge_log\" | grep -q \"pending event get the action\""
	ddl_merge_ts=$(tail -n +$((merge_log_offset + 1)) "$merge_log" | grep "pending event get the action" | head -n 1 | grep -oE 'pendingEventCommitTs[^0-9]*[0-9]+' | head -n 1 | grep -oE '[0-9]+' || true)
	if [ -z "$ddl_merge_ts" ]; then
		echo "failed to extract DDL commitTs for merge from logs"
		exit 1
	fi
	expected_merge_start_ts=$((ddl_merge_ts - 1))

	merge_table_with_retry $table_1_id "$CHANGEFEED_ID" 10
	ensure 60 "grep -q \"merge dispatcher uses pending block event to calculate start ts\" \"$merge_log\""
	merge_line=$(grep "merge dispatcher uses pending block event to calculate start ts" "$merge_log" | grep -E "pendingCommitTs[^0-9]*${ddl_merge_ts}" | tail -n 1 || true)
	if [ -z "$merge_line" ]; then
		echo "failed to find merge startTs decision log line"
		exit 1
	fi
	merge_start_ts=$(echo "$merge_line" | grep -oE 'startTs[^0-9]*[0-9]+' | tail -n 1 | grep -oE '[0-9]+' || true)
	merge_pending_is_syncpoint=$(echo "$merge_line" | grep -oE 'pendingIsSyncPoint[^a-zA-Z]*(true|false)' | tail -n 1 | grep -oE '(true|false)' | tail -n 1 || true)
	merge_skip_dml=$(echo "$merge_line" | grep -oE 'skipDMLAsStartTs[^a-zA-Z]*(true|false)' | tail -n 1 | grep -oE '(true|false)' | tail -n 1 || true)
	if [ "$merge_start_ts" != "$expected_merge_start_ts" ]; then
		echo "unexpected merged dispatcher startTs, got: $merge_start_ts, want: $expected_merge_start_ts"
		exit 1
	fi
	if [ "$merge_pending_is_syncpoint" != "false" ]; then
		echo "unexpected pendingIsSyncPoint, got: $merge_pending_is_syncpoint, want: false"
		exit 1
	fi
	if [ "$merge_skip_dml" != "true" ]; then
		echo "unexpected merged dispatcher skipDMLAsStartTs, got: $merge_skip_dml, want: true"
		exit 1
	fi

	cdc_pid_2=$(get_cdc_pid "127.0.0.1" "8302")
	kill_cdc_pid $cdc_pid_2
	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "2-1" --addr "127.0.0.1:8302"
	ensure 120 "mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -N -s -e \"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='${DB_NAME}' AND table_name='${TABLE_1_NEW}' AND column_name='c_merge';\" | grep -q '^1$'"

	deployDiffConfig
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml 60
	rm -f $WORK_DIR/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
