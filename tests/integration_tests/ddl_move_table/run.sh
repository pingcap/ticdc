#!/bin/bash
#
# This test verifies that moving a table dispatcher during an in-flight multi-table DDL barrier
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
	do_retry 5 3 cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c "$CHANGEFEED_ID"

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

	export GO_FAILPOINTS='github.com/pingcap/ticdc/maintainer/scheduler/StopBalanceScheduler=return(true);github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockOrWaitBeforeWrite=sleep(20000)'
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

	deployDiffConfig
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml 60
	rm -f $WORK_DIR/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap 'stop_tidb_cluster; collect_logs $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
