#!/bin/bash
#
# This test verifies that moving a dispatcher/merging a dispatcher during an in-flight syncpoint does not
# restart the dispatcher from (syncpoint_ts - 1). Otherwise, the dispatcher may re-scan and
# re-apply events with commitTs <= syncpoint_ts while the table-trigger dispatcher is writing
# the syncpoint, which can break the snapshot consistency semantics.

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare

WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

DB_NAME="sp_move"
TABLE_NAME="t"
MERGE_TABLE_NAME="t_merge"
CHANGEFEED_ID="test"
dml_pid=0

deployConfig() {
	cat $CUR/conf/diff_config_part1.toml >$CUR/conf/diff_config.toml
	sed -i "s#output-dir = \"/tmp/tidb_cdc_test/in_flight_syncpoint_during_scheduling/sync_diff/output\"#output-dir = \"$WORK_DIR/sync_diff/output\"#g" $CUR/conf/diff_config.toml
	echo "snapshot = \"$1\"" >>$CUR/conf/diff_config.toml
	cat $CUR/conf/diff_config_part2.toml >>$CUR/conf/diff_config.toml
	echo "snapshot = \"$2\"" >>$CUR/conf/diff_config.toml
}

run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		echo "only mysql sink supports syncpoint record"
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300"

	SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1"
	cat >$WORK_DIR/changefeed_scheduler.toml <<EOF
[scheduler]
enable-table-across-nodes = true
region-threshold=1
region-count-per-span=10
EOF
	do_retry 5 3 cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c "$CHANGEFEED_ID" --config="$WORK_DIR/changefeed_scheduler.toml"

	run_sql "DROP DATABASE IF EXISTS ${DB_NAME};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE ${DB_NAME};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE ${DB_NAME};" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_sql "CREATE TABLE ${DB_NAME}.${TABLE_NAME} (id INT PRIMARY KEY, v INT);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO ${DB_NAME}.${TABLE_NAME} VALUES (1, 1), (2, 2), (3, 3);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "${DB_NAME}.${TABLE_NAME}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60

	# Prepare a split table for merge test. It should be blocked by syncpoint together with other tables.
	run_sql "CREATE TABLE ${DB_NAME}.${MERGE_TABLE_NAME} (id INT PRIMARY KEY, v INT);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO ${DB_NAME}.${MERGE_TABLE_NAME} VALUES (1, 1), (2, 2), (3, 3);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "${DB_NAME}.${MERGE_TABLE_NAME}" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 60

	merge_table_id=$(get_table_id "$DB_NAME" "$MERGE_TABLE_NAME")
	run_sql "SPLIT TABLE ${DB_NAME}.${MERGE_TABLE_NAME} BETWEEN (1) AND (100000) REGIONS 20;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	split_table_with_retry $merge_table_id "$CHANGEFEED_ID" 10
	query_dispatcher_count "127.0.0.1:8300" "$CHANGEFEED_ID" 3 100 ge

	# Restart node1 to enable failpoints:
	# - StopBalanceScheduler: keep the table on node1 until we explicitly move it.
	# - BlockOrWaitBeforeWrite: block syncpoint writing on the table-trigger dispatcher.
	cdc_pid_1=$(get_cdc_pid "$CDC_HOST" "$CDC_PORT")
	kill_cdc_pid $cdc_pid_1
	cleanup_process $CDC_BINARY

	export GO_FAILPOINTS='github.com/pingcap/ticdc/maintainer/scheduler/StopBalanceScheduler=return(true);github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockOrWaitBeforeWrite=pause'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-1" --addr "127.0.0.1:8300"

	check_coordinator_and_maintainer "127.0.0.1:8300" "$CHANGEFEED_ID" 60

	cdc_cli_changefeed pause --changefeed-id="$CHANGEFEED_ID"
	sleep 2
	cdc_cli_changefeed update --config="$CUR/conf/changefeed.toml" --changefeed-id="$CHANGEFEED_ID" --no-confirm
	cdc_cli_changefeed resume --changefeed-id="$CHANGEFEED_ID"

	# Start node2 for moving the table.
	export GO_FAILPOINTS='github.com/pingcap/ticdc/maintainer/scheduler/StopBalanceScheduler=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301"

	# Keep generating DML until a syncpoint is triggered.
	i=10
	while true; do
		run_sql "INSERT INTO ${DB_NAME}.${TABLE_NAME} (id, v) VALUES (${i}, ${i}) ON DUPLICATE KEY UPDATE v = v + 1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		i=$((i + 1))
		sleep 1
	done &
	dml_pid=$!

	# Wait for the table-trigger dispatcher to receive a syncpoint event and extract its commitTs.
	ensure 60 "grep -q \"dispatcher receive sync point event\" $WORK_DIR/cdc0-1.log"
	syncpoint_ts=$(grep "dispatcher receive sync point event" $WORK_DIR/cdc0-1.log | head -n 1 | grep -oE 'commitTs[^0-9]*[0-9]+' | head -n 1 | grep -oE '[0-9]+' || true)
	if [ -z "$syncpoint_ts" ]; then
		echo "failed to extract syncpoint commitTs from logs"
		exit 1
	fi
	echo "syncpoint_ts: $syncpoint_ts"

	# Ensure the table-trigger dispatcher has received the maintainer action to start writing this syncpoint.
	ensure 60 "grep \"pending event get the action\" $WORK_DIR/cdc0-1.log | grep -Eq \"(pendingEventCommitTs[^0-9]*${syncpoint_ts}.*innerAction=0|innerAction=0.*pendingEventCommitTs[^0-9]*${syncpoint_ts})\""

	# Merge the split table while the syncpoint is pending.
	merge_table_with_retry $merge_table_id "$CHANGEFEED_ID" 10
	ensure 60 "grep -q \"merge dispatcher uses pending block event to calculate start ts\" $WORK_DIR/cdc0-1.log"
	merge_line=$(grep "merge dispatcher uses pending block event to calculate start ts" $WORK_DIR/cdc0-1.log | grep -E "pendingCommitTs[^0-9]*${syncpoint_ts}" | tail -n 1 || true)
	if [ -z "$merge_line" ]; then
		echo "failed to find merge startTs decision log line"
		exit 1
	fi
	merge_start_ts=$(echo "$merge_line" | grep -oE 'startTs[^0-9]*[0-9]+' | tail -n 1 | grep -oE '[0-9]+' || true)
	merge_pending_is_syncpoint=$(echo "$merge_line" | grep -oE 'pendingIsSyncPoint[^a-zA-Z]*(true|false)' | tail -n 1 | grep -oE '(true|false)' | tail -n 1 || true)
	if [ "$merge_start_ts" != "$syncpoint_ts" ]; then
		echo "unexpected merged dispatcher startTs, got: $merge_start_ts, want: $syncpoint_ts"
		exit 1
	fi
	if [ "$merge_pending_is_syncpoint" != "true" ]; then
		echo "unexpected pendingIsSyncPoint, got: $merge_pending_is_syncpoint, want: true"
		exit 1
	fi

	kill $dml_pid || true
	wait $dml_pid || true

	table_id=$(get_table_id "$DB_NAME" "$TABLE_NAME")
	move_table_with_retry "127.0.0.1:8301" $table_id "$CHANGEFEED_ID" 10

	# The moved dispatcher must not start from (syncpoint_ts-1), otherwise it may replay events with commitTs <= syncpoint_ts.
	ensure 30 "grep \"new dispatcher created\" $WORK_DIR/cdc1.log | grep -q \"tableID: ${table_id}\""
	dispatcher_start_ts=$(grep "new dispatcher created" $WORK_DIR/cdc1.log | grep "tableID: ${table_id}" | tail -n 1 | grep -oE 'startTs[^0-9]*[0-9]+' | tail -n 1 | grep -oE '[0-9]+' || true)
	if [ "$dispatcher_start_ts" -lt "$syncpoint_ts" ]; then
		echo "unexpected dispatcher startTs, got: $dispatcher_start_ts, want >= $syncpoint_ts"
		exit 1
	fi

	# Restart node0 to disable BlockOrWaitBeforeWrite, so the pending syncpoint can be written downstream.
	cdc_pid_0=$(get_cdc_pid "$CDC_HOST" "$CDC_PORT")
	kill_cdc_pid $cdc_pid_0
	export GO_FAILPOINTS='github.com/pingcap/ticdc/maintainer/scheduler/StopBalanceScheduler=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-2" --addr "127.0.0.1:8300"

	# Wait until the syncpoint is written downstream, then validate snapshot consistency by sync_diff.
	ensure 120 "mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -N -s -e \"SELECT secondary_ts FROM tidb_cdc.syncpoint_v1 WHERE changefeed='default/${CHANGEFEED_ID}' AND primary_ts='${syncpoint_ts}';\" | grep -E '^[0-9]+'"
	secondary_ts=$(mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -N -s -e "SELECT secondary_ts FROM tidb_cdc.syncpoint_v1 WHERE changefeed='default/${CHANGEFEED_ID}' AND primary_ts='${syncpoint_ts}';" | tail -n 1)
	echo "secondary_ts: $secondary_ts"

	deployConfig "$syncpoint_ts" "$secondary_ts"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 120
	rm -f $CUR/conf/diff_config.toml

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
