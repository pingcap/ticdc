#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function generateReplaySQL() {
	echo "USE mysql_conflict_keys;" >"$WORK_DIR/initial.sql"
	echo "USE mysql_conflict_keys;" >"$WORK_DIR/replay.sql"
	echo "SET autocommit = 1;" >>"$WORK_DIR/replay.sql"
	local i table prefix valueA valueB
	for i in $(seq 1 128); do
		prefix=$(printf '%04d' "$i")
		for table in accent pad_space prefix virtual_generated stored_generated; do
			case $table in
			accent)
				valueA="'${i}e'"
				valueB="'${i}é'"
				;;
			pad_space)
				valueA="'${i}key'"
				valueB="'${i}key '"
				;;
			prefix)
				valueA="'${prefix}-a'"
				valueB="'${prefix}-b'"
				;;
			*)
				valueA="REPEAT('a', $i)"
				valueB="REPEAT('b', $i)"
				;;
			esac
			# Each group has independent PKs and UKs, so later groups cannot
			# repair a missing final row. Each statement commits separately.
			echo "INSERT INTO $table (id,u) VALUES ($((i * 3 - 2)), $valueA);" >>"$WORK_DIR/initial.sql"
			cat >>"$WORK_DIR/replay.sql" <<SQL
DELETE FROM $table WHERE id = $((i * 3 - 2));
INSERT INTO $table (id,u) VALUES ($((i * 3 - 1)), $valueB);
DELETE FROM $table WHERE id = $((i * 3 - 1));
INSERT INTO $table (id,u) VALUES ($((i * 3)), $valueA);
SQL
		done
	done
}

function runCase() {
	local mode=$1
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# Flush each event separately so INSERT/DELETE pairs cannot be folded
	# away by batching, and exercise REPLACE with multiple DML workers.
	SINK_URI="mysql://normal:123456@127.0.0.1:3306/?safe-mode=true&worker-count=4&max-txn-row=1&batch-dml-enable=false"
	if [ "$mode" = "redo" ]; then
		cdc_cli_changefeed create -c mysql-conflict-keys --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"
	else
		cdc_cli_changefeed create -c mysql-conflict-keys --sink-uri="$SINK_URI"
	fi
	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	generateReplaySQL
	run_sql_file "$WORK_DIR/initial.sql" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	for table in accent pad_space prefix virtual_generated stored_generated; do
		check_table_exists mysql_conflict_keys.$table ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	# Accumulate the whole transaction sequence before replay, allowing the
	# conflict detector to dispatch dependent transactions to its workers.
	cdc_cli_changefeed pause -c mysql-conflict-keys
	if [ "$mode" = "redo" ]; then
		cleanup_process $CDC_BINARY
		export GO_FAILPOINTS='github.com/pingcap/ticdc/pkg/sink/mysql/MySQLSinkHangLongTime=return(true)'
		run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	fi
	run_sql_file "$WORK_DIR/replay.sql" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	for table in accent pad_space prefix virtual_generated stored_generated; do
		result=$(mysql -uroot -h${UP_TIDB_HOST} -P${UP_TIDB_PORT} -Nse \
			"SELECT COUNT(*) = 128 AND SUM(MOD(id, 3) = 0) = 128 FROM mysql_conflict_keys.$table")
		if [ "$result" != "1" ]; then
			echo "unexpected upstream rows in $table"
			exit 1
		fi
	done
	local replayTS checkpoint=0
	replayTS=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	cdc_cli_changefeed resume -c mysql-conflict-keys
	if [ "$mode" = "redo" ]; then
		local storage="file://$WORK_DIR/redo"
		ensure 30 check_redo_resolved_ts mysql-conflict-keys "$replayTS" "$storage" "$WORK_DIR/redo_meta"
		cleanup_process $CDC_BINARY
		export GO_FAILPOINTS=''
		# The sink must still contain the initial rows, so only redo apply
		# can bring the downstream to the final state.
		for table in accent pad_space prefix virtual_generated stored_generated; do
			result=$(mysql -uroot -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -Nse \
				"SELECT COUNT(*) = 128 AND SUM(MOD(id, 3) = 1) = 128 FROM mysql_conflict_keys.$table")
			if [ "$result" != "1" ]; then
				echo "MySQL writes were not blocked for $table before redo apply"
				exit 1
			fi
		done
		$CDC_BINARY redo apply --tmp-dir="$WORK_DIR/redo_apply" \
			--storage="$storage" --sink-uri="$SINK_URI" >"$WORK_DIR/cdc_redo.log" 2>&1
		check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 1
	else
		# Wait for every replayed transaction to finish before comparing data;
		# a transient match must not hide an older REPLACE still in flight.
		for attempt in $(seq 1 60); do
			checkpoint=$(cdc_cli_changefeed query -c mysql-conflict-keys | grep -v "Command to ticdc" | jq -r '.checkpoint_tso // 0')
			if [ "$checkpoint" -ge "$replayTS" ]; then
				break
			fi
			sleep 1
		done
		if [ "$checkpoint" -lt "$replayTS" ]; then
			echo "changefeed did not finish replay: checkpoint=$checkpoint, target=$replayTS"
			exit 1
		fi
		check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
		cdc_cli_changefeed remove -c mysql-conflict-keys
		cleanup_process $CDC_BINARY
	fi
	cp "$WORK_DIR/sync_diff/output/summary.txt" "$WORK_DIR/${mode}_summary.log"
}

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR --tidb-config $CUR/conf/tidb_config.toml
	runCase normal
	run_sql "DROP DATABASE mysql_conflict_keys;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "DROP DATABASE mysql_conflict_keys;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	runCase redo
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
