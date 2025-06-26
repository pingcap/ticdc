#!/bin/bash
# This test is aimed to test the ddl execution for split tables when one table is split and merge
# 1. we start two TiCDC servers, and create a table with  multiple regions.
# 2. we enable the split table param, and start a changefeed, and write a ddl and dml event.
# 3. we use failpoint to block the dispatcher write ddl to downstream once.
# 4. then we do merge and split to change the dispatcher id.
# 5. we restart the cdc server to disable the failpoint.
# 6. we check the data consistency between the upstream and downstream.

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

	cd $WORK_DIR

	export GO_FAILPOINTS='github.com/pingcap/ticdc/maintainer/scheduler/StopBalanceScheduler=return(true);github.com/pingcap/ticdc/maintainer/scheduler/StopSplitScheduler=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300"

	run_sql_file $CUR/data/pre.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/pre.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 20

	# make node0 to be maintainer
	sleep 10
	export GO_FAILPOINTS='github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockOrWaitBeforeWrite=pause'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301"

	TOPIC_NAME="ticdc-ddl-split-table-with-merge-and-split-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://root:@127.0.0.1:3306/" ;;
	esac
	do_retry 5 3 run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c "test" --config="$CUR/conf/changefeed.toml"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac
}

main() {
	prepare

	# move the table to node 2
	table_id=$(get_table_id "test" "table_1")
	move_split_table_with_retry "127.0.0.1:8301" $table_id "test" 10 || true

	run_sql "ALTER TABLE test.table_1 ADD COLUMN new_col INT;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_ignore_error "INSERT INTO test.table_1 (data) VALUES ('$(date +%s)');" ${UP_TIDB_HOST} ${UP_TIDB_PORT} || true

	sleep 10

	## merge and split make the dispatcher id changed
	merge_table_with_retry $table_id "test" 10 || true
	sleep 10
	split_table_with_retry $table_id "test" 10 || true

	sleep 10

	# restart node2 to disable failpoint
	cdc_pid_1=$(ps aux | grep cdc | grep 8301 | awk '{print $2}')
	kill_cdc_pid $cdc_pid_1
	sleep 5
	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1-1" --addr "127.0.0.1:8301"

	sleep 20

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 20

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
main
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
