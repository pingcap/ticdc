#!/bin/bash
# This test is aimed to test the splitable check for split tables.
# 1. We start two TiCDC servers, and create a table with some data and multiple regions.
# 2. We enable the split table param, and start a changefeed.
# 3. We execute some normal DDLs and DMLs first (add/drop columns that don't break splitable).
# 4. Then we execute a DDL that will break splitable (add unique key), which should cause an error.
# 5. We verify that the changefeed reports the expected error.

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

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301"

	# Create tables and split them into multiple regions
	run_sql_file $CUR/data/pre.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/pre.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 500

	TOPIC_NAME="ticdc-split-table-check-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://root:@127.0.0.1:3306/" ;;
	esac
	sleep 10
	run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c "test" --config="$CUR/conf/$1.toml"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac
}

main() {
	prepare changefeed
	
	# Execute some normal DDLs and DMLs first (these should work fine)
	echo "Executing normal DDLs and DMLs..."
	run_sql_file $CUR/data/normal_ddls.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/dmls.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	
	# Wait a bit for the normal operations to complete
	sleep 10
	
	# Check that the changefeed is still running normally
	ensure 20 check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} test "normal" "null" ""
	
	# Now execute the DDL that will break splitable (this should cause an error)
	echo "Executing DDL that will break splitable..."
	run_sql_file $CUR/data/problematic_ddls.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	
	# Wait for the error to be detected and check that the changefeed has failed with the expected error
	ensure 20 check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} test "failed" "splitable" ""
	
	echo "SUCCESS: Changefeed correctly reported splitable violation error"
	
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
main
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
