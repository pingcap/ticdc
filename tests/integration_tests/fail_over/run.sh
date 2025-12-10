#!/bin/bash
# This case is used to test the fail-over feature of TiCDC.
# The test just test the basic normal fail-over process
# (kill cdc node and start it again | two node, kill one and restart this one )

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

## One TiCDC, and kill/restart the cdc node
function failOverOnlyOneNode() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	export GO_FAILPOINTS='github.com/pingcap/ticdc/downstreamadapter/dispatcher/HandleEventsSlowly=return(true)'

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	cdc_pid=$(get_cdc_pid "$CDC_HOST" "$CDC_PORT")

	TOPIC_NAME="ticdc-failover-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	do_retry 5 3 cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	sleep 1

	kill_cdc_pid $cdc_pid
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	sleep 15

	check_table_exists fail_over_test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 120

	export GO_FAILPOINTS=''

	cleanup_process $CDC_BINARY
	stop_tidb_cluster
}

## Two TiCDC, failover one node and then failover the other node
function failOverWhenTwoNode() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	export GO_FAILPOINTS='github.com/pingcap/ticdc/downstreamadapter/dispatcher/HandleEventsSlowly=return(true)'

	## server 1
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300"
	cdc_pid_1=$(get_cdc_pid "$CDC_HOST" "$CDC_PORT")
	echo $cdc_pid_1
	## server 2
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301"

	TOPIC_NAME="ticdc-failover-test-two-node-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	do_retry 5 3 cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	sleep 1

	kill_cdc_pid $cdc_pid_1
	cdc_pid_2=$(get_cdc_pid "$CDC_HOST" "8301")
	## restart server 1
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-1" --addr "127.0.0.1:8300"
	kill_cdc_pid $cdc_pid_2
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1-1" --addr "127.0.0.1:8301"

	sleep 15

	check_table_exists fail_over_test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 120

	export GO_FAILPOINTS=''

	cleanup_process $CDC_BINARY
	stop_tidb_cluster
}

trap 'stop_tidb_cluster; collect_logs $WORK_DIR' EXIT
failOverOnlyOneNode $*
failOverWhenTwoNode $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
