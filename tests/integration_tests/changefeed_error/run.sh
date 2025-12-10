#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=20

function check_no_changefeed() {
	pd=$1
	count=$(cdc_cli_changefeed list --pd=$pd | grep -v "Command to ticdc" | jq '.|length')
	if [[ ! "$count" -eq "0" ]]; then
		exit 1
	fi
}

function check_no_capture() {
	pd=$1
	count=$(cdc cli capture list --pd=$pd 2>&1 | grep -v "Command to ticdc" | jq '.|length')
	if [[ ! "$count" -eq "0" ]]; then
		exit 1
	fi
}

export -f check_no_changefeed
export -f check_no_capture

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	run_sql "CREATE DATABASE changefeed_error;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=changefeed_error
	export GO_FAILPOINTS='github.com/pingcap/ticdc/logservice/schemastore/getAllPhysicalTablesGCFastFail=1*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	# capture_pid=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')

	TOPIC_NAME="ticdc-sink-retry-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac
	changefeedid="changefeed-error"
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $changefeedid
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	# CASE 1: Test unretryable error
	echo "Start case 1: Test unretryable error"
	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid} "failed" "[CDC:ErrSnapshotLostByGC]" ""
	cdc_cli_changefeed resume -c $changefeedid

	check_table_exists "changefeed_error.usertable" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=changefeed_error
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY

	# make sure old capture key and old owner key expire in etcd
	ensure $MAX_RETRIES "check_etcd_meta_not_exist '/tidb/cdc/default/__cdc_meta__/capture' 'capture'"
	ensure $MAX_RETRIES "check_etcd_meta_not_exist '/tidb/cdc/default/__cdc_meta__/owner' 'owner'"
	echo "Pass case 1"

	# CASE 2: Test retryable error
	echo "Start case 2: Test retryable error"
	export GO_FAILPOINTS='github.com/pingcap/ticdc/maintainer/NewChangefeedRetryError=return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid} "warning" "failpoint injected retriable error" ""

	# try to create another changefeed to make sure the coordinator is not stuck
	changefeedid_2="changefeed-error-2"
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $changefeedid_2
	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid_2} "warning" "failpoint injected retriable error" ""

	cdc_cli_changefeed remove -c $changefeedid
	ensure $MAX_RETRIES check_no_changefeed ${UP_PD_HOST_1}:${UP_PD_PORT_1}

	cdc_cli_changefeed remove -c $changefeedid_2
	ensure $MAX_RETRIES check_no_changefeed ${UP_PD_HOST_1}:${UP_PD_PORT_1}

	# test changefeed remove twice, and it should return "Changefeed not found"
	result=$(cdc_cli_changefeed remove -c $changefeedid_2)
	if [[ $result != *"Changefeed not found"* ]]; then
		echo "changefeeed remove result is expected to contains 'Changefeed not found', \
            but actually got $result"
		exit 1
	fi

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
	ensure $MAX_RETRIES "check_etcd_meta_not_exist '/tidb/cdc/default/__cdc_meta__/owner' 'owner'"
	echo "Pass case 2"

	# CASE 3: updating GC safepoint failure case
	echo "Start case 3: updating GC safepoint failure case"
	export GO_FAILPOINTS='github.com/pingcap/ticdc/pkg/txnutil/gc/InjectActualGCSafePoint=return(9223372036854775807)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	changefeedid_3="changefeed-error-3"
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $changefeedid_3
	ensure $MAX_RETRIES check_changefeed_state http://${UP_PD_HOST_1}:${UP_PD_PORT_1} ${changefeedid_3} "failed" "[CDC:ErrSnapshotLostByGC]" ""

	cdc_cli_changefeed remove -c $changefeedid_3
	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
	echo "Pass case 3"
}

trap 'stop_tidb_cluster; collect_logs $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
