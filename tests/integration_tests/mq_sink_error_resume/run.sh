#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_COUNT=3
DB_COUNT=4
MAX_RETRIES=20

function run() {
	# test MQ sink only in this case
	if [ "$SINK_TYPE" != "kafka" ] && [ "$SINK_TYPE" != "pulsar" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	cd $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	TOPIC_NAME="ticdc-mq-sink-error-resume-test-$RANDOM"

	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	esac
	# Return an failpoint error to fail a kafka changefeed.
	# Note we return one error for the failpoint, if owner retry changefeed frequently, it may break the test.
	export GO_FAILPOINTS='github.com/pingcap/ticdc/pkg/sink/kafka/KafkaSinkAsyncSendError=1*return(true);github.com/pingcap/ticdc/downstreamadapter/sink/pulsar/PulsarSinkAsyncSendError=1*return(true)'
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	changefeed_id=$(cdc_cli_changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" | grep '^ID:' | head -n1 | awk '{print $2}')
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	run_sql "CREATE DATABASE mq_sink_error_resume;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table mq_sink_error_resume.t1(id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table mq_sink_error_resume.t2(id int primary key auto_increment, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO mq_sink_error_resume.t1 VALUES ();" &

	ensure $MAX_RETRIES "check_changefeed_status '127.0.0.1:8300' '$changefeed_id' 'warning' 'last_warning' '$SINK_TYPE sink injected error'"
	ensure $MAX_RETRIES "check_changefeed_status '127.0.0.1:8300' '$changefeed_id' 'normal'"

	check_table_exists "mq_sink_error_resume.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "mq_sink_error_resume.t2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	run_sql "INSERT INTO mq_sink_error_resume.t1 VALUES (),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO mq_sink_error_resume.t2 VALUES (),();" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "UPDATE mq_sink_error_resume.t2 SET val = 100;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
