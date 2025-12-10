#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

MAX_RETRIES=20
pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
protocols=("open-protocol" "canal-json" "simple")

declare -r DB_NAME="kafka_log_info"

function build_sink_uri() {
	local protocol=$1
	local topic=$2
	echo "kafka://127.0.0.1:9092/$topic?protocol=$protocol&partition-num=1&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760"
}

function cleanup_changefeed() {
	local id=$1
	cdc_cli_changefeed remove --pd="${pd_addr}" --changefeed-id="$id" >/dev/null 2>&1 || true
}

function stop_cdc() {
	cleanup_process $CDC_BINARY
	export GO_FAILPOINTS=""
}

function start_cdc_with_failpoint() {
	local failpoints=$1
	export GO_FAILPOINTS="$failpoints"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
}

function test_dml_log_info() {
	local protocol=$1
	local topic="kafka-log-info-dml-${protocol}-${RANDOM}"
	local changefeed_id="kafka-log-info-${protocol}-dml"
	local sink_uri=$(build_sink_uri $protocol $topic)

	run_sql "DROP TABLE IF EXISTS ${DB_NAME}.dml_table" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE ${DB_NAME}.dml_table(id INT PRIMARY KEY AUTO_INCREMENT, val INT);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	start_cdc_with_failpoint 'github.com/pingcap/ticdc/pkg/sink/kafka/KafkaSinkAsyncSendError=1*return(true)'
	cdc_cli_changefeed create --pd=$pd_addr --sink-uri="$sink_uri" --changefeed-id="$changefeed_id"

	run_sql "INSERT INTO ${DB_NAME}.dml_table(val) VALUES (1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	local pattern='eventType=dml.*\\"Table\\":\\"dml_table\\".*\\"StartTs\\":.*\\"CommitTs\\":'
	ensure $MAX_RETRIES "check_logs_contains $WORK_DIR '$pattern' ''"

	cleanup_changefeed $changefeed_id
	stop_cdc
}

function test_ddl_log_info() {
	local protocol=$1
	local topic="kafka-log-info-ddl-${protocol}-${RANDOM}"
	local changefeed_id="kafka-log-info-${protocol}-ddl"
	local sink_uri=$(build_sink_uri $protocol $topic)

	run_sql "DROP TABLE IF EXISTS ${DB_NAME}.ddl_table;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	start_cdc_with_failpoint 'github.com/pingcap/ticdc/pkg/sink/kafka/KafkaSinkSyncSendMessageError=1*return(true);github.com/pingcap/ticdc/pkg/sink/kafka/KafkaSinkSyncSendMessagesError=1*return(true)'
	cdc_cli_changefeed create --pd=$pd_addr --sink-uri="$sink_uri" --changefeed-id="$changefeed_id"

	run_sql "CREATE TABLE ${DB_NAME}.ddl_table(id INT PRIMARY KEY);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	local ddl_pattern="eventType=ddl.*ddlQuery=.*CREATE TABLE*"
	ensure $MAX_RETRIES "check_logs_contains $WORK_DIR '$ddl_pattern' ''"

	cleanup_changefeed $changefeed_id
	stop_cdc
}

function test_checkpoint_log_info() {
	local protocol=$1
	local topic="kafka-log-info-checkpoint-${protocol}-${RANDOM}"
	local changefeed_id="kafka-log-info-${protocol}-checkpoint"
	local sink_uri=$(build_sink_uri $protocol $topic)

	start_cdc_with_failpoint 'github.com/pingcap/ticdc/pkg/sink/kafka/KafkaSinkSyncSendMessagesError=1*return(true)'
	cdc_cli_changefeed create --pd=$pd_addr --sink-uri="$sink_uri" --changefeed-id="$changefeed_id"

	ensure $MAX_RETRIES "check_logs_contains $WORK_DIR 'eventType=checkpoint.*checkpointTs=' ''"

	cleanup_changefeed $changefeed_id
	stop_cdc
}

function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		echo "skip kafka_log_info for sink type $SINK_TYPE"
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	run_sql "DROP DATABASE IF EXISTS ${DB_NAME};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE ${DB_NAME};" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	for protocol in "${protocols[@]}"; do
		test_dml_log_info $protocol
		test_ddl_log_info $protocol
		test_checkpoint_log_info $protocol
	done
}

trap "stop_cdc; stop_tidb_cluster" EXIT

run $*
check_logs $WORK_DIR

echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
