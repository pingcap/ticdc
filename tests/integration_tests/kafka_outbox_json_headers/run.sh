#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=20

function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# kafka-console-consumer is required to read back messages and assert
	# on header values. Fail immediately if it is absent rather than silently
	# skipping the content assertions.
	KAFKA_CONSUMER_BIN=$CUR/../../../bin/bin/kafka-console-consumer
	if [ ! -x "$KAFKA_CONSUMER_BIN" ]; then
		echo "FAIL: kafka-console-consumer binary not found at $KAFKA_CONSUMER_BIN; run make prepare_test_binaries first"
		exit 1
	fi

	TOPIC_NAME="outbox-headers-$RANDOM"
	changefeed_id="outbox-headers"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=outbox-json"

	cdc_cli_changefeed create \
		--sink-uri="$SINK_URI" \
		--config="$CUR/conf/changefeed.toml" \
		-c $changefeed_id

	ensure $MAX_RETRIES check_changefeed_state \
		http://${UP_PD_HOST_1}:${UP_PD_PORT_1} $changefeed_id "normal" "null" ""

	run_sql_file $CUR/data/ddl.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Wait for the changefeed to process all rows by polling checkpoint_ts.
	# By the time checkpoint advances past the DML commit timestamp, all
	# messages are guaranteed to be visible in Kafka.
	dml_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	ensure $MAX_RETRIES check_changefeed_checkpoint $changefeed_id $dml_ts
	ensure $MAX_RETRIES check_changefeed_state \
		http://${UP_PD_HOST_1}:${UP_PD_PORT_1} $changefeed_id "normal" "null" ""

	# Consume both messages and assert on header values.
	# --property print.headers=true emits headers as comma-separated key:value
	# pairs on each output line, before a tab and then the message value.
	# Expected headers include Id, traceparent, and tracestate.
	consumer_output=$($KAFKA_CONSUMER_BIN \
		--bootstrap-server 127.0.0.1:9092 \
		--topic "$TOPIC_NAME" \
		--from-beginning \
		--max-messages 2 \
		--timeout-ms 10000 \
		--property print.headers=true \
		2>/dev/null)

	echo "$consumer_output" | grep -q \
		"traceparent:00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" || {
		echo "FAIL: expected traceparent header for row 1 not found"
		echo "consumer output: $consumer_output"
		exit 1
	}

	echo "$consumer_output" | grep -q \
		"traceparent:00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" || {
		echo "FAIL: expected traceparent header for row 2 not found"
		echo "consumer output: $consumer_output"
		exit 1
	}

	echo "$consumer_output" | grep -q \
		"Id:550e8400-e29b-41d4-a716-446655440001" || {
		echo "FAIL: expected Id header for row 1 not found"
		echo "consumer output: $consumer_output"
		exit 1
	}

	echo "$consumer_output" | grep -q \
		"Id:550e8400-e29b-41d4-a716-446655440002" || {
		echo "FAIL: expected Id header for row 2 not found"
		echo "consumer output: $consumer_output"
		exit 1
	}

	echo "$consumer_output" | grep -q \
		"tracestate:rojo=00f067aa0ba902b7" || {
		echo "FAIL: expected tracestate header for row 1 not found"
		echo "consumer output: $consumer_output"
		exit 1
	}

	echo "$consumer_output" | grep -q \
		"tracestate:congo=BleGNlZWRzIHRo" || {
		echo "FAIL: expected tracestate header for row 2 not found"
		echo "consumer output: $consumer_output"
		exit 1
	}

	echo "all Kafka header assertions passed"

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
