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

	KAFKA_CONSUMER_BIN=$CUR/../../../bin/bin/kafka-console-consumer
	if [ ! -x "$KAFKA_CONSUMER_BIN" ]; then
		echo "FAIL: kafka-console-consumer binary not found at $KAFKA_CONSUMER_BIN; run make prepare_test_binaries first"
		exit 1
	fi

	TOPIC_NAME="outbox-basic-$RANDOM"
	changefeed_id="outbox-basic"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=outbox-json"

	cdc_cli_changefeed create \
		--sink-uri="$SINK_URI" \
		--config="$CUR/conf/changefeed.toml" \
		-c $changefeed_id

	ensure $MAX_RETRIES check_changefeed_state \
		http://${UP_PD_HOST_1}:${UP_PD_PORT_1} $changefeed_id "normal" "null" ""

	run_sql_file $CUR/data/ddl.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Capture a TSO after all DML has committed, then wait for the changefeed
	# checkpoint to advance past it. This guarantees every row (inserts as well
	# as the updates and deletes that must be silently skipped) has been fully
	# processed before we assert on the changefeed health.
	dml_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	ensure $MAX_RETRIES check_changefeed_checkpoint $changefeed_id $dml_ts
	ensure $MAX_RETRIES check_changefeed_state \
		http://${UP_PD_HOST_1}:${UP_PD_PORT_1} $changefeed_id "normal" "null" ""

	consumer_output=$($KAFKA_CONSUMER_BIN \
		--bootstrap-server 127.0.0.1:9092 \
		--topic "$TOPIC_NAME" \
		--from-beginning \
		--max-messages 8 \
		--timeout-ms 10000 \
		2>/dev/null || true)

	# The UPDATE events changed two rows to status="processed". Verify that value
	# does not appear in any Kafka message, proving UPDATE events were silently
	# skipped by the outbox-json encoder.
	if echo "$consumer_output" | grep -q '"status": "processed"'; then
		echo "FAIL: UPDATE event produced an outbox message (should be silently skipped)"
		echo "consumer output: $consumer_output"
		exit 1
	fi

	echo "$consumer_output" | grep -q '"order_id": 39879' || {
		echo "FAIL: expected payload for order 39879 not found"
		echo "consumer output: $consumer_output"
		exit 1
	}

	echo "$consumer_output" | grep -q '"status": "order.confirmed"' || {
		echo "FAIL: expected order.confirmed payload not found"
		echo "consumer output: $consumer_output"
		exit 1
	}

	echo "$consumer_output" | grep -q '"status": "order.shipped"' || {
		echo "FAIL: expected order.shipped payload not found"
		echo "consumer output: $consumer_output"
		exit 1
	}

	echo "all outbox message assertions passed"

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
