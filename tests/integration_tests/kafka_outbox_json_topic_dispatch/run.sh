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

	# Use a fixed default topic; per-tenant topics are auto-created at runtime
	# when the first row with that tenant_id is processed.
	DEFAULT_TOPIC="outbox-dispatch-default-$RANDOM"
	changefeed_id="outbox-dispatch"
	SINK_URI="kafka://127.0.0.1:9092/$DEFAULT_TOPIC?protocol=outbox-json"

	cdc_cli_changefeed create \
		--sink-uri="$SINK_URI" \
		--config="$CUR/conf/changefeed.toml" \
		-c $changefeed_id

	ensure $MAX_RETRIES check_changefeed_state \
		http://${UP_PD_HOST_1}:${UP_PD_PORT_1} $changefeed_id "normal" "null" ""

	run_sql_file $CUR/data/ddl.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Generate a per-run prefix so that tenant topic names are unique and
	# cannot collide with topics left over from a previous test run.
	TENANT_PREFIX="t${RANDOM}-"
	data_tmp=$(mktemp)
	sed "s/__PREFIX__/${TENANT_PREFIX}/g" "$CUR/data/data.sql" >"$data_tmp"
	run_sql_file "$data_tmp" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	rm -f "$data_tmp"

	# Wait for the changefeed to process all rows by polling checkpoint_ts rather
	# than sleeping. Each distinct tenant_id causes a new Kafka topic to be
	# auto-created; the changefeed must remain healthy throughout.
	dml_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	ensure $MAX_RETRIES check_changefeed_checkpoint $changefeed_id $dml_ts
	ensure $MAX_RETRIES check_changefeed_state \
		http://${UP_PD_HOST_1}:${UP_PD_PORT_1} $changefeed_id "normal" "null" ""

	# Verify that per-tenant topics were created by the runtime topic discovery.
	# The Confluent kafka-topics binary is required; it is downloaded by
	# `make prepare_test_binaries` and must be present to run this test.
	KAFKA_TOPICS_BIN=$CUR/../../../bin/bin/kafka-topics
	if [ ! -x "$KAFKA_TOPICS_BIN" ]; then
		echo "FAIL: kafka-topics binary not found at $KAFKA_TOPICS_BIN; run make prepare_test_binaries first"
		exit 1
	fi
	topic_list=$($KAFKA_TOPICS_BIN --list --bootstrap-server 127.0.0.1:9092 2>/dev/null)
	for tenant in "tenant-a" "tenant-b" "tenant-c"; do
		expected="${TENANT_PREFIX}${tenant}"
		echo "$topic_list" | grep -q "^${expected}$" || {
			echo "FAIL: expected Kafka topic '${expected}' not found; topic list: $topic_list"
			exit 1
		}
	done
	echo "all per-tenant topics confirmed in Kafka"

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
