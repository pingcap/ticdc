#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function start_schema_registry() {
	if ! curl -o /dev/null -s "http://127.0.0.1:8088"; then
		echo 'Starting schema registry...'
		./bin/bin/schema-registry-start -daemon ./bin/etc/schema-registry/schema-registry.properties
		local i=0
		while ! curl -o /dev/null -s "http://127.0.0.1:8088"; do
			i=$((i + 1))
			if [ "$i" -gt 30 ]; then
				echo 'Failed to start schema registry'
				exit 1
			fi
			sleep 2
		done
	fi

	curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"compatibility": "NONE"}' http://127.0.0.1:8088/config
}

function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"

	start_schema_registry
	start_tidb_cluster --workdir "$WORK_DIR"
	run_sql_file "$CUR/data/prepare.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"

	TOPIC_NAME="ticdc-debezium-avro-$RANDOM"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=debezium&enable-tidb-extension=true&partition-num=1&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760"
	schema_registry_uri="http://127.0.0.1:8088"
	changefeed_id="debezium-avro-$RANDOM"

	cdc_cli_changefeed create --start-ts="$start_ts" --sink-uri="$SINK_URI" -c "$changefeed_id" --schema-registry="$schema_registry_uri"
	run_sql_file "$CUR/data/workload.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	GO111MODULE=on go run ./tests/integration_tests/debezium_avro/verify \
		--topic "$TOPIC_NAME" \
		--kafka-addr "127.0.0.1:9092" \
		--schema-registry "$schema_registry_uri" \
		--timeout "120s" \
		2>&1 | tee "$WORK_DIR/debezium_avro_verify.log"

	cleanup_process "$CDC_BINARY"
}

trap 'stop_test $WORK_DIR' EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
