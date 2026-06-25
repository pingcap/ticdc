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

function check_schema_registry_subject() {
	local subject=$1
	local expected=$2

	curl -fsS "http://127.0.0.1:8088/subjects/${subject}/versions/latest" | grep -q "$expected"
}

function build_sink_uri() {
	local topic=$1
	echo "kafka://127.0.0.1:9092/$topic?protocol=debezium-avro&enable-tidb-extension=true&avro-enable-watermark=true&partition-num=1&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760&avro-decimal-handling-mode=precise&avro-bigint-unsigned-handling-mode=string"
}

function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"

	start_schema_registry
	start_tidb_cluster --workdir "$WORK_DIR"

	run_sql_file "$CUR/data/prepare.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql_file "$CUR/data/prepare.sql" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"

	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"

	TOPIC_PREFIX="ticdc-debezium-avro-$RANDOM"
	DEFAULT_TOPIC_NAME="$TOPIC_PREFIX-default"
	SINK_URI=$(build_sink_uri "$DEFAULT_TOPIC_NAME")
	schema_registry_uri="http://127.0.0.1:8088"
	changefeed_id="debezium-avro-$RANDOM"
	changefeed_config="$WORK_DIR/changefeed.toml"

	cat >"$changefeed_config" <<EOF
[sink]
dispatchers = [
    { matcher = ['test.*'], topic = "${TOPIC_PREFIX}-{schema}-{table}" },
]
EOF

	cdc_cli_changefeed create --start-ts="$start_ts" --sink-uri="$SINK_URI" -c "$changefeed_id" --config "$changefeed_config" --schema-registry="$schema_registry_uri"
	sleep 5 # wait for changefeed to start

	dml_tables=(
		"tp_account"
		"tp_int"
		"tp_unsigned_int"
		"tp_real"
		"tp_unsigned_real"
		"tp_time"
		"tp_text"
		"tp_blob"
		"tp_char_binary"
		"tp_other"
		"cs_gbk"
		"t1"
		"vec"
	)

	consumer_topics=""
	for table in "${dml_tables[@]}"; do
		topic_name="${TOPIC_PREFIX}-test-${table}"
		if [ -z "$consumer_topics" ]; then
			consumer_topics="$topic_name"
		else
			consumer_topics="$consumer_topics,$topic_name"
		fi
	done
	run_kafka_consumer "$WORK_DIR" "$(build_sink_uri "$consumer_topics")" "$changefeed_config" "$schema_registry_uri"

	run_sql_file "$CUR/data/workload.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql_file "$CUR/data/ddl.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql_file "$CUR/data/ddl.sql" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql_file "$CUR/data/post_ddl_workload.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	check_sync_diff "$WORK_DIR" "$CUR/conf/diff_config.toml" 120
	for table in "${dml_tables[@]}"; do
		topic_name="${TOPIC_PREFIX}-test-${table}"
		check_schema_registry_subject "$topic_name-key" "${table}Key"
		check_schema_registry_subject "$topic_name-value" "${table}Envelope"
	done

	echo "Starting build checksum checker..."
	cd "$CUR/../../utils/checksum_checker"
	if [ ! -f ./checksum_checker ]; then
		GO111MODULE=on go build
	fi
	./checksum_checker --upstream-uri "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/" --downstream-uri "root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/" --databases "test" --config="$changefeed_config"

	cleanup_process "$CDC_BINARY"
}

trap 'stop_test $WORK_DIR' EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
