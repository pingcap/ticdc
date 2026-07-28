#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

<<<<<<< HEAD
=======
STATE_WAIT_TIMEOUT_SECONDS=30
STATE_CHECK_INTERVAL_SECONDS=1
TABLE_CHECK_RETRIES=15
BATCH_LIMIT=262144
SMALL_TOPIC_LIMIT=524288
LARGE_TOPIC_LIMIT=2097152
ROW_BYTES=1048576
SCHEMA_REGISTRY_URI=http://127.0.0.1:8088
GENERATOR_DIR=$CUR/../../utils/gen_kafka_big_messages
consumer_pid=""

function start_schema_registry() {
	if curl -o /dev/null -s "$SCHEMA_REGISTRY_URI"; then
		return
	fi

	echo "Starting schema registry..."
	./bin/bin/schema-registry-start -daemon ./bin/etc/schema-registry/schema-registry.properties
	local i=0
	while ! curl -o /dev/null -s "$SCHEMA_REGISTRY_URI"; do
		i=$((i + 1))
		if [ "$i" -gt 30 ]; then
			echo "Failed to start schema registry"
			exit 1
		fi
		sleep 2
	done
	curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"compatibility": "NONE"}' "$SCHEMA_REGISTRY_URI/config"
}

function build_message_generator() {
	if [ ! -f "$GENERATOR_DIR/gen_kafka_big_messages" ]; then
		(cd "$GENERATOR_DIR" && GO111MODULE=on go build)
	fi
}

function kafka_sink_uri() {
	local topic_name=$1
	local protocol=$2
	local extra_params=$3
	local sink_uri="kafka://127.0.0.1:9092/${topic_name}?protocol=${protocol}&partition-num=1&kafka-version=${KAFKA_VERSION}&max-message-bytes=${BATCH_LIMIT}"
	if [ "$extra_params" != "" ]; then
		sink_uri="${sink_uri}&${extra_params}"
	fi
	echo "$sink_uri"
}

function start_kafka_consumer() {
	local work_dir=$1
	local sink_uri=$2
	local schema_registry_uri=$3
	local protocol_case=$4
	local downstream_uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/?safe-mode=true&batch-dml-enable=false&enable-ddl-ts=false"
	local args=(
		--log-file "$work_dir/cdc_kafka_consumer.log"
		--log-level debug
		--upstream-uri "$sink_uri"
		--downstream-uri "$downstream_uri"
	)
	if [ "$schema_registry_uri" != "" ]; then
		args+=(--schema-registry-uri "$schema_registry_uri")
	fi
	if [[ "$protocol_case" == simple_* ]]; then
		args+=(--upstream-tidb-dsn "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/?")
	fi

	cdc_kafka_consumer "${args[@]}" >>"$work_dir/cdc_kafka_consumer_stdout.log" 2>&1 &
	consumer_pid=$!
}

function stop_kafka_consumer() {
	if [ "$consumer_pid" != "" ]; then
		kill -9 "$consumer_pid" 2>/dev/null || true
		wait "$consumer_pid" 2>/dev/null || true
		consumer_pid=""
	fi
}

function wait_changefeed_state() {
	local pd_addr=$1
	local changefeed_id=$2
	local expected_state=$3
	local expected_error=$4
	local deadline=$((SECONDS + STATE_WAIT_TIMEOUT_SECONDS))

	while true; do
		if check_changefeed_state "$pd_addr" "$changefeed_id" "$expected_state" "$expected_error" ""; then
			return
		fi
		if [ "$SECONDS" -ge "$deadline" ]; then
			echo "changefeed $changefeed_id did not reach state $expected_state within ${STATE_WAIT_TIMEOUT_SECONDS}s"
			return 1
		fi
		sleep "$STATE_CHECK_INTERVAL_SECONDS"
	done
}

function render_diff_config() {
	local work_dir=$1
	local database_name=$2
	local diff_config=$3

	sed -e "s/database_name/${database_name}/g" \
		-e "s|/tmp/tidb_cdc_test/kafka_big_messages/sync_diff/output|${work_dir}/sync_diff/output|g" \
		"$CUR/conf/diff_config.toml" >"$diff_config"
}

function run_protocol_case() {
	local protocol_case=$1
	local protocol=$2
	local schema_registry_uri=$3
	local extra_params=$4
	local topic_case=${protocol_case//_/-}
	local topic_name="big-message-${topic_case}-${RANDOM}"
	local changefeed_id="kafka-big-messages-${topic_case}"
	local database_name="kafka_big_messages_${protocol_case}"
	local work_dir="$WORK_DIR/$protocol_case"
	local sql_file="$work_dir/test.sql"
	local diff_config="$work_dir/diff_config.toml"
	local pd_addr="http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
	local sink_uri
	local initial_topic_limit=$SMALL_TOPIC_LIMIT
	local expected_error=ErrMessageTooLarge
	if [ "$protocol_case" = "async_error" ]; then
		initial_topic_limit=$LARGE_TOPIC_LIMIT
		expected_error=ErrKafkaSendMessage
	fi

	mkdir -p "$work_dir"
	render_diff_config "$work_dir" "$database_name" "$diff_config"
	kafka_topic --topic "$topic_name" --max-message-bytes "$initial_topic_limit"
	local start_ts
	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	sink_uri=$(kafka_sink_uri "$topic_name" "$protocol" "$extra_params")

	if [ "$schema_registry_uri" != "" ]; then
		cdc_cli_changefeed create --start-ts="$start_ts" --sink-uri="$sink_uri" --schema-registry="$schema_registry_uri" -c "$changefeed_id"
	else
		cdc_cli_changefeed create --start-ts="$start_ts" --sink-uri="$sink_uri" -c "$changefeed_id"
	fi
	start_kafka_consumer "$work_dir" "$sink_uri" "$schema_registry_uri" "$protocol_case"
	wait_changefeed_state "$pd_addr" "$changefeed_id" "normal" "null"

	# Lower the topic limit after the producer has started. The encoder and
	# producer still accept the message, then Kafka rejects it asynchronously.
	if [ "$protocol_case" = "async_error" ]; then
		local ready_database="${database_name}_ready"
		run_sql "CREATE DATABASE ${ready_database}; CREATE TABLE ${ready_database}.ready(id INT PRIMARY KEY); INSERT INTO ${ready_database}.ready VALUES (1)" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
		ensure "$TABLE_CHECK_RETRIES" "run_sql 'SELECT id FROM ${ready_database}.ready' '$DOWN_TIDB_HOST' '$DOWN_TIDB_PORT' && check_contains 'id: 1'"
		kafka_topic --topic "$topic_name" --max-message-bytes "$SMALL_TOPIC_LIMIT" --alter
	fi

	"$GENERATOR_DIR/gen_kafka_big_messages" --row-bytes="$ROW_BYTES" --row-count=1 --database-name="$database_name" --table-name=test --sql-file-path="$sql_file"
	run_sql_file "$sql_file" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "CREATE TABLE ${database_name}.finish_mark(id INT PRIMARY KEY)" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	wait_changefeed_state "$pd_addr" "$changefeed_id" "warning" "$expected_error"

	# Only increase Kafka's topic limit. TiCDC must recreate the sink, read the
	# new limit, and resume without updating, pausing, or resuming the changefeed.
	kafka_topic --topic "$topic_name" --max-message-bytes "$LARGE_TOPIC_LIMIT" --alter
	wait_changefeed_state "$pd_addr" "$changefeed_id" "normal" "null"
	check_table_exists "${database_name}.finish_mark" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" "$TABLE_CHECK_RETRIES"
	check_sync_diff "$work_dir" "$diff_config"

	cdc_cli_changefeed remove -c "$changefeed_id"
	stop_kafka_consumer
}

>>>>>>> fa340f118 (kafka: unify sink errors and replace failpoint tests (#5786))
function run() {
	# test kafka sink only in this case
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

<<<<<<< HEAD
	start_tidb_cluster --workdir $WORK_DIR
=======
	local cases=(
		"canal_json|canal-json||enable-tidb-extension=true"
		"open_protocol|open-protocol||"
		"async_error|open-protocol||max-retry=0"
		"simple_json|simple||"
		"simple_avro|simple||encoding-format=avro"
		"avro|avro|$SCHEMA_REGISTRY_URI|enable-tidb-extension=true&avro-enable-watermark=true&avro-decimal-handling-mode=string&avro-bigint-unsigned-handling-mode=string"
	)
>>>>>>> fa340f118 (kafka: unify sink errors and replace failpoint tests (#5786))

	TOPIC_NAME="big-message-test-$RANDOM"

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query $UP_PD_HOST_1 $UP_PD_PORT_1)

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# Use a max-message-bytes parameter that is larger than the kafka topic max message bytes.
	# Test if TiCDC automatically uses the max-message-bytes of the topic.
	# See: https://github.com/PingCAP-QE/ci/blob/ddde195ebf4364a0028d53405d1194aa37a4d853/jenkins/pipelines/ci/ticdc/cdc_ghpr_kafka_integration_test.groovy#L178
	# Use a topic that has already been created.
	# See: https://github.com/PingCAP-QE/ci/blob/ddde195ebf4364a0028d53405d1194aa37a4d853/jenkins/pipelines/ci/ticdc/cdc_ghpr_kafka_integration_test.groovy#L180
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=1&kafka-version=${KAFKA_VERSION}&max-message-bytes=12582912"
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=1&version=${KAFKA_VERSION}"

	echo "Starting generate kafka big messages..."
	cd $CUR/../../utils/gen_kafka_big_messages
	if [ ! -f ./gen_kafka_big_messages ]; then
		GO111MODULE=on go build
	fi
	# Generate data larger than kafka broker max.message.bytes. We can send this data correctly.
	./gen_kafka_big_messages --row-count=15 --sql-file-path=$CUR/test.sql

	run_sql_file $CUR/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	table="kafka_big_messages.test"
	check_table_exists $table ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
