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
	local versions

	versions=$(curl -fsS "http://127.0.0.1:8088/subjects/${subject}/versions" | tr -d '[]' | tr ',' '\n')
	for version in $versions; do
		if curl -fsS "http://127.0.0.1:8088/subjects/${subject}/versions/${version}" | grep -q "$expected"; then
			return 0
		fi
	done

	echo "subject ${subject} does not contain ${expected}"
	return 1
}

function run_handling_modes() {
	local mode database topic sink_uri config_file diff_config
	for mode in default precise string; do
		database="debezium_avro_$mode"
		topic="ticdc-debezium-avro-handling-$mode-$RANDOM"
		config_file="$WORK_DIR/handling-$mode.toml"
		diff_config="$WORK_DIR/diff-$mode.toml"
		sink_uri="kafka://127.0.0.1:9092/$topic?protocol=debezium-avro&enable-tidb-extension=true&avro-enable-watermark=true&partition-num=1"
		cat >"$config_file" <<EOF
[filter]
rules = ['$database.*']
EOF
		if [ "$mode" != "default" ]; then
			cat >>"$config_file" <<EOF
[sink.kafka-config.codec-config]
avro-decimal-handling-mode = "precise"
avro-bigint-unsigned-handling-mode = "string"
EOF
		fi
		if [ "$mode" = "string" ]; then
			# Verify URI overrides in both the producer and consumer.
			sed -i 's/avro-bigint-unsigned-handling-mode = "string"/avro-bigint-unsigned-handling-mode = "long"/' "$config_file"
			sink_uri="$sink_uri&avro-decimal-handling-mode=string&avro-bigint-unsigned-handling-mode=string"
		fi
		sed "s/debezium_avro_modes/$database/g" "$CUR/data/handling_modes.sql" >"$WORK_DIR/handling-$mode.sql"
		if [ "$mode" = "default" ]; then
			# Avro long accepts unsigned BIGINT only within the signed range.
			sed -i -e 's/18446744073709551615/9223372036854775807/g' \
				-e 's/9223372036854775808/9007199254740993/g' "$WORK_DIR/handling-$mode.sql"
		fi

		cdc_cli_changefeed create -c "debezium-avro-$mode" --sink-uri="$sink_uri" \
			--config="$config_file" --schema-registry="$schema_registry_uri"
		run_kafka_consumer "$WORK_DIR" "$sink_uri" "$config_file" "$schema_registry_uri" "-$mode"
		run_sql_file "$WORK_DIR/handling-$mode.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
		kafka_dump --topic "$topic" --schema-registry-uri "$schema_registry_uri" \
			--timeout 90s --until-table handling_modes --until-count 6 >"$WORK_DIR/handling-$mode.jsonl"
		python3 "$CUR/check_handling_modes.py" "$WORK_DIR/handling-$mode.jsonl" "$mode"
		check_table_exists "$database.handling_modes" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 200
		sed -e "s/test[.][?][*]/$database.*/" \
			-e "s|debezium_avro/output|debezium_avro/output-$mode|" \
			"$CUR/conf/diff_config.toml" >"$diff_config"
		check_sync_diff "$WORK_DIR" "$diff_config"
		cdc_cli_changefeed remove -c "debezium-avro-$mode"
		cleanup_process cdc_kafka_consumer
	done
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

	TOPIC_NAME="ticdc-debezium-avro-$RANDOM"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=debezium-avro&enable-tidb-extension=true&avro-enable-watermark=true&partition-num=1&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760&avro-decimal-handling-mode=precise&avro-bigint-unsigned-handling-mode=string"
	schema_registry_uri="http://127.0.0.1:8088"
	changefeed_id="debezium-avro-$RANDOM"

	cdc_cli_changefeed create --start-ts="$start_ts" --sink-uri="$SINK_URI" -c "$changefeed_id" --schema-registry="$schema_registry_uri"
	sleep 5 # wait for changefeed to start

	run_kafka_consumer "$WORK_DIR" "$SINK_URI" "" "$schema_registry_uri"

	run_sql_file "$CUR/data/workload.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql_file "$CUR/data/ddl.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql_file "$CUR/data/post_ddl_workload.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	check_sync_diff "$WORK_DIR" "$CUR/conf/diff_config.toml" 120
	check_schema_registry_subject "$TOPIC_NAME-key" "tp_accountKey"
	check_schema_registry_subject "$TOPIC_NAME-value" "tp_accountEnvelope"

	cdc_cli_changefeed remove -c "$changefeed_id"
	cleanup_process cdc_kafka_consumer
	run_handling_modes

	cleanup_process "$CDC_BINARY"
}

trap 'stop_test $WORK_DIR' EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
