#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR="$OUT_DIR/$TEST_NAME"
CDC_BINARY=cdc.test
SINK_TYPE=${1:-}
BROKER_ADDR=${ICEBERG_DUAL_WRITE_BROKER:-127.0.0.1:9092}
BROKER_HOST=${BROKER_ADDR%:*}
BROKER_PORT=${BROKER_ADDR##*:}

if [ -z "$SINK_TYPE" ] || [ "$SINK_TYPE" != "iceberg" ]; then
	echo "skip iceberg integration test, sink type is $SINK_TYPE"
	exit 0
fi

if [ -z "${ICEBERG_SPARK_READBACK:-}" ]; then
	if command -v spark-sql >/dev/null 2>&1; then
		export ICEBERG_SPARK_READBACK=1
	else
		export ICEBERG_SPARK_READBACK=0
	fi
fi
if [ "${ICEBERG_SPARK_READBACK}" = "1" ]; then
	export ICEBERG_SPARK_PACKAGES="${ICEBERG_SPARK_PACKAGES:-org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1}"
fi

function wait_kafka_ready() {
	check_time=${1:-60}
	i=0
	while [ $i -lt $check_time ]; do
		if (echo >"/dev/tcp/$BROKER_HOST/$BROKER_PORT") >/dev/null 2>&1; then
			return 0
		fi
		i=$((i + 1))
		sleep 1
	done
	echo "Kafka broker $BROKER_ADDR is not ready after ${check_time}s"
	return 1
}

function wait_log_contains() {
	log_file=$1
	pattern=$2
	check_time=${3:-60}
	i=0
	while [ $i -lt $check_time ]; do
		if [ -f "$log_file" ] && grep -q "$pattern" "$log_file"; then
			return 0
		fi
		i=$((i + 1))
		sleep 1
	done
	echo "pattern not found after ${check_time}s: ${pattern}"
	if [ -f "$log_file" ]; then
		cat "$log_file"
	fi
	return 1
}

function consume_kafka_messages() {
	topic_name=$1
	max_messages=${2:-20}

	if command -v kafka-console-consumer >/dev/null 2>&1; then
		kafka-console-consumer \
			--bootstrap-server "$BROKER_ADDR" \
			--topic "$topic_name" \
			--from-beginning \
			--max-messages "$max_messages" \
			--timeout-ms 10000 2>/dev/null || true
		return 0
	fi

	if command -v kcat >/dev/null 2>&1; then
		kcat -b "$BROKER_ADDR" -t "$topic_name" -C -o beginning -c "$max_messages" -q -e 2>/dev/null || true
		return 0
	fi

	echo "no kafka consumer tool found (expected kafka-console-consumer or kcat)"
	return 1
}

function prepare() {
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	if ! wait_kafka_ready 90; then
		echo "dual-write test requires a reachable Kafka broker at $BROKER_ADDR"
		exit 1
	fi

	start_tidb_cluster --workdir "$WORK_DIR"

	do_retry 5 2 run_sql "CREATE TABLE test.dual_write_test(id INT PRIMARY KEY, val INT);"
	start_ts=$(run_cdc_cli_tso_query "${UP_PD_HOST_1}" "${UP_PD_PORT_1}")

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"

	WAREHOUSE_DIR="$WORK_DIR/iceberg_warehouse"
}

function wait_file_exists() {
	file_pattern=$1
	check_time=${2:-60}
	i=0
	while [ $i -lt $check_time ]; do
		if ls $file_pattern >/dev/null 2>&1; then
			return 0
		fi
		i=$((i + 1))
		sleep 1
	done
	echo "file not found after ${check_time}s: ${file_pattern}"
	return 1
}

function iceberg_check_dual_write() {
	WAREHOUSE_DIR="$WORK_DIR/iceberg_warehouse"
	TABLE_ROOT="$WAREHOUSE_DIR/ns/test/dual_write_test"
	METADATA_DIR="$TABLE_ROOT/metadata"
	DATA_DIR="$TABLE_ROOT/data"

	SINK_URI="iceberg://?warehouse=file://$WAREHOUSE_DIR&catalog=hadoop&namespace=ns&mode=append&commit-interval=1s&enable-checkpoint-table=true&enable-global-checkpoint-table=true&partitioning=days(_tidb_commit_time)&broker=$BROKER_ADDR"

	changefeed_id=$(
		cdc_cli_changefeed create --start-ts="$start_ts" --sink-uri="$SINK_URI" |
			awk '/^ID:/ { print $2; exit }'
	)
	if [ -z "$changefeed_id" ]; then
		echo "failed to create changefeed for dual-write case"
		exit 1
	fi
	wait_changefeed_table_assigned "$changefeed_id" "test" "dual_write_test"

	echo "Inserting test data..."
	do_retry 5 2 run_sql "INSERT INTO test.dual_write_test(id, val) VALUES (1, 100);"
	do_retry 5 2 run_sql "INSERT INTO test.dual_write_test(id, val) VALUES (2, 200);"
	do_retry 5 2 run_sql "UPDATE test.dual_write_test SET val = 250 WHERE id = 2;"
	do_retry 5 2 run_sql "DELETE FROM test.dual_write_test WHERE id = 1;"

	echo "Waiting for Iceberg data files..."
	wait_file_exists "$METADATA_DIR/v*.metadata.json" 180
	wait_file_exists "$DATA_DIR/snap-*.parquet" 180

	echo "Verifying Iceberg commit watermark..."
	latest_meta=$(ls -1 "$METADATA_DIR"/v*.metadata.json | sort -V | tail -n 1)
	committed_ts=$(jq -r '.snapshots[-1].summary["tidb.committed_resolved_ts"] // ""' "$latest_meta")
	if [ -z "$committed_ts" ] || [ "$committed_ts" == "null" ]; then
		echo "missing tidb.committed_resolved_ts in iceberg metadata"
		cat "$latest_meta"
		exit 1
	fi
	echo "Iceberg committed_ts: $committed_ts"

	echo "Verifying dual-write runtime logs..."
	cdc_log="$WORK_DIR/cdc.log"
	wait_log_contains "$cdc_log" "iceberg dual-write: wrote to kafka" 120
	wait_log_contains "$cdc_log" "ticdc-iceberg_test_dual_write_test" 120
	if grep -q "iceberg dual-write: failed to write to kafka" "$cdc_log"; then
		echo "detected dual-write failures in cdc log"
		grep "iceberg dual-write: failed to write to kafka" "$cdc_log"
		exit 1
	fi

	echo "Verifying dual-write Kafka records..."
	kafka_topic="ticdc-iceberg_test_dual_write_test"
	kafka_messages=""
	i=0
	while [ $i -lt 3 ]; do
		kafka_messages=$(consume_kafka_messages "$kafka_topic" 20 || true)
		if [ -n "$kafka_messages" ]; then
			break
		fi
		i=$((i + 1))
		sleep 2
	done
	if [ -z "$kafka_messages" ]; then
		echo "no Kafka records found for topic $kafka_topic"
		exit 1
	fi
	if ! printf '%s\n' "$kafka_messages" | grep -q "iceberg-change:"; then
		echo "Kafka records missing expected iceberg dual-write marker"
		echo "$kafka_messages"
		exit 1
	fi

	echo "Verifying checkpoint tables..."
	CHECKPOINT_DIR="$WAREHOUSE_DIR/ns/__ticdc/__tidb_checkpoints/data"
	GLOBAL_CHECKPOINT_DIR="$WAREHOUSE_DIR/ns/__ticdc/__tidb_global_checkpoints/data"
	CHECKPOINT_METADATA_DIR="$WAREHOUSE_DIR/ns/__ticdc/__tidb_checkpoints/metadata"
	GLOBAL_CHECKPOINT_METADATA_DIR="$WAREHOUSE_DIR/ns/__ticdc/__tidb_global_checkpoints/metadata"
	wait_file_exists "$CHECKPOINT_DIR/snap-*.parquet" 180
	wait_file_exists "$GLOBAL_CHECKPOINT_DIR/snap-*.parquet" 180
	wait_file_exists "$CHECKPOINT_METADATA_DIR/v*.metadata.json" 180
	wait_file_exists "$GLOBAL_CHECKPOINT_METADATA_DIR/v*.metadata.json" 180

	echo "Verifying dual-write checkpoint in checkpoint table..."
	latest_checkpoint_meta=$(ls -1 "$CHECKPOINT_METADATA_DIR"/v*.metadata.json | sort -V | tail -n 1)
	checkpoint_count=$(jq '.snapshots | length' "$latest_checkpoint_meta" 2>/dev/null || echo "0")
	echo "Checkpoint snapshots: $checkpoint_count"

	if [ "${ICEBERG_SPARK_READBACK:-0}" = "1" ]; then
		warehouse_uri="file://$WAREHOUSE_DIR"
		table_name="iceberg_test.ns.test.dual_write_test"
		readback=$(iceberg_spark_sql_scalar \
			--warehouse "$warehouse_uri" \
			--sql "SELECT concat_ws(',', CAST(count(*) AS STRING), CAST(sum(CASE WHEN _tidb_op = 'I' THEN 1 ELSE 0 END) AS STRING), CAST(sum(CASE WHEN _tidb_op = 'U' THEN 1 ELSE 0 END) AS STRING), CAST(sum(CASE WHEN _tidb_op = 'D' THEN 1 ELSE 0 END) AS STRING)) AS v FROM $table_name")
		expected="4,2,1,1"
		if [ "$readback" != "$expected" ]; then
			echo "spark readback mismatch, expected $expected, got: $readback"
			exit 1
		fi
		echo "Spark readback passed: $readback"
	fi

	cleanup_process "$CDC_BINARY"
}

function wait_changefeed_table_assigned() {
	changefeed_id=$1
	db_name=$2
	table_name=$3

	if [ "${TICDC_NEWARCH:-}" != "true" ]; then
		return 0
	fi

	table_id=$(get_table_id "$db_name" "$table_name")
	echo "wait table ${db_name}.${table_name} (id=${table_id}) assigned to changefeed ${changefeed_id}"

	auth_user=${TICDC_USER:-ticdc}
	auth_pass=${TICDC_PASSWORD:-ticdc_secret}
	url="http://127.0.0.1:8300/api/v2/changefeeds/${changefeed_id}/tables?keyspace=${KEYSPACE_NAME}"
	last_body=""
	last_code=""

	i=0
	while [ $i -lt 60 ]; do
		resp=$(curl -sS -w '\n%{http_code}' --user "${auth_user}:${auth_pass}" "$url" 2>&1 || true)
		body=${resp%$'\n'*}
		code=${resp##*$'\n'}
		last_body=$body
		last_code=$code

		if [ "$code" != "200" ]; then
			echo "wait table ${db_name}.${table_name}: http $code response: $body"
			sleep 2
			i=$((i + 1))
			continue
		fi

		if ! echo "$body" | jq -e . >/dev/null 2>&1; then
			echo "wait table ${db_name}.${table_name}: invalid json response: $body"
			sleep 2
			i=$((i + 1))
			continue
		fi

		if echo "$body" | jq -e --argjson tid "$table_id" \
			'def items: (if type=="array" then . else .items // [] end); items[]?.table_ids[]? | select(. == $tid)' >/dev/null; then
			echo "table ${db_name}.${table_name} assigned to changefeed ${changefeed_id}"
			return 0
		fi

		if [ $((i % 10)) -eq 0 ]; then
			items_count=$(echo "$body" | jq -r 'if type=="array" then length else (.items | length) end' 2>/dev/null || echo "unknown")
			echo "wait table ${db_name}.${table_name}: not assigned yet (items=${items_count})"
		fi

		sleep 2
		i=$((i + 1))
	done

	echo "table ${db_name}.${table_name} not assigned to changefeed ${changefeed_id} after $((i * 2))s (last_http=${last_code})"
	if [ -n "$last_body" ]; then
		echo "last response: $last_body"
	fi
	return 1
}

trap 'stop_test "$WORK_DIR"' EXIT
prepare "$@"
iceberg_check_dual_write "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
