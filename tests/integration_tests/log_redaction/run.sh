#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# Create test database
	run_sql "CREATE DATABASE IF NOT EXISTS log_redaction_test;"

	echo "=== Test 1: Redaction OFF mode ==="
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log off --logsuffix "_off"

	TOPIC_NAME="ticdc-log-redaction-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	cdc_cli_changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/test_errors.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT} 2>&1 | grep -v "ERROR 1062" || true

	check_table_exists log_redaction_test.users ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	# Verify OFF - sensitive data visible in plain text
	if grep -q "Password1!" $WORK_DIR/cdc_off.log; then
		echo "[$(date)] ✓ OFF mode: Sensitive data visible"
	else
		echo "[$(date)] ✗ OFF mode: Expected sensitive data in logs"
		exit 1
	fi

	cleanup_process $CDC_BINARY

	echo ""
	echo "=== Test 2: Redaction MARKER mode ==="
	run_sql "DROP DATABASE IF EXISTS log_redaction_test;"
	run_sql "CREATE DATABASE log_redaction_test;"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log marker --logsuffix "_marker"

	cdc_cli_changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/test_errors.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT} 2>&1 | grep -v "ERROR 1062" || true

	sleep 3

	# Verify MARKER - sensitive data wrapped with markers
	if grep -q "‹.*Password1!.*›" $WORK_DIR/cdc_marker.log; then
		echo "[$(date)] ✓ MARKER mode: Sensitive data wrapped in ‹›"
	else
		echo "[$(date)] ✗ MARKER mode: Expected ‹Password1!› in logs"
		exit 1
	fi

	cleanup_process $CDC_BINARY

	echo ""
	echo "=== Test 3: Redaction ON mode ==="
	run_sql "DROP DATABASE IF EXISTS log_redaction_test;"
	run_sql "CREATE DATABASE log_redaction_test;"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log on --logsuffix "_on"

	cdc_cli_changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/test_errors.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT} 2>&1 | grep -v "ERROR 1062" || true

	sleep 3

	# Verify ON - sensitive data not visible
	if ! grep -q "Password1!" $WORK_DIR/cdc_on.log; then
		echo "[$(date)] ✓ ON mode: Sensitive data redacted"
	else
		echo "[$(date)] ✗ ON mode: Found sensitive data in logs"
		exit 1
	fi

	cleanup_process $CDC_BINARY

	echo ""
	echo "=== Test 4: API mode switching ==="
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log off --logsuffix "_api" --addr "127.0.0.1:8300"

	# OFF -> MARKER transition
	http_code=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
		-H "Content-Type: application/json" \
		-d '{"mode": "marker"}' \
		http://127.0.0.1:8300/api/v2/log/redact)

	if [ "$http_code" = "200" ]; then
		echo "[$(date)] ✓ OFF -> MARKER transition succeeded"
	else
		echo "[$(date)] ✗ OFF -> MARKER transition failed"
		exit 1
	fi

	# MARKER -> ON transition
	http_code=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
		-H "Content-Type: application/json" \
		-d '{"mode": "on"}' \
		http://127.0.0.1:8300/api/v2/log/redact)

	if [ "$http_code" = "200" ]; then
		echo "[$(date)] ✓ MARKER -> ON transition succeeded"
	else
		echo "[$(date)] ✗ MARKER -> ON transition failed"
		exit 1
	fi

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
