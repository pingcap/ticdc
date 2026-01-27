#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# Helper function to log raw content before validation
log_raw_content() {
	local description="$1"
	local content="$2"
	echo "  [RAW] $description:"
	echo "$content" | sed 's/^/    /'
	echo ""
}

# Helper function to wait for log content with timeout
wait_for_log_content() {
	local log_file="$1"
	local pattern="$2"
	local description="$3"
	local max_wait="${4:-30}"

	echo "  Waiting for $description (max ${max_wait}s)..."
	for i in $(seq 1 $max_wait); do
		if grep -q "$pattern" "$log_file" 2>/dev/null; then
			echo "  Found after ${i}s"
			return 0
		fi
		sleep 1
	done
	echo "  Not found after ${max_wait}s"
	return 1
}

# Helper function to require log pattern (STRICT validation - fails if not found)
# If $captured_logs is set, validates against that instead of re-grepping the file
require_log_pattern() {
	local log_file="$1"
	local pattern="$2"
	local description="$3"
	local context="${4:-}"

	# Use captured_logs if available, otherwise grep the file
	local search_content
	if [ -n "${captured_logs:-}" ]; then
		search_content="$captured_logs"
	else
		search_content=$(cat "$log_file" 2>/dev/null || echo "")
	fi

	if echo "$search_content" | grep -q "$pattern" 2>/dev/null; then
		echo "    ✓ VALIDATED: $description"
		return 0
	else
		echo "    ✗ FAILED: $description - pattern not found: $pattern"
		if [ -n "$context" ]; then
			echo "    Context: $context"
		fi
		echo "    Debug: searching log content..."
		echo "$search_content" | grep -E "(Row:|DMLEvent|WriteEvents)" 2>/dev/null | head -5 | sed 's/^/    /' || echo "    No matching logs found"
		exit 1
	fi
}

# Helper function to require log pattern is NOT present (negative validation)
# If $captured_logs is set, validates against that instead of re-grepping the file
require_no_log_pattern() {
	local log_file="$1"
	local pattern="$2"
	local description="$3"

	# Use captured_logs if available, otherwise grep the file
	local search_content
	if [ -n "${captured_logs:-}" ]; then
		search_content="$captured_logs"
	else
		search_content=$(cat "$log_file" 2>/dev/null || echo "")
	fi

	if echo "$search_content" | grep -q "$pattern" 2>/dev/null; then
		echo "    ✗ FAILED: $description - sensitive data leaked!"
		echo "$search_content" | grep "$pattern" 2>/dev/null | head -3 | sed 's/^/    /'
		exit 1
	else
		echo "    ✓ VALIDATED: $description"
		return 0
	fi
}

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# Create test database on both upstream and downstream
	run_sql "CREATE DATABASE IF NOT EXISTS log_redaction_test;"
	run_sql "CREATE DATABASE IF NOT EXISTS log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# ==========================================================================
	# Test 1: Redaction OFF mode with BlackHole sink
	# This test uses blackhole sink to FORCE DMLEvent logging code path
	# ==========================================================================
	echo "=== Test 1: Redaction OFF mode (BlackHole sink - forces DMLEvent logging) ==="
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log off --logsuffix "_off_blackhole"

	# Create blackhole changefeed to force DMLEvent.String() logging
	BLACKHOLE_SINK_URI="blackhole://"
	cdc_cli_changefeed create --sink-uri="$BLACKHOLE_SINK_URI" --changefeed-id="blackhole-off-test" --config=$CUR/conf/changefeed.toml

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Wait for blackhole sink to process events (it logs at Debug level)
	echo "  Waiting for BlackHole sink to process events..."
	wait_for_log_content "$WORK_DIR/cdc_off_blackhole.log" "BlackHoleSink: WriteEvents" "DMLEvent logs" 30

	echo "  [Validation] OFF mode with BlackHole sink:"
	echo ""

	# Capture BlackHole logs once for all validations
	captured_logs=$(grep "BlackHoleSink: WriteEvents" "$WORK_DIR/cdc_off_blackhole.log" 2>/dev/null || echo "")
	log_raw_content "BlackHole DMLEvent logs (OFF mode)" "$captured_logs"

	# STRICT POSITIVE VALIDATION: DMLEvent with raw data MUST be present
	echo "  [1/2] Verifying DMLEvent logs contain raw sensitive data:"
	require_log_pattern "$WORK_DIR/cdc_off_blackhole.log" \
		"BlackHoleSink: WriteEvents.*Row:.*Password1!" \
		"DMLEvent shows plain text row data (Password1! visible)" \
		"BlackHole sink should log DMLEvent with raw row values in OFF mode"

	# Also verify Row format is present (ensures safeRowToString was called)
	echo "  [2/2] Verifying Row format in DMLEvent output:"
	require_log_pattern "$WORK_DIR/cdc_off_blackhole.log" \
		"Row:.*," \
		"Row data format is present in logs" \
		"safeRowToString() should format row data with comma separators"

	# Clear captured_logs after use
	captured_logs=""

	echo "[$(date)] ✓ OFF mode (BlackHole): Raw data visible in DMLEvent logs"
	cleanup_process $CDC_BINARY

	# ==========================================================================
	# Test 2: Redaction MARKER mode with BlackHole sink
	# ==========================================================================
	echo ""
	echo "=== Test 2: Redaction MARKER mode (BlackHole sink) ==="
	run_sql "DROP DATABASE IF EXISTS log_redaction_test;"
	run_sql "DROP DATABASE IF EXISTS log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE DATABASE log_redaction_test;"
	run_sql "CREATE DATABASE log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log marker --logsuffix "_marker_blackhole"

	cdc_cli_changefeed create --sink-uri="$BLACKHOLE_SINK_URI" --changefeed-id="blackhole-marker-test" --config=$CUR/conf/changefeed.toml

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	echo "  Waiting for BlackHole sink to process events..."
	wait_for_log_content "$WORK_DIR/cdc_marker_blackhole.log" "BlackHoleSink: WriteEvents" "DMLEvent logs" 30

	echo "  [Validation] MARKER mode with BlackHole sink:"
	echo ""

	# Capture BlackHole logs once for all validations
	captured_logs=$(grep "BlackHoleSink: WriteEvents" "$WORK_DIR/cdc_marker_blackhole.log" 2>/dev/null || echo "")
	log_raw_content "BlackHole DMLEvent logs (MARKER mode)" "$captured_logs"

	# STRICT POSITIVE VALIDATION: DMLEvent MUST have markers wrapping data
	echo "  [1/3] Verifying DMLEvent logs have ‹› markers:"
	require_log_pattern "$WORK_DIR/cdc_marker_blackhole.log" \
		"BlackHoleSink: WriteEvents.*Row:.*‹" \
		"DMLEvent row data wrapped with ‹› markers" \
		"MARKER mode should wrap sensitive data with ‹› markers"

	echo "  [2/3] Verifying sensitive data is wrapped (not raw):"
	# Check that Password1! appears ONLY inside markers (using captured_logs)
	if echo "$captured_logs" | grep -q "Password1!" &&
		echo "$captured_logs" | grep -q "‹.*Password1!.*›"; then
		echo "    ✓ VALIDATED: Password1! is wrapped in ‹› markers"
	else
		# Check if raw Password1! appears without markers (this is a failure)
		if echo "$captured_logs" | grep -v "‹" | grep -q "Password1!"; then
			echo "    ✗ FAILED: Password1! appears without ‹› markers"
			exit 1
		fi
		echo "    ✓ VALIDATED: Sensitive data properly handled in MARKER mode"
	fi

	echo "  [3/3] Verifying closing marker is present:"
	require_log_pattern "$WORK_DIR/cdc_marker_blackhole.log" \
		"›" \
		"Closing marker › is present" \
		"MARKER mode must have both opening ‹ and closing › markers"

	# Clear captured_logs after use
	captured_logs=""

	echo "[$(date)] ✓ MARKER mode (BlackHole): Data wrapped with ‹› markers"
	cleanup_process $CDC_BINARY

	# ==========================================================================
	# Test 3: Redaction ON mode with BlackHole sink
	# ==========================================================================
	echo ""
	echo "=== Test 3: Redaction ON mode (BlackHole sink) ==="
	run_sql "DROP DATABASE IF EXISTS log_redaction_test;"
	run_sql "DROP DATABASE IF EXISTS log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE DATABASE log_redaction_test;"
	run_sql "CREATE DATABASE log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log on --logsuffix "_on_blackhole"

	cdc_cli_changefeed create --sink-uri="$BLACKHOLE_SINK_URI" --changefeed-id="blackhole-on-test" --config=$CUR/conf/changefeed.toml

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	echo "  Waiting for BlackHole sink to process events..."
	wait_for_log_content "$WORK_DIR/cdc_on_blackhole.log" "BlackHoleSink: WriteEvents" "DMLEvent logs" 30

	echo "  [Validation] ON mode with BlackHole sink:"
	echo ""

	# Capture BlackHole logs once for all validations
	captured_logs=$(grep "BlackHoleSink: WriteEvents" "$WORK_DIR/cdc_on_blackhole.log" 2>/dev/null || echo "")
	log_raw_content "BlackHole DMLEvent logs (ON mode)" "$captured_logs"

	# STRICT POSITIVE VALIDATION: DMLEvent MUST show redacted format (?)
	echo "  [1/3] Verifying DMLEvent logs show redacted '?' placeholder:"
	require_log_pattern "$WORK_DIR/cdc_on_blackhole.log" \
		"BlackHoleSink: WriteEvents.*Row:.*\?" \
		"DMLEvent row data shows '?' redaction placeholder" \
		"ON mode should replace all sensitive data with '?'"

	# STRICT NEGATIVE VALIDATION: No sensitive data should leak
	echo "  [2/3] Verifying NO sensitive data leaks in logs:"
	sensitive_patterns=("Password1!" "SecretPass1!" "SecretPass2!" "4532-1000-1000" "4532-2000-2000")
	for pattern in "${sensitive_patterns[@]}"; do
		require_no_log_pattern "$WORK_DIR/cdc_on_blackhole.log" \
			"$pattern" \
			"No leak of sensitive value: $pattern"
	done

	echo "  [3/3] Verifying Row format shows only '?' (no actual values):"
	# Extract Row content from captured_logs and verify it's just "?"
	row_content=$(echo "$captured_logs" | grep -o "Row: [^;]*" | head -1 || echo "")
	if [ -n "$row_content" ]; then
		log_raw_content "Row content in ON mode" "$row_content"
		if echo "$row_content" | grep -q "Row: \?"; then
			echo "    ✓ VALIDATED: Row shows only '?' placeholder"
		else
			echo "    ✗ FAILED: Row should show only '?' but got: $row_content"
			exit 1
		fi
	else
		echo "    ✗ FAILED: No Row content found in logs"
		exit 1
	fi

	# Clear captured_logs after use
	captured_logs=""

	echo "[$(date)] ✓ ON mode (BlackHole): All sensitive data fully redacted to '?'"
	cleanup_process $CDC_BINARY

	# ==========================================================================
	# Test 4: MySQL sink validation (original test with strict validation)
	# ==========================================================================
	if [ "$SINK_TYPE" = "mysql" ]; then
		echo ""
		echo "=== Test 4: MySQL sink redaction validation ==="
		run_sql "DROP DATABASE IF EXISTS log_redaction_test;"
		run_sql "DROP DATABASE IF EXISTS log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
		run_sql "CREATE DATABASE log_redaction_test;"
		run_sql "CREATE DATABASE log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

		# Test OFF mode with MySQL sink
		echo "  [4a] OFF mode with MySQL sink:"
		run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log off --logsuffix "_off_mysql"

		SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
		cdc_cli_changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml

		run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		check_table_exists log_redaction_test.users ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
		check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

		# Capture MySQL logs once for all validations
		captured_logs=$(grep -E "(Query:|Args:)" "$WORK_DIR/cdc_off_mysql.log" 2>/dev/null || echo "")
		log_raw_content "MySQL Query/Args logs (OFF mode)" "$captured_logs"

		# STRICT: Query and Args must show raw data
		require_log_pattern "$WORK_DIR/cdc_off_mysql.log" \
			"Query:.*Password1!" \
			"MySQL Query shows plain text values"
		require_log_pattern "$WORK_DIR/cdc_off_mysql.log" \
			"Args:.*Password1!" \
			"MySQL Args shows plain text values"

		captured_logs=""
		cleanup_process $CDC_BINARY

		# Test MARKER mode with MySQL sink
		echo ""
		echo "  [4b] MARKER mode with MySQL sink:"
		run_sql "DROP DATABASE IF EXISTS log_redaction_test;"
		run_sql "DROP DATABASE IF EXISTS log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
		run_sql "CREATE DATABASE log_redaction_test;"
		run_sql "CREATE DATABASE log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

		run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log marker --logsuffix "_marker_mysql"
		cdc_cli_changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml

		run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		sleep 3

		# Capture MySQL logs once for all validations
		captured_logs=$(grep -E "(Query:|Args:)" "$WORK_DIR/cdc_marker_mysql.log" 2>/dev/null || echo "")
		log_raw_content "MySQL Query/Args logs (MARKER mode)" "$captured_logs"

		# STRICT: Query and Args must have markers
		require_log_pattern "$WORK_DIR/cdc_marker_mysql.log" \
			"Query:.*‹" \
			"MySQL Query wrapped with ‹› markers"
		require_log_pattern "$WORK_DIR/cdc_marker_mysql.log" \
			"Args:.*‹.*Password1!.*›" \
			"MySQL Args values wrapped with ‹› markers"

		captured_logs=""
		cleanup_process $CDC_BINARY

		# Test ON mode with MySQL sink
		echo ""
		echo "  [4c] ON mode with MySQL sink:"
		run_sql "DROP DATABASE IF EXISTS log_redaction_test;"
		run_sql "DROP DATABASE IF EXISTS log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
		run_sql "CREATE DATABASE log_redaction_test;"
		run_sql "CREATE DATABASE log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

		run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log on --logsuffix "_on_mysql"
		cdc_cli_changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml

		run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		sleep 3

		# Capture MySQL logs once for all validations
		captured_logs=$(grep -E "(Query:|Args:)" "$WORK_DIR/cdc_on_mysql.log" 2>/dev/null || echo "")
		log_raw_content "MySQL Query/Args logs (ON mode)" "$captured_logs"

		# STRICT: Query and Args must show '?' placeholder
		require_log_pattern "$WORK_DIR/cdc_on_mysql.log" \
			"Query: \?" \
			"MySQL Query redacted to '?'"
		require_log_pattern "$WORK_DIR/cdc_on_mysql.log" \
			"Args:.*\?" \
			"MySQL Args redacted to '?' placeholders"

		# STRICT: No sensitive data should leak
		require_no_log_pattern "$WORK_DIR/cdc_on_mysql.log" \
			"Password1!" \
			"No sensitive data leaked in MySQL sink ON mode"

		captured_logs=""
		cleanup_process $CDC_BINARY

		echo "[$(date)] ✓ MySQL sink: All redaction modes validated"
	fi

	# ==========================================================================
	# Test 4b: Kafka sink validation (tests Kafka-specific redaction)
	# ==========================================================================
	if [ "$SINK_TYPE" = "kafka" ]; then
		echo ""
		echo "=== Test 4b: Kafka sink redaction validation ==="

		# Kafka sink logs message key/value at DEBUG level
		# Log message: "send message to kafka" with messageKey and messageValue fields

		# Test ON mode with Kafka sink (most important - full redaction)
		echo "  [4b-1] ON mode with Kafka sink:"
		run_sql "DROP DATABASE IF EXISTS log_redaction_test;"
		run_sql "CREATE DATABASE log_redaction_test;"

		KAFKA_TOPIC="log-redaction-test-$RANDOM"
		KAFKA_SINK_URI="kafka://127.0.0.1:9092/$KAFKA_TOPIC?protocol=open-protocol"

		run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log on --logsuffix "_on_kafka"

		cdc_cli_changefeed create --sink-uri="$KAFKA_SINK_URI" --changefeed-id="kafka-on-test" --config=$CUR/conf/changefeed.toml

		run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

		echo "  Waiting for Kafka sink to process events..."
		wait_for_log_content "$WORK_DIR/cdc_on_kafka.log" "send message to kafka" "Kafka message logs" 30

		echo "  [Validation] ON mode with Kafka sink:"
		echo ""

		# Capture Kafka logs once for all validations
		captured_logs=$(grep "send message to kafka" "$WORK_DIR/cdc_on_kafka.log" 2>/dev/null || echo "")
		log_raw_content "Kafka message logs (ON mode)" "$captured_logs"

		# STRICT POSITIVE VALIDATION: messageKey and messageValue must show redacted format
		echo "  [1/2] Verifying Kafka logs show redacted '?' placeholder:"
		require_log_pattern "$WORK_DIR/cdc_on_kafka.log" \
			"send message to kafka.*messageKey.*\?.*messageValue.*\?" \
			"Kafka messageKey and messageValue redacted to '?'" \
			"ON mode should redact both messageKey and messageValue"

		# STRICT NEGATIVE VALIDATION: No sensitive data should leak in Kafka logs
		echo "  [2/2] Verifying NO sensitive data leaks in Kafka logs:"
		sensitive_patterns=("Password1!" "SecretPass1!" "user1@example.com" "4532-1000-1000")
		for pattern in "${sensitive_patterns[@]}"; do
			require_no_log_pattern "$WORK_DIR/cdc_on_kafka.log" \
				"$pattern" \
				"No leak of sensitive value in Kafka logs: $pattern"
		done

		captured_logs=""
		cleanup_process $CDC_BINARY

		# Test MARKER mode with Kafka sink
		echo ""
		echo "  [4b-2] MARKER mode with Kafka sink:"
		run_sql "DROP DATABASE IF EXISTS log_redaction_test;"
		run_sql "CREATE DATABASE log_redaction_test;"

		KAFKA_TOPIC="log-redaction-marker-$RANDOM"
		KAFKA_SINK_URI="kafka://127.0.0.1:9092/$KAFKA_TOPIC?protocol=open-protocol"

		run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log marker --logsuffix "_marker_kafka"

		cdc_cli_changefeed create --sink-uri="$KAFKA_SINK_URI" --changefeed-id="kafka-marker-test" --config=$CUR/conf/changefeed.toml

		run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

		echo "  Waiting for Kafka sink to process events..."
		wait_for_log_content "$WORK_DIR/cdc_marker_kafka.log" "send message to kafka" "Kafka message logs" 30

		echo "  [Validation] MARKER mode with Kafka sink:"
		echo ""

		# Capture Kafka logs once for all validations
		captured_logs=$(grep "send message to kafka" "$WORK_DIR/cdc_marker_kafka.log" 2>/dev/null || echo "")
		log_raw_content "Kafka message logs (MARKER mode)" "$captured_logs"

		# STRICT POSITIVE VALIDATION: messageKey and messageValue must have markers
		echo "  [1/1] Verifying Kafka logs have ‹› markers:"
		require_log_pattern "$WORK_DIR/cdc_marker_kafka.log" \
			"send message to kafka.*‹" \
			"Kafka message values wrapped with ‹› markers" \
			"MARKER mode should wrap Kafka message data with ‹› markers"

		captured_logs=""
		cleanup_process $CDC_BINARY

		echo "[$(date)] ✓ Kafka sink: Redaction modes validated"
	fi

	# ==========================================================================
	# Test 5: API mode switching
	# ==========================================================================
	echo ""
	echo "=== Test 5: API mode switching ==="
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log off --logsuffix "_api" --addr "127.0.0.1:8300"

	# OFF -> MARKER transition
	echo "  Testing OFF -> MARKER transition:"
	response=$(curl -s -w "\n%{http_code}" -X POST \
		--user ticdc:ticdc_secret \
		-H "Content-Type: application/json" \
		-d '{"redact_info_log": "marker"}' \
		http://127.0.0.1:8300/api/v2/log/redact)
	http_code=$(echo "$response" | tail -n1)
	response_body=$(echo "$response" | sed '$d')

	if [ "$http_code" = "200" ]; then
		echo "    ✓ VALIDATED: OFF -> MARKER transition succeeded"
	else
		echo "    ✗ FAILED: OFF -> MARKER transition failed (HTTP $http_code)"
		log_raw_content "API Response" "$response_body"
		exit 1
	fi

	# MARKER -> ON transition
	echo "  Testing MARKER -> ON transition:"
	response=$(curl -s -w "\n%{http_code}" -X POST \
		--user ticdc:ticdc_secret \
		-H "Content-Type: application/json" \
		-d '{"redact_info_log": "on"}' \
		http://127.0.0.1:8300/api/v2/log/redact)
	http_code=$(echo "$response" | tail -n1)

	if [ "$http_code" = "200" ]; then
		echo "    ✓ VALIDATED: MARKER -> ON transition succeeded"
	else
		echo "    ✗ FAILED: MARKER -> ON transition failed (HTTP $http_code)"
		exit 1
	fi

	# ON -> OFF transition (should FAIL - security restriction)
	echo "  Testing ON -> OFF transition (should be rejected):"
	response=$(curl -s -w "\n%{http_code}" -X POST \
		--user ticdc:ticdc_secret \
		-H "Content-Type: application/json" \
		-d '{"redact_info_log": "off"}' \
		http://127.0.0.1:8300/api/v2/log/redact)
	http_code=$(echo "$response" | tail -n1)

	if [ "$http_code" = "400" ]; then
		echo "    ✓ VALIDATED: ON -> OFF transition correctly rejected (security feature)"
	else
		echo "    ✗ FAILED: ON -> OFF should be rejected but got HTTP $http_code"
		exit 1
	fi

	cleanup_process $CDC_BINARY

	# ==========================================================================
	# Test 6: API error handling for invalid modes
	# ==========================================================================
	echo ""
	echo "=== Test 6: API error handling for invalid modes ==="
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log off --logsuffix "_api_error" --addr "127.0.0.1:8300"

	# Test "maker" typo (common mistake for "marker")
	echo "  Testing 'maker' typo (should return error):"
	response=$(curl -s -w "\n%{http_code}" -X POST \
		--user ticdc:ticdc_secret \
		-H "Content-Type: application/json" \
		-d '{"redact_info_log": "maker"}' \
		http://127.0.0.1:8300/api/v2/log/redact)
	http_code=$(echo "$response" | tail -n1)
	response_body=$(echo "$response" | sed '$d')

	if [ "$http_code" = "400" ]; then
		echo "    ✓ HTTP 400 returned for 'maker' typo"
		if echo "$response_body" | grep -q "invalid redact mode"; then
			echo "    ✓ VALIDATED: Error message contains 'invalid redact mode'"
		else
			echo "    ✗ FAILED: Error message should contain 'invalid redact mode'"
			exit 1
		fi
	else
		echo "    ✗ FAILED: Expected HTTP 400 but got $http_code"
		exit 1
	fi

	# Test another invalid mode
	echo "  Testing 'invalid_mode' (should return error):"
	response=$(curl -s -w "\n%{http_code}" -X POST \
		--user ticdc:ticdc_secret \
		-H "Content-Type: application/json" \
		-d '{"redact_info_log": "invalid_mode"}' \
		http://127.0.0.1:8300/api/v2/log/redact)
	http_code=$(echo "$response" | tail -n1)

	if [ "$http_code" = "400" ]; then
		echo "    ✓ VALIDATED: HTTP 400 returned for 'invalid_mode'"
	else
		echo "    ✗ FAILED: Expected HTTP 400 but got $http_code"
		exit 1
	fi

	echo "[$(date)] ✓ API error handling: Invalid modes return proper errors"
	cleanup_process $CDC_BINARY

	echo ""
	echo "=========================================="
	echo "All log redaction tests passed!"
	echo "=========================================="
}

trap stop_tidb_cluster EXIT
run $*
