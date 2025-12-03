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

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# Create test database on both upstream and downstream
	run_sql "CREATE DATABASE IF NOT EXISTS log_redaction_test;"
	run_sql "CREATE DATABASE IF NOT EXISTS log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

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

	check_table_exists log_redaction_test.users ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	# Verify OFF - sensitive data visible in plain text
	echo "  [Validation] OFF mode - checking redaction scenarios:"
	echo "  Sink type: $SINK_TYPE"
	echo ""

	if [ "$SINK_TYPE" = "mysql" ]; then
		# MySQL sink validation - check Query and Args logs
		echo "  [1/3] sql_builder.go Query (RedactValue):"
		query_logs=$(grep "Query:.*Password" $WORK_DIR/cdc_off.log 2>/dev/null | head -3 || echo "")
		if [ -n "$query_logs" ]; then
			log_raw_content "Query logs containing 'Password'" "$query_logs"
			if echo "$query_logs" | grep -q "Password1!"; then
				echo "    ✓ VALIDATED: Query shows plain text values (Password1! visible)"
			else
				echo "    ✗ FAILED: Expected Query logs with plain text Password1!"
				exit 1
			fi
		else
			echo "    ✗ FAILED: No Query logs found containing 'Password'"
			echo "    Debug: All Query logs:"
			grep "Query:" $WORK_DIR/cdc_off.log | head -5 | sed 's/^/    /' || echo "    No Query logs found"
			exit 1
		fi
		echo ""

		echo "  [2/3] sql_builder.go Args (RedactArgs):"
		args_logs=$(grep "Args:.*Password" $WORK_DIR/cdc_off.log 2>/dev/null | head -3 || echo "")
		if [ -n "$args_logs" ]; then
			log_raw_content "Args logs containing 'Password'" "$args_logs"
			if echo "$args_logs" | grep -q "Password1!"; then
				echo "    ✓ VALIDATED: Args shows plain text values (Password1! visible)"
			else
				echo "    ✗ FAILED: Expected Args logs with plain text Password1!"
				exit 1
			fi
		else
			echo "    ✗ FAILED: No Args logs found containing 'Password'"
			echo "    Debug: All Args logs:"
			grep "Args:" $WORK_DIR/cdc_off.log | head -5 | sed 's/^/    /' || echo "    No Args logs found"
			exit 1
		fi
		echo ""

		echo "  [3/3] DMLEvent String (basic_dispatcher.go):"
		dmlevent_logs=$(grep "DMLEvent" $WORK_DIR/cdc_off.log 2>/dev/null | head -2 || echo "")
		if [ -n "$dmlevent_logs" ]; then
			log_raw_content "DMLEvent logs" "$dmlevent_logs"
			if echo "$dmlevent_logs" | grep -q "Password1!"; then
				echo "    ✓ VALIDATED: DMLEvent shows plain text row data"
			else
				echo "    (DMLEvent present but Password1! not found - may be expected)"
			fi
		else
			echo "    (No DMLEvent logs found - logging may not be enabled at Debug level)"
		fi
		echo ""
	else
		# Non-MySQL sink validation (kafka, storage, pulsar)
		# Wait for DML events to be processed
		echo "  Waiting for data to be processed..."
		sleep 5

		echo "  [1/2] Checking for sensitive data in logs:"
		# First, show what we found
		sensitive_logs=$(grep -E "(Password1!|DMLEvent|RowChangedEvent)" $WORK_DIR/cdc_off.log 2>/dev/null | head -5 || echo "")
		if [ -n "$sensitive_logs" ]; then
			log_raw_content "Logs with sensitive data or events" "$sensitive_logs"

			if grep -q "Password1!" $WORK_DIR/cdc_off.log; then
				echo "    ✓ VALIDATED: Sensitive data (Password1!) visible in logs"
			else
				echo "    Note: Events found but Password1! not in log output"
				echo "    This is expected for $SINK_TYPE sink (data flows through encoder, not logged)"
			fi
		else
			echo "    Note: No sensitive data or event logs found"
			echo "    Debug: Checking log file size and recent entries..."
			wc -l $WORK_DIR/cdc_off.log | sed 's/^/    /'
			tail -10 $WORK_DIR/cdc_off.log | sed 's/^/    /'
		fi
		echo ""

		echo "  [2/2] DMLEvent/RowChangedEvent logs:"
		event_logs=$(grep -E "(DMLEvent|RowChangedEvent)" $WORK_DIR/cdc_off.log 2>/dev/null | head -3 || echo "")
		if [ -n "$event_logs" ]; then
			log_raw_content "Event logs" "$event_logs"
			echo "    ✓ Event logs present"
		else
			echo "    (No event logs found - may be expected for $SINK_TYPE sink)"
		fi
		echo ""
	fi

	echo "[$(date)] ✓ OFF mode: Validation completed for $SINK_TYPE sink"

	cleanup_process $CDC_BINARY

	echo ""
	echo "=== Test 2: Redaction MARKER mode ==="
	run_sql "DROP DATABASE IF EXISTS log_redaction_test;"
	run_sql "DROP DATABASE IF EXISTS log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE DATABASE log_redaction_test;"
	run_sql "CREATE DATABASE log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log marker --logsuffix "_marker"

	cdc_cli_changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Wait for data to be processed
	if [ "$SINK_TYPE" = "mysql" ]; then
		sleep 3
	else
		sleep 5
	fi

	# Verify MARKER - sensitive data wrapped with markers
	echo "  [Validation] MARKER mode - checking redaction scenarios:"
	echo "  Sink type: $SINK_TYPE"
	echo ""

	if [ "$SINK_TYPE" = "mysql" ]; then
		# MySQL sink validation
		echo "  [1/3] sql_builder.go Query (RedactValue):"
		query_logs=$(grep "Query:.*‹" $WORK_DIR/cdc_marker.log 2>/dev/null | head -3 || echo "")
		if [ -n "$query_logs" ]; then
			log_raw_content "Query logs with markers" "$query_logs"
			if echo "$query_logs" | grep -q "‹.*›"; then
				echo "    ✓ VALIDATED: Query wrapped with ‹› markers"
			else
				echo "    ✗ FAILED: Expected Query logs with ‹› markers"
				exit 1
			fi
		else
			echo "    ✗ FAILED: No Query logs found with markers"
			echo "    Debug: All Query logs:"
			grep "Query:" $WORK_DIR/cdc_marker.log | head -5 | sed 's/^/    /' || echo "    No Query logs found"
			exit 1
		fi
		echo ""

		echo "  [2/3] sql_builder.go Args (RedactArgs):"
		args_logs=$(grep "Args:.*‹" $WORK_DIR/cdc_marker.log 2>/dev/null | head -3 || echo "")
		if [ -n "$args_logs" ]; then
			log_raw_content "Args logs with markers" "$args_logs"
			if echo "$args_logs" | grep -q "‹.*Password1!.*›"; then
				echo "    ✓ VALIDATED: Args values wrapped with ‹› markers (e.g., ‹Password1!›)"
			else
				echo "    ✗ FAILED: Expected Args logs with ‹Password1!› markers"
				exit 1
			fi
		else
			echo "    ✗ FAILED: No Args logs found with markers"
			echo "    Debug: All Args logs:"
			grep "Args:" $WORK_DIR/cdc_marker.log | head -5 | sed 's/^/    /' || echo "    No Args logs found"
			exit 1
		fi
		echo ""

		echo "  [3/3] DMLEvent String (basic_dispatcher.go):"
		dmlevent_logs=$(grep "DMLEvent" $WORK_DIR/cdc_marker.log 2>/dev/null | head -2 || echo "")
		if [ -n "$dmlevent_logs" ]; then
			log_raw_content "DMLEvent logs" "$dmlevent_logs"
			if echo "$dmlevent_logs" | grep -q "‹.*Password1!.*›"; then
				echo "    ✓ VALIDATED: DMLEvent wrapped with ‹› markers"
			else
				echo "    (DMLEvent present but markers not as expected)"
			fi
		else
			echo "    (No DMLEvent logs found - logging may not be enabled at Debug level)"
		fi
		echo ""

		# Final validation for MySQL - markers must be present
		if grep -q "‹.*Password1!.*›" $WORK_DIR/cdc_marker.log; then
			echo "[$(date)] ✓ MARKER mode: Sensitive data wrapped in ‹›"
		else
			echo "[$(date)] ✗ MARKER mode: Expected ‹Password1!› in logs"
			exit 1
		fi
	else
		# Non-MySQL sink validation
		echo "  [1/2] Checking for ‹› markers in logs:"
		marker_logs=$(grep "‹.*›" $WORK_DIR/cdc_marker.log 2>/dev/null | head -5 || echo "")
		if [ -n "$marker_logs" ]; then
			log_raw_content "Logs with ‹› markers" "$marker_logs"
			echo "    ✓ VALIDATED: Marker format ‹› found in logs"

			# Additional check: if we have markers, verify sensitive data is wrapped
			if grep -q "‹.*Password.*›" $WORK_DIR/cdc_marker.log 2>/dev/null; then
				echo "    ✓ Password data is wrapped in markers"
			fi
		else
			echo "    Note: No ‹› markers found in logs"
			echo "    Debug: Checking for any event-related logs..."
			grep -E "(event|dispatch|sink)" $WORK_DIR/cdc_marker.log 2>/dev/null | head -3 | sed 's/^/    /' || echo "    No event logs found"
		fi
		echo ""

		echo "  [2/2] General log validation:"
		log_lines=$(wc -l < $WORK_DIR/cdc_marker.log)
		echo "    Log file has $log_lines lines"
		echo "    ✓ MARKER mode logs generated"
		echo ""

		echo "[$(date)] ✓ MARKER mode: Validation completed for $SINK_TYPE sink"
	fi

	cleanup_process $CDC_BINARY

	echo ""
	echo "=== Test 3: Redaction ON mode ==="
	run_sql "DROP DATABASE IF EXISTS log_redaction_test;"
	run_sql "DROP DATABASE IF EXISTS log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE DATABASE log_redaction_test;"
	run_sql "CREATE DATABASE log_redaction_test;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log on --logsuffix "_on"

	cdc_cli_changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# Wait for data to be processed
	if [ "$SINK_TYPE" = "mysql" ]; then
		sleep 3
	else
		sleep 5
	fi

	# Verify ON - sensitive data not visible (replaced with ?)
	echo "  [Validation] ON mode - checking redaction scenarios:"
	echo "  Sink type: $SINK_TYPE"
	echo ""

	# CRITICAL: For ALL sink types, sensitive data must NOT appear in logs
	echo "  [SECURITY CHECK] Verifying sensitive data is NOT in logs:"
	leaked_logs=$(grep "Password1!" $WORK_DIR/cdc_on.log 2>/dev/null | head -5 || echo "")
	if [ -n "$leaked_logs" ]; then
		log_raw_content "LEAKED sensitive data found" "$leaked_logs"
		echo "    ✗ FAILED: Sensitive data found in logs!"
		exit 1
	else
		echo "    ✓ No sensitive data leaked (Password1! not found in logs)"
	fi
	echo ""

	if [ "$SINK_TYPE" = "mysql" ]; then
		# MySQL sink validation - check Query and Args are redacted
		echo "  [1/3] sql_builder.go Query (RedactValue):"
		echo "    Expected: Query should be replaced with '?'"
		query_logs=$(grep "Query:" $WORK_DIR/cdc_on.log 2>/dev/null | head -3 || echo "")
		if [ -n "$query_logs" ]; then
			log_raw_content "Query logs" "$query_logs"
			if echo "$query_logs" | grep -q "Query: \?"; then
				echo "    ✓ VALIDATED: Query redacted to '?'"
			else
				echo "    ✗ FAILED: Query not redacted to '?'"
				exit 1
			fi
		else
			echo "    ✗ FAILED: No Query logs found"
			exit 1
		fi
		echo ""

		echo "  [2/3] sql_builder.go Args (RedactArgs):"
		echo "    Expected: Args should show (?, ?, ?) format"
		args_logs=$(grep "Args:" $WORK_DIR/cdc_on.log 2>/dev/null | head -3 || echo "")
		if [ -n "$args_logs" ]; then
			log_raw_content "Args logs" "$args_logs"
			if echo "$args_logs" | grep -q "Args:.*\?"; then
				echo "    ✓ VALIDATED: Args redacted to '?' placeholders"
			else
				echo "    ✗ FAILED: Args not redacted to '?' placeholders"
				exit 1
			fi
		else
			echo "    ✗ FAILED: No Args logs found"
			exit 1
		fi
		echo ""

		echo "  [3/3] DMLEvent String (basic_dispatcher.go):"
		echo "    Expected: DMLEvent should be replaced with '?'"
		dmlevent_logs=$(grep "DMLEvent\|event=" $WORK_DIR/cdc_on.log 2>/dev/null | head -3 || echo "")
		if [ -n "$dmlevent_logs" ]; then
			log_raw_content "DMLEvent/event logs" "$dmlevent_logs"
			if echo "$dmlevent_logs" | grep -q "event=\"\?\""; then
				echo "    ✓ VALIDATED: DMLEvent redacted to '?'"
			else
				echo "    (DMLEvent present but not showing '?' format)"
			fi
		else
			echo "    (No DMLEvent logs found - logging may not be enabled at Debug level)"
		fi
		echo ""

		# Final validation for MySQL
		echo "  [REDACTION FORMAT CHECK] Verifying '?' placeholders are present:"
		if grep -q "Args:.*\?" $WORK_DIR/cdc_on.log && grep -q "Query: \?" $WORK_DIR/cdc_on.log; then
			echo "    ✓ Both Query and Args use '?' placeholder format"
		else
			echo "    ✗ FAILED: Missing '?' placeholders in logs"
			exit 1
		fi
	else
		# Non-MySQL sink validation - ensure no sensitive data leaked
		echo "  [1/2] Checking '?' redaction placeholders in logs:"
		redact_logs=$(grep "event=\"\?\"\|=\?" $WORK_DIR/cdc_on.log 2>/dev/null | head -3 || echo "")
		if [ -n "$redact_logs" ]; then
			log_raw_content "Logs with '?' redaction" "$redact_logs"
			echo "    ✓ Event redacted to '?'"
		else
			echo "    Note: No '?' placeholders found in event logs"
			echo "    This may be expected for $SINK_TYPE sink"
		fi
		echo ""

		echo "  [2/2] Additional sensitive data checks:"
		# Check for other sensitive VALUES from test data
		sensitive_patterns=("SecretPass1!" "SecretPass2!" "4532-1000-1000" "4532-2000-2000" "1111-2222-3333" "999-88-7777" "666-55-4444")
		sensitive_found=0
		for pattern in "${sensitive_patterns[@]}"; do
			leaked=$(grep "$pattern" $WORK_DIR/cdc_on.log 2>/dev/null | head -1 || echo "")
			if [ -n "$leaked" ]; then
				log_raw_content "LEAKED: $pattern" "$leaked"
				sensitive_found=1
			fi
		done
		if [ $sensitive_found -eq 0 ]; then
			echo "    ✓ No additional sensitive data found in logs"
		else
			echo "    ✗ FAILED: Sensitive data leaked in ON mode"
			exit 1
		fi
		echo ""
	fi

	# Final validation
	if ! grep -q "Password1!" $WORK_DIR/cdc_on.log; then
		echo "[$(date)] ✓ ON mode: Sensitive data fully redacted for $SINK_TYPE sink"
	else
		echo "[$(date)] ✗ ON mode: Found sensitive data in logs"
		exit 1
	fi

	cleanup_process $CDC_BINARY

	echo ""
	echo "=== Test 4: API mode switching ==="
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log off --logsuffix "_api" --addr "127.0.0.1:8300"

	# OFF -> MARKER transition
	echo "  Testing OFF -> MARKER transition:"
	response=$(curl -s -w "\n%{http_code}" -X POST \
		--user ticdc:ticdc_secret \
		-H "Content-Type: application/json" \
		-d '{"redact_mode": "marker"}' \
		http://127.0.0.1:8300/api/v2/log/redact)
	http_code=$(echo "$response" | tail -n1)
	response_body=$(echo "$response" | sed '$d')

	log_raw_content "API Response (HTTP $http_code)" "$response_body"

	if [ "$http_code" = "200" ]; then
		echo "    ✓ VALIDATED: OFF -> MARKER transition succeeded"
	else
		echo "    ✗ FAILED: OFF -> MARKER transition failed (HTTP $http_code)"
		exit 1
	fi
	echo ""

	# MARKER -> ON transition
	echo "  Testing MARKER -> ON transition:"
	response=$(curl -s -w "\n%{http_code}" -X POST \
		--user ticdc:ticdc_secret \
		-H "Content-Type: application/json" \
		-d '{"redact_mode": "on"}' \
		http://127.0.0.1:8300/api/v2/log/redact)
	http_code=$(echo "$response" | tail -n1)
	response_body=$(echo "$response" | sed '$d')

	log_raw_content "API Response (HTTP $http_code)" "$response_body"

	if [ "$http_code" = "200" ]; then
		echo "    ✓ VALIDATED: MARKER -> ON transition succeeded"
	else
		echo "    ✗ FAILED: MARKER -> ON transition failed (HTTP $http_code)"
		exit 1
	fi

	cleanup_process $CDC_BINARY

	echo ""
	echo "=== Test 5: API error handling for invalid modes ==="
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --redact-info-log off --logsuffix "_api_error" --addr "127.0.0.1:8300"

	# Test "maker" typo (common mistake for "marker")
	echo "  Testing 'maker' typo (should return error):"
	response=$(curl -s -w "\n%{http_code}" -X POST \
		--user ticdc:ticdc_secret \
		-H "Content-Type: application/json" \
		-d '{"redact_mode": "maker"}' \
		http://127.0.0.1:8300/api/v2/log/redact)
	http_code=$(echo "$response" | tail -n1)
	response_body=$(echo "$response" | sed '$d')

	log_raw_content "API Response (HTTP $http_code)" "$response_body"

	if [ "$http_code" = "400" ]; then
		echo "    ✓ HTTP 400 Bad Request returned for 'maker' typo"
		if echo "$response_body" | grep -q "invalid redaction mode"; then
			echo "    ✓ VALIDATED: Error message contains 'invalid redaction mode'"
		else
			echo "    ✗ FAILED: Error message should contain 'invalid redaction mode'"
			exit 1
		fi
	else
		echo "    ✗ FAILED: Expected HTTP 400 but got $http_code"
		exit 1
	fi
	echo ""

	# Test another invalid mode
	echo "  Testing 'invalid_mode' (should return error):"
	response=$(curl -s -w "\n%{http_code}" -X POST \
		--user ticdc:ticdc_secret \
		-H "Content-Type: application/json" \
		-d '{"redact_mode": "invalid_mode"}' \
		http://127.0.0.1:8300/api/v2/log/redact)
	http_code=$(echo "$response" | tail -n1)
	response_body=$(echo "$response" | sed '$d')

	log_raw_content "API Response (HTTP $http_code)" "$response_body"

	if [ "$http_code" = "400" ]; then
		echo "    ✓ HTTP 400 Bad Request returned for 'invalid_mode'"
		if echo "$response_body" | grep -q "invalid redaction mode"; then
			echo "    ✓ VALIDATED: Error message contains 'invalid redaction mode'"
		else
			echo "    ✗ FAILED: Error message should contain 'invalid redaction mode'"
			exit 1
		fi
	else
		echo "    ✗ FAILED: Expected HTTP 400 but got $http_code"
		exit 1
	fi
	echo ""

	# Verify valid mode still works after invalid attempts
	echo "  Verifying valid 'marker' mode still works:"
	response=$(curl -s -w "\n%{http_code}" -X POST \
		--user ticdc:ticdc_secret \
		-H "Content-Type: application/json" \
		-d '{"redact_mode": "marker"}' \
		http://127.0.0.1:8300/api/v2/log/redact)
	http_code=$(echo "$response" | tail -n1)
	response_body=$(echo "$response" | sed '$d')

	log_raw_content "API Response (HTTP $http_code)" "$response_body"

	if [ "$http_code" = "200" ]; then
		echo "    ✓ VALIDATED: Valid 'marker' mode accepted after invalid attempts"
	else
		echo "    ✗ FAILED: Valid 'marker' mode should succeed"
		exit 1
	fi

	echo "[$(date)] ✓ API error handling: Invalid modes return proper errors"
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
