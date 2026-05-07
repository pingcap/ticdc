#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# This test only exercises CLI output format; no downstream sink needed.
if [ "$SINK_TYPE" != "mysql" ]; then
	echo "[$(date)] <<<<<< skip $TEST_NAME for sink type $SINK_TYPE (only needs mysql/blackhole) >>>>>>"
	exit 0
fi

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# Build Go validator
	cd $CUR && GO111MODULE=on go build -o $WORK_DIR/toml_validator . && cd -
	VALIDATOR="$WORK_DIR/toml_validator"

	# --- Create changefeeds ---
	cdc_cli_changefeed create --sink-uri="blackhole://" -c "cf-default"
	cdc_cli_changefeed create --sink-uri="blackhole://" -c "cf-overrides" \
		--config="$CUR/conf/overrides.toml"
	cdc_cli_changefeed create --sink-uri="blackhole://" -c "cf-large-filter" \
		--config="$CUR/conf/large_filter.toml"
	cdc_cli_changefeed create \
		--sink-uri="blackhole://root:secret@10.189.5.160:4000?safe-mode=true" \
		-c "cf-realistic" --config="$CUR/conf/realistic.toml"

	# Wait for changefeeds to be running
	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "cf-default" "normal" "null" ""
	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "cf-overrides" "normal" "null" ""
	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "cf-large-filter" "normal" "null" ""
	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "cf-realistic" "normal" "null" ""

	# --- Capture outputs ---
	cdc_cli_changefeed query -c "cf-default" | grep -v "Command to ticdc" >"$WORK_DIR/default.json"
	cdc_cli_changefeed query -c "cf-default" --output toml | grep -v "Command to ticdc" >"$WORK_DIR/default.toml"
	cdc_cli_changefeed query -c "cf-default" -o toml | grep -v "Command to ticdc" >"$WORK_DIR/default_short.toml"
	cdc_cli_changefeed query -c "cf-overrides" | grep -v "Command to ticdc" >"$WORK_DIR/overrides.json"
	cdc_cli_changefeed query -c "cf-overrides" --output toml | grep -v "Command to ticdc" >"$WORK_DIR/overrides.toml"
	cdc_cli_changefeed query -c "cf-large-filter" | grep -v "Command to ticdc" >"$WORK_DIR/large_filter.json"
	cdc_cli_changefeed query -c "cf-large-filter" --output toml | grep -v "Command to ticdc" >"$WORK_DIR/large_filter.toml"
	cdc_cli_changefeed query -c "cf-default" --simple --output toml | grep -v "Command to ticdc" >"$WORK_DIR/simple.toml"
	cdc_cli_changefeed query -c "cf-realistic" | grep -v "Command to ticdc" >"$WORK_DIR/realistic.json"
	cdc_cli_changefeed query -c "cf-realistic" --output toml | grep -v "Command to ticdc" >"$WORK_DIR/realistic.toml"

	# --- Test 1: JSON output valid ---
	jq -e '.id == "cf-default"' "$WORK_DIR/default.json" >/dev/null
	jq -e '.config != null' "$WORK_DIR/default.json" >/dev/null
	jq -e '.sink_uri == "blackhole:"' "$WORK_DIR/default.json" >/dev/null
	echo "PASS: Test 1 - JSON output valid"

	# --- Test 2: TOML output valid & parseable ---
	$VALIDATOR validate_toml "$WORK_DIR/default.toml"
	echo "PASS: Test 2 - TOML output valid"

	# --- Test 3: Kebab-case keys, no snake_case ---
	$VALIDATOR check_kebab_keys "$WORK_DIR/default.toml"
	echo "PASS: Test 3 - Kebab-case keys verified"

	# --- Test 4: Section structure ---
	grep -q "\[config\]" "$WORK_DIR/default.toml"
	grep -q "\[config\.filter\]" "$WORK_DIR/default.toml"
	grep -q "\[config\.mounter\]" "$WORK_DIR/default.toml"
	grep -q "\[config\.sink\]" "$WORK_DIR/default.toml"
	echo "PASS: Test 4 - TOML section structure correct"

	# --- Test 5: Duration formatting ---
	grep -q "sync-point-interval = '10m0s'" "$WORK_DIR/default.toml"
	if grep -qE "600000000000|86400000000000" "$WORK_DIR/default.toml"; then
		echo "FAIL: Test 5 - raw nanosecond values found in TOML"
		exit 1
	fi
	echo "PASS: Test 5 - Durations are human-readable"

	# --- Test 6: Timestamp formatting ---
	if grep -qE "create-time = '[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}'" "$WORK_DIR/default.toml"; then
		echo "PASS: Test 6 - Timestamp is human-readable"
	else
		echo "FAIL: Test 6 - create-time not in expected format"
		grep "create-time" "$WORK_DIR/default.toml" || true
		exit 1
	fi

	# --- Test 7: JSON/TOML correspondence ---
	$VALIDATOR compare_json_toml "$WORK_DIR/default.json" "$WORK_DIR/default.toml"
	echo "PASS: Test 7 - JSON/TOML field correspondence"

	# --- Test 8: Overrides config values ---
	$VALIDATOR check_overrides "$WORK_DIR/overrides.toml"
	echo "PASS: Test 8 - Overrides config values correct"

	# --- Test 9: Overrides JSON/TOML match ---
	$VALIDATOR compare_json_toml "$WORK_DIR/overrides.json" "$WORK_DIR/overrides.toml"
	echo "PASS: Test 9 - Overrides JSON/TOML match"

	# --- Test 10: Large filter rules ---
	$VALIDATOR check_filter_rules "$WORK_DIR/large_filter.toml" 12
	echo "PASS: Test 10 - All 12 filter rules present"

	# --- Test 11: Simplified query - no config section ---
	if grep -q "\[config\]" "$WORK_DIR/simple.toml"; then
		echo "FAIL: Test 11 - simplified output should not have [config] section"
		exit 1
	fi
	echo "PASS: Test 11 - Simplified query has no config"

	# --- Test 12: Simplified query - has status fields ---
	grep -q "checkpoint-tso" "$WORK_DIR/simple.toml"
	grep -q "upstream-id" "$WORK_DIR/simple.toml"
	echo "PASS: Test 12 - Simplified query has status fields"

	# --- Test 13: Invalid format error ---
	set +e
	bad_output=$(cdc_cli_changefeed query -c "cf-default" --output yaml 2>&1)
	rc=$?
	set -e
	if [ "$rc" -ne 0 ] && echo "$bad_output" | grep -qi "invalid\|unknown\|unsupported"; then
		echo "PASS: Test 13 - Invalid format produces error"
	else
		echo "FAIL: Test 13 - expected non-zero exit with error message, rc=$rc, output: $bad_output"
		exit 1
	fi

	# --- Test 14: Default output is JSON ---
	jq -e '.id == "cf-default"' "$WORK_DIR/default.json" >/dev/null
	echo "PASS: Test 14 - Default output is valid JSON"

	# --- Test 15: -o toml short flag ---
	$VALIDATOR validate_toml "$WORK_DIR/default_short.toml"
	echo "PASS: Test 15 - Short flag -o toml works"

	# --- Test 16: Single-quote string values ---
	grep -q "id = 'cf-default'" "$WORK_DIR/default.toml"
	grep -q "sink-uri = 'blackhole:'" "$WORK_DIR/default.toml"
	echo "PASS: Test 16 - Single-quote string values"

	# --- Test 17: Overrides JSON validation ---
	jq -e '.config.case_sensitive == true' "$WORK_DIR/overrides.json" >/dev/null
	jq -e '.config.mounter.worker_num == 8' "$WORK_DIR/overrides.json" >/dev/null
	echo "PASS: Test 17 - Overrides JSON output correct"

	# --- Test 18: Password redaction in JSON ---
	if jq -e '.sink_uri | contains("secret")' "$WORK_DIR/realistic.json" >/dev/null 2>&1; then
		echo "FAIL: Test 18 - password not redacted in JSON"
		exit 1
	fi
	jq -e '.sink_uri | contains("xxxxx")' "$WORK_DIR/realistic.json" >/dev/null
	echo "PASS: Test 18 - Password redacted in JSON"

	# --- Test 19: Password redaction in TOML ---
	if grep -q "secret" "$WORK_DIR/realistic.toml"; then
		echo "FAIL: Test 19 - password not redacted in TOML"
		exit 1
	fi
	grep -q "xxxxx" "$WORK_DIR/realistic.toml"
	echo "PASS: Test 19 - Password redacted in TOML"

	# --- Test 20: Realistic config values in TOML ---
	$VALIDATOR check_realistic "$WORK_DIR/realistic.toml"
	echo "PASS: Test 20 - Realistic config values correct"

	# --- Test 21: Realistic JSON/TOML match ---
	$VALIDATOR compare_json_toml "$WORK_DIR/realistic.json" "$WORK_DIR/realistic.toml"
	echo "PASS: Test 21 - Realistic JSON/TOML match"

	# --- Test 22: Default single-element array format ---
	$VALIDATOR check_default_array_format "$WORK_DIR/default.toml"
	echo "PASS: Test 22 - Default single-element array multi-line format"

	# --- Test 23: Config fields 2-space indented ---
	$VALIDATOR check_indentation "$WORK_DIR/default.toml" config_fields
	echo "PASS: Test 23 - Config fields indented"

	# --- Test 24: Nested section headers indented ---
	$VALIDATOR check_indentation "$WORK_DIR/default.toml" nested_headers
	echo "PASS: Test 24 - Nested section headers indented"

	# --- Test 25: Nested section fields indented ---
	$VALIDATOR check_indentation "$WORK_DIR/default.toml" nested_fields
	echo "PASS: Test 25 - Nested section fields indented"

	# --- Test 26: Large filter rules present ---
	jq -e '.config.filter.rules | length == 12' "$WORK_DIR/large_filter.json" >/dev/null
	echo "PASS: Test 26 - Large filter JSON valid"

	# --- Test 27: Large filter array block format ---
	$VALIDATOR check_array_format "$WORK_DIR/large_filter.toml"
	echo "PASS: Test 27 - Large filter array block format correct"

	# --- Test 28: Large filter JSON/TOML match ---
	$VALIDATOR compare_json_toml "$WORK_DIR/large_filter.json" "$WORK_DIR/large_filter.toml"
	echo "PASS: Test 28 - Large filter JSON/TOML match"

	# Cleanup changefeeds
	cdc_cli_changefeed --changefeed-id "cf-default" remove
	cdc_cli_changefeed --changefeed-id "cf-overrides" remove
	cdc_cli_changefeed --changefeed-id "cf-large-filter" remove
	cdc_cli_changefeed --changefeed-id "cf-realistic" remove

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run "$@"
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
