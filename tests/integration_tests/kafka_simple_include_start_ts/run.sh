#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
pd_addr="http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"

assert_probe_start_ts() {
	local dump_file=$1
	local expected_ts=$2
	local expect_field=$3
	python3 - "$dump_file" "$expected_ts" "$expect_field" <<'PY'
import json, sys

path, expected, expect_field = sys.argv[1], sys.argv[2], sys.argv[3]
want = expect_field == "true"
expected_ts = int(expected)
found = 0
for line in open(path):
    line = line.strip()
    if not line:
        continue
    try:
        msg = json.loads(line)
    except json.JSONDecodeError:
        continue
    if msg.get("table") != "cdc_start_ts_probe" or msg.get("type") != "INSERT":
        continue
    found += 1
    start_ts = msg.get("startTs")
    if want:
        if not isinstance(start_ts, int):
            raise SystemExit("startTs missing or not an integer: %r" % (start_ts,))
        if start_ts != expected_ts:
            raise SystemExit("startTs %s != expected %s" % (start_ts, expected_ts))
        row_ts = (msg.get("data") or {}).get("expected_start_ts")
        if str(row_ts) != str(expected_ts):
            raise SystemExit("row expected_start_ts %s != %s" % (row_ts, expected_ts))
    elif start_ts is not None:
        raise SystemExit("startTs must be omitted, got %r" % (start_ts,))
if found < 2:
    raise SystemExit("found %d probe INSERT messages, want 2" % found)
print("probe ok: %d INSERT messages, startTs present=%s ts=%s" % (found, want, expected))
PY
}

insert_probe_rows() {
	mysql -uroot -h${UP_TIDB_HOST} -P${UP_TIDB_PORT} --default-character-set utf8mb4 -N -e "
		CREATE DATABASE IF NOT EXISTS test;
		CREATE TABLE IF NOT EXISTS test.cdc_start_ts_probe (
			id BIGINT PRIMARY KEY,
			expected_start_ts BIGINT UNSIGNED NOT NULL
		);
		TRUNCATE TABLE test.cdc_start_ts_probe;
		START TRANSACTION;
		SELECT @@tidb_current_ts INTO @ts;
		INSERT INTO test.cdc_start_ts_probe VALUES (1, @ts), (2, @ts);
		COMMIT;
		SELECT @ts;
	"
}

run_mode() {
	local mode=$1
	local sink_uri=$2
	local config_args=$3
	local expect_field=$4
	local changefeed_id="simple-start-ts-${mode}"
	local dump_file="$WORK_DIR/dump-${mode}.jsonl"

	echo "===== $mode ====="
	cdc_cli_changefeed create --sink-uri="$sink_uri" $config_args -c "$changefeed_id"
	ensure 20 check_changefeed_state "$pd_addr" "$changefeed_id" "normal" "null" ""
	sleep 5

	local ts
	ts=$(insert_probe_rows | tail -n 1 | tr -d '[:space:]')
	if [[ ! "$ts" =~ ^[0-9]+$ ]]; then
		echo "failed to read transaction TSO, got: $ts"
		exit 1
	fi
	echo "probe transaction start ts: $ts"

	kafka_dump --topic "$TOPIC_NAME" --timeout 90s --until-table cdc_start_ts_probe --until-count 2 >"$dump_file"
	assert_probe_start_ts "$dump_file" "$ts" "$expect_field"

	cdc_cli_changefeed remove -c "$changefeed_id"
}

function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-simple-include-start-ts-$RANDOM"

	run_mode "uri" \
		"kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=simple&partition-num=1&simple-include-start-ts=true" \
		"" \
		"true"

	TOPIC_NAME="ticdc-simple-include-start-ts-toml-$RANDOM"
	run_mode "toml" \
		"kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=simple&partition-num=1" \
		"--config=$CUR/conf/changefeed.toml" \
		"true"

	TOPIC_NAME="ticdc-simple-include-start-ts-off-$RANDOM"
	run_mode "off" \
		"kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=simple&partition-num=1" \
		"" \
		"false"

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
