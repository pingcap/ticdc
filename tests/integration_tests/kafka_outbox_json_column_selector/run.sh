#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=20

function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# Create the outbox table before starting any changefeed so that it appears
	# in the schema snapshot used by getVerifiedTables during API-side validation.
	run_sql_file $CUR/data/ddl.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# --- Reject case ----------------------------------------------------------
	# The column selector excludes `payload`, which is the configured
	# value-column. getVerifiedTables must surface ErrColumnSelectorFailed and
	# the CLI must return a non-zero exit code.
	TOPIC_REJECT="outbox-cs-reject-$RANDOM"
	SINK_URI_REJECT="kafka://127.0.0.1:9092/$TOPIC_REJECT?protocol=outbox-json"

	set +e
	reject_output=$(cdc_cli_changefeed create \
		--sink-uri="$SINK_URI_REJECT" \
		--config="$CUR/conf/changefeed_reject.toml" \
		-c "outbox-cs-reject" 2>&1)
	reject_exit=$?
	set -e

	if [ $reject_exit -eq 0 ]; then
		echo "FAIL: changefeed create with reject config should have returned non-zero"
		echo "output: $reject_output"
		exit 1
	fi
	echo "$reject_output" | grep -q "ErrColumnSelectorFailed" || {
		echo "FAIL: expected ErrColumnSelectorFailed in error output, got:"
		echo "$reject_output"
		exit 1
	}
	echo "reject changefeed correctly refused at creation time"

	# --- OK case --------------------------------------------------------------
	# The column selector excludes only `status`, which is not required by the
	# outbox config. Changefeed creation must succeed and reach "normal" state.
	TOPIC_OK="outbox-cs-ok-$RANDOM"
	SINK_URI_OK="kafka://127.0.0.1:9092/$TOPIC_OK?protocol=outbox-json"

	cdc_cli_changefeed create \
		--sink-uri="$SINK_URI_OK" \
		--config="$CUR/conf/changefeed_ok.toml" \
		-c "outbox-cs-ok"

	ensure $MAX_RETRIES check_changefeed_state \
		http://${UP_PD_HOST_1}:${UP_PD_PORT_1} "outbox-cs-ok" "normal" "null" ""

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
