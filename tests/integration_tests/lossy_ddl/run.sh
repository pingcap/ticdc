#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# Check that every inserted row reached the sink and that no lossy DDL
# generated an extra row event.
function check_lossy_ddl() {
	# The checkpoint advances only after the sink flushes an event, so reaching
	# the TSO taken after prepare.sql also proves the DDLs were replicated.
	checkpoint=$(cdc_cli_changefeed query -c lossy_ddl 2>&1 |
		grep -v "Command to ticdc" | jq -r '.checkpoint_tso // 0')
	if [[ "$checkpoint" -lt "$finish_ts" ]]; then
		echo "checkpoint $checkpoint has not reached $finish_ts"
		return 1
	fi

	# Read the row count only after the checkpoint passed, so a late extra row
	# cannot slip in between the two checks.
	rows=$(curl -s http://127.0.0.1:8300/metrics |
		awk '/^ticdc_sink_batch_row_count_sum\{/ {sum += $2} END {printf "%d", sum}')
	if [[ "$rows" -ne 23 ]]; then
		echo "expect 23 rows in blackhole sink, got $rows"
		return 1
	fi
}

export -f check_lossy_ddl

function run() {
	# next-gen mode doesn't support lossy ddl
	if [ "$NEXT_GEN" = 1 ]; then
		return
	fi
	# Use blackhole sink to check if the DDL is lossy.
	# So no need to run this test for other sinks.
	if [ "$SINK_TYPE" != "storage" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	SINK_URI="blackhole://"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	cdc_cli_changefeed create --pd=$pd_addr --sink-uri="$SINK_URI" -c lossy_ddl

	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	finish_ts=$(run_cdc_cli_tso_query $UP_PD_HOST_1 $UP_PD_PORT_1)
	export finish_ts

	ensure 50 check_lossy_ddl
	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
