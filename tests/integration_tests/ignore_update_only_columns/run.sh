#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function check_result() {
	run_sql "SELECT version, updated_at FROM ignore_update_only_columns.user_a WHERE id = 1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "version: 1"
	check_contains "updated_at: 10"

	run_sql "SELECT version, updated_at FROM ignore_update_only_columns.user_b WHERE id = 1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "version: 1"
	check_contains "updated_at: 10"

	run_sql "SELECT name, version FROM ignore_update_only_columns.user_a WHERE id = 2;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "name: Bob updated"
	check_contains "version: 2"

	run_sql "select count(1) from ignore_update_only_columns.user_a where id = 3;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 0"

	run_sql "SELECT version FROM ignore_update_only_columns.user_a WHERE id = 30;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "version: 2"

	run_sql "SELECT email, version FROM ignore_update_only_columns.user_a WHERE id = 4;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "email: d-updated@example.com"
	check_contains "version: 2"

	run_sql "select count(1) from ignore_update_only_columns.user_a where id = 5;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 0"

	run_sql "select count(1) from ignore_update_only_columns.user_a where id = 6;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 1"
}

function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-ignore-update-only-columns-$RANDOM"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760"
	CONSUMER_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760"

	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --server="127.0.0.1:8300" --config=$CUR/conf/changefeed.toml
	run_kafka_consumer $WORK_DIR "$CONSUMER_URI" "$CUR/conf/changefeed.toml" "" ""

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_table_exists "ignore_update_only_columns.finish_mark" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_result

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
