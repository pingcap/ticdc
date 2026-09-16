#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# test mysql sink only in this case
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_sql "CREATE table test.multi_cdc1(id int primary key, val int);"
	run_sql "CREATE table test.multi_cdc2(id int primary key, val int);"

	# run one cdc cluster
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --cluster-id "test1" --addr "127.0.0.1:8300" --logsuffix mult_cdc.server1
	# run another cdc cluster
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --cluster-id "test11" --addr "127.0.0.1:8301" --logsuffix mult_cdc.server2

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"

	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --server "http://127.0.0.1:8300" --config="$CUR/conf/changefeed1.toml" --changefeed-id "test1"
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --server "http://127.0.0.1:8301" --config="$CUR/conf/changefeed2.toml" --changefeed-id "test2"

	ensure 20 "curl --silent --show-error --fail --user ticdc:ticdc_secret 'http://127.0.0.1:8300/api/v2/changefeeds?keyspace=${KEYSPACE_NAME}' | jq -e '.total == 1 and .items[0].id == \"test1\"' >/dev/null"
	ensure 20 "curl --silent --show-error --fail --user ticdc:ticdc_secret 'http://127.0.0.1:8301/api/v2/changefeeds?keyspace=${KEYSPACE_NAME}' | jq -e '.total == 1 and .items[0].id == \"test2\"' >/dev/null"

	cdc_pid_1=$(get_cdc_pid 127.0.0.1 8300)
	kill_cdc_pid $cdc_pid_1
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --cluster-id "test1" --addr "127.0.0.1:8300" --logsuffix mult_cdc.server1

	ensure 20 "curl --silent --show-error --fail --user ticdc:ticdc_secret 'http://127.0.0.1:8300/api/v2/changefeeds?keyspace=${KEYSPACE_NAME}' | jq -e '.total == 1 and .items[0].id == \"test1\"' >/dev/null"
	ensure 20 "curl --silent --show-error --fail --user ticdc:ticdc_secret 'http://127.0.0.1:8301/api/v2/changefeeds?keyspace=${KEYSPACE_NAME}' | jq -e '.total == 1 and .items[0].id == \"test2\"' >/dev/null"

	# same dml for table multi_cdc1
	run_sql "INSERT INTO test.multi_cdc1(id, val) VALUES (1, 1);"
	run_sql "INSERT INTO test.multi_cdc1(id, val) VALUES (2, 2);"
	run_sql "INSERT INTO test.multi_cdc1(id, val) VALUES (3, 3);"

	# same dml for table multi_cdc2
	run_sql "INSERT INTO test.multi_cdc2(id, val) VALUES (1, 1);"
	run_sql "INSERT INTO test.multi_cdc2(id, val) VALUES (2, 2);"
	run_sql "INSERT INTO test.multi_cdc2(id, val) VALUES (3, 3);"

	check_table_exists "test.multi_cdc1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "test.multi_cdc2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
