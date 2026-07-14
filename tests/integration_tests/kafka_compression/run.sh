#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function test_compression() {
	local compression=$1

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	TOPIC_NAME="ticdc-kafka-compression-$compression-test-$RANDOM"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true&kafka-version=${KAFKA_VERSION}&compression=$compression"
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $compression
	run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=canal-json&version=${KAFKA_VERSION}&enable-tidb-extension=true"

	run_sql "CREATE TABLE test.tp_int_$compression (
		id INT AUTO_INCREMENT,
		c_tinyint TINYINT NULL,
		c_smallint SMALLINT NULL,
		c_mediumint MEDIUMINT NULL,
		c_int INT NULL,
		c_bigint BIGINT NULL,
		PRIMARY KEY (id)
	);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO test.tp_int_$compression(c_tinyint, c_smallint, c_mediumint, c_int, c_bigint)
		VALUES (1, 2, 3, 4, 5);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE test.${compression}_finish_mark (id INT PRIMARY KEY);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_table_exists test.${compression}_finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	cdc_cli_changefeed pause -c $compression
	cdc_cli_changefeed remove -c $compression
}

function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	test_compression gzip
	test_compression snappy
	test_compression lz4
	test_compression zstd

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
