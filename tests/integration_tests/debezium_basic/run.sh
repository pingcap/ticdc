#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# use kafka-consumer with debezium decoder to sync data from kafka to mysql
function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	# clean up environment
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	# start tidb cluster
	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	TOPIC_NAME="ticdc-debezium-basic-$RANDOM"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=debezium&enable-tidb-extension=true"

	run_cdc_cli changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml
	sleep 5 # wait for changefeed to start
	# determine the sink uri and run corresponding consumer
	run_kafka_consumer $WORK_DIR $SINK_URI $CUR/conf/changefeed.toml

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE test.finish_mark1 (a int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.finish_mark1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	run_sql_file $CUR/data/data_gbk.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE test.finish_mark2 (a int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.finish_mark2 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	echo "Starting build checksum checker..."
	cd $CUR/../../utils/checksum_checker
	if [ ! -f ./checksum_checker ]; then
		GO111MODULE=on go build
	fi
	# wait the dml event replicate the downstream
	sleep 10
	run_sql "CREATE TABLE test.finish_mark3 (a int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.finish_mark3 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	./checksum_checker --upstream-uri "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/" --downstream-uri "root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/" --databases "test" --config="$CUR/conf/changefeed.toml"

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
