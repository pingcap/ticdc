#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
CHANGEFEED_ID="test"

function prepare_cluster() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR

	# Record TSO before creating test tables to skip system table DDLs.
	start_ts=$(run_cdc_cli_tso_query $UP_PD_HOST_1 $UP_PD_PORT_1)

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
}

function build_checksum_checker() {
	echo "Starting build checksum checker..."
	local pwd=$(pwd)
	cd $CUR/../../utils/checksum_checker
	if [ ! -f ./checksum_checker ]; then
		GO111MODULE=on go build
	fi
	cd $pwd
}

function run_checksum_checker() {
	build_checksum_checker
	check_table_exists "test1.finishmark" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	$CUR/../../utils/checksum_checker/checksum_checker \
		--upstream-uri "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/" \
		--downstream-uri "root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/" \
		--databases "test,test1" \
		--config="$CUR/conf/changefeed.toml"
}

function run_kafka() {
	prepare_cluster

	TOPIC_NAME="column-selector-test-$RANDOM"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=canal-json&partition-num=1&enable-tidb-extension=true"
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $CHANGEFEED_ID --config="$CUR/conf/changefeed.toml"

	cdc_kafka_consumer --upstream-uri $SINK_URI --downstream-uri="mysql://root@127.0.0.1:3306/?safe-mode=true&batch-dml-enable=false&enable-ddl-ts=false" --upstream-tidb-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/?" --config="$CUR/conf/changefeed.toml" --log-file $WORK_DIR/cdc_kafka_consumer.log 2>&1 &

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_checksum_checker

	cleanup_process $CDC_BINARY
}

function run_storage() {
	prepare_cluster

	SINK_URI="file://$WORK_DIR/storage_test?flush-interval=5s&protocol=canal-json&enable-tidb-extension=true"
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $CHANGEFEED_ID --config="$CUR/conf/changefeed.toml"

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_storage_consumer $WORK_DIR $SINK_URI $CUR/conf/changefeed.toml ""
	run_checksum_checker
}

function run() {
	case "$SINK_TYPE" in
	kafka)
		run_kafka
		;;
	storage)
		run_storage
		;;
	*)
		return
		;;
	esac
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
