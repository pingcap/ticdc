#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
CHANGEFEED_ID="test"

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

function run() {
	if [ "$SINK_TYPE" != "storage" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR

	# Record TSO before creating test tables to skip system table DDLs.
	start_ts=$(run_cdc_cli_tso_query $UP_PD_HOST_1 $UP_PD_PORT_1)

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="file://$WORK_DIR/storage_test?flush-interval=5s&protocol=csv"
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c $CHANGEFEED_ID --config="$CUR/conf/changefeed.toml"

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_storage_consumer $WORK_DIR $SINK_URI $CUR/conf/changefeed.toml ""
	run_checksum_checker

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
