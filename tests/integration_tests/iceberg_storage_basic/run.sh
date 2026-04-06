#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function check_iceberg_artifacts() {
	local metadata_dir=$1
	local data_dir=$2

	ensure 60 test -d "$metadata_dir"
	ensure 60 test -d "$data_dir"
	ensure 60 "ls \"$metadata_dir\"/v*.metadata.json >/dev/null 2>&1"
	ensure 60 "ls \"$data_dir\"/snap-*.parquet >/dev/null 2>&1"

	latest_metadata=$(ls "$metadata_dir"/v*.metadata.json | sort -V | tail -n 1)
	grep -q '"format-version":[[:space:]]*2' "$latest_metadata"
	grep -q '"tidb.committed_resolved_ts"' "$latest_metadata"
}

function run() {
	if [ "$SINK_TYPE" != "storage" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# Use an unpartitioned table layout to make artifact checks deterministic.
	SINK_URI="file://$WORK_DIR/storage_test?protocol=iceberg&namespace=ns&commit-interval=2s&partitioning="
	cdc_cli_changefeed create --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml

	run_sql_file $CUR/data/schema.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_iceberg_artifacts \
		"$WORK_DIR/storage_test/ns/test/t_iceberg/metadata" \
		"$WORK_DIR/storage_test/ns/test/t_iceberg/data"

	run_storage_consumer $WORK_DIR $SINK_URI $CUR/conf/changefeed.toml ""
	sleep 8
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 100
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
