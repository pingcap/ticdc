#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd $pd_addr
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"
}

trap 'stop_test $WORK_DIR' EXIT

# This workload depends on TiDB TTL table options and is intended for MySQL/TiDB sink.
if [ "$SINK_TYPE" == "mysql" ]; then
	prepare $*

	cd "$CUR"
	set -euxo pipefail

	GO111MODULE=on go run main.go \
		-dsn "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/?charset=utf8mb4&parseTime=true&interpolateParams=true&multiStatements=true" \
		-workers "${BIS_WORKERS:-32}" \
		-operations "${BIS_OPERATIONS:-80000}" \
		-seed-rows "${BIS_SEED_ROWS:-20000}" \
		-metadata-bytes "${BIS_METADATA_BYTES:-1024}" \
		2>&1 | tee $WORK_DIR/bis_batch_metadata_ttl.log

	check_table_exists bulk_ingestion.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/diff_config.toml 200 3

	cleanup_process $CDC_BINARY
	echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
fi
