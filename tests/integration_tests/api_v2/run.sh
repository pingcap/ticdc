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

	cd $WORK_DIR

	# create table to upstream.
	run_sql "CREATE DATABASE api_v2" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE DATABASE api_v2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY
	# cdc cli changefeed create -c="cf-blackhole" --sink-uri="blackhole://"

	SINK_PARA="{\"changefeed_id\":\"cf-blackhole\", \"sink_uri\":\"blackhole:\/\/\"}"
	# cdc cli changefeed create --sink-uri="$SINK_URI"
    if [ "$IS_NEXT_GEN" = 1 ]; then
        curl -X POST -H "Content-type: appliction/json" "http://$TIKV_WORKER_HOST:$TIKV_WORKER_PORT/cdc/api/v2/changefeeds?keyspace_id=1" -d "$SINK_PARA"
    else
	    curl -X POST -H "Content-type:application/json" "http://$CDC_DEFAULT_HOST:$CDC_DEFAULT_PORT/api/v2/changefeeds" -d "$SINK_PARA"
    fi

	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "cf-blackhole" "normal" "null" ""
}

trap stop_tidb_cluster EXIT
# kafka and storage is not supported yet.
if [ "$SINK_TYPE" == "mysql" ]; then
	prepare $*

	cd "$(dirname "$0")"
	set -euxo pipefail

	GO111MODULE=on go run main.go model.go request.go cases.go

	cleanup_process $CDC_BINARY
	echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
fi
