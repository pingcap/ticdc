#!/bin/bash
#
# Verify TiCDC rejects using the same TiDB cluster as both upstream and downstream.

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	# Only meaningful for MySQL/TiDB sink.
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	UP_SINK_URI="mysql://root@127.0.0.1:4000/"
	DOWN_SINK_URI="mysql://root@127.0.0.1:3306/"

	# 1) Create should be rejected when sink points to upstream cluster.
	result=$(cdc_cli_changefeed create --sink-uri="$UP_SINK_URI" -c "same-up-down-create" 2>&1 || true)
	if [[ "$result" != *"CDC:ErrSameUpstreamDownstream"* ]] || [[ "$result" != *"creating a changefeed"* ]]; then
		echo "Expected create to fail with ErrSameUpstreamDownstream, got:"
		echo "$result"
		exit 1
	fi

	# 2) Update should be rejected when updating sink to upstream cluster.
	changefeed_id="same-up-down"
	cdc_cli_changefeed create --sink-uri="$DOWN_SINK_URI" -c "$changefeed_id"
	cdc_cli_changefeed pause -c "$changefeed_id"

	result=$(cdc_cli_changefeed update -c "$changefeed_id" --sink-uri="$UP_SINK_URI" --no-confirm 2>&1 || true)
	if [[ "$result" != *"CDC:ErrSameUpstreamDownstream"* ]] || [[ "$result" != *"updating a changefeed"* ]]; then
		echo "Expected update to fail with ErrSameUpstreamDownstream, got:"
		echo "$result"
		exit 1
	fi

	# 3) Resume should be rejected even if the sink URI is modified via etcd directly (e.g. legacy metadata).
	info_key="/tidb/cdc/default/$KEYSPACE_NAME/changefeed/info/$changefeed_id"
	info_value=$(ETCDCTL_API=3 etcdctl get "$info_key" --print-value-only)
	new_info_value=$(echo "$info_value" | jq -c --arg uri "$UP_SINK_URI" '.["sink-uri"]=$uri')
	ETCDCTL_API=3 etcdctl put "$info_key" "$new_info_value"

	result=$(cdc_cli_changefeed resume -c "$changefeed_id" 2>&1 || true)
	if [[ "$result" != *"CDC:ErrSameUpstreamDownstream"* ]] || [[ "$result" != *"resuming a changefeed"* ]]; then
		echo "Expected resume to fail with ErrSameUpstreamDownstream, got:"
		echo "$result"
		exit 1
	fi

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"

