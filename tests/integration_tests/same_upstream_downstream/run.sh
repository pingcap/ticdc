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

	UP_SINK_URI="mysql://root@${UP_TIDB_HOST}:${UP_TIDB_PORT}/"
	DOWN_SINK_URI="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"
	UP_PD_ENDPOINTS="${UP_PD_HOST_1}:${UP_PD_PORT_1}"

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
	info_value=$(ETCDCTL_API=3 etcdctl --endpoints="$UP_PD_ENDPOINTS" get "$info_key" --print-value-only)
	new_info_value=$(echo "$info_value" | jq -c --arg uri "$UP_SINK_URI" '.["sink-uri"]=$uri')
	ETCDCTL_API=3 etcdctl --endpoints="$UP_PD_ENDPOINTS" put "$info_key" "$new_info_value"

	result=$(cdc_cli_changefeed resume -c "$changefeed_id" 2>&1 || true)
	if [[ "$result" != *"CDC:ErrSameUpstreamDownstream"* ]] || [[ "$result" != *"resuming a changefeed"* ]]; then
		echo "Expected resume to fail with ErrSameUpstreamDownstream, got:"
		echo "$result"
		exit 1
	fi

	# 4) Create is allowed when the changefeed config sets `allow-same-cluster`, as long as the
	# changefeed cannot capture its own writes. The sink writes into the upstream cluster, but
	# table routing maps the source table into another schema, which is not matched by the filter.
	allow_same_cluster_id="allow-same-cluster"
	src_db="allow_same_cluster_src"
	dst_db="allow_same_cluster_dst"
	dst_table="t1_routed"
	run_sql "create database $src_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "create database $dst_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "create table $src_db.t1 (id int primary key, v varchar(16));" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "create table $dst_db.$dst_table (id int primary key, v varchar(16));" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	result=$(cdc_cli_changefeed create --sink-uri="$UP_SINK_URI" \
		--config="$CUR/conf/allow_same_cluster.toml" -c "$allow_same_cluster_id" 2>&1 || true)
	if [[ "$result" != *"Create changefeed successfully"* ]]; then
		echo "Expected create to be allowed with allow-same-cluster, got:"
		echo "$result"
		exit 1
	fi

	run_sql "insert into $src_db.t1 values (1, 'a'), (2, 'b');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	count=0
	for _ in $(seq 1 60); do
		count=$(mysql -h"$UP_TIDB_HOST" -P"$UP_TIDB_PORT" -uroot -N \
			-e "select count(*) from $dst_db.$dst_table" 2>/dev/null || echo 0)
		if [ "$count" == "2" ]; then
			break
		fi
		sleep 2
	done
	if [ "$count" != "2" ]; then
		echo "Expected 2 rows in $dst_db.$dst_table of the upstream cluster, got: $count"
		exit 1
	fi

	cdc_cli_changefeed remove -c "$allow_same_cluster_id"

	# 5) `allow-same-cluster` is rejected without table routing, and when the route target stays
	# inside the filter range, because the changefeed would then capture the writes of its own sink.
	result=$(cdc_cli_changefeed create --sink-uri="$UP_SINK_URI" \
		--config="$CUR/conf/allow_same_cluster_no_route.toml" -c "allow-same-cluster-no-route" 2>&1 || true)
	if [[ "$result" != *"CDC:ErrInvalidReplicaConfig"* ]] || [[ "$result" != *"requires table routing to be enabled"* ]]; then
		echo "Expected create to be rejected without table routing, got:"
		echo "$result"
		exit 1
	fi

	result=$(cdc_cli_changefeed create --sink-uri="$UP_SINK_URI" \
		--config="$CUR/conf/allow_same_cluster_bad_route.toml" -c "allow-same-cluster-bad-route" 2>&1 || true)
	if [[ "$result" != *"CDC:ErrInvalidReplicaConfig"* ]] || [[ "$result" != *"which the filter replicates"* ]]; then
		echo "Expected create to be rejected when the route target is replicated as well, got:"
		echo "$result"
		exit 1
	fi

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
