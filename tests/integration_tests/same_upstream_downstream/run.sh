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

	wait_for_rows() {
		local expected=$1 table=$2 count=0
		for _ in $(seq 1 60); do
			count=$(mysql -h"$UP_TIDB_HOST" -P"$UP_TIDB_PORT" -uroot -N \
				-e "select count(*) from $table" 2>/dev/null || echo 0)
			if [ "$count" == "$expected" ]; then
				return 0
			fi
			sleep 2
		done
		echo "Expected $expected rows in $table of the upstream cluster, got: $count"
		return 1
	}

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

	wait_for_rows 2 "$dst_db.$dst_table"

	cdc_cli_changefeed remove -c "$allow_same_cluster_id"

	# 5) The target schema may also follow the source schema: `allow_same_cluster_src2_routed` is not
	# replicated by the filter, so the configuration is accepted and stays safe for tables created
	# later.
	derived_id="allow-same-cluster-derived"
	src_db2="allow_same_cluster_src2"
	dst_db2="allow_same_cluster_src2_routed"
	run_sql "create database $src_db2;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "create database $dst_db2;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	result=$(cdc_cli_changefeed create --sink-uri="$UP_SINK_URI" \
		--config="$CUR/conf/allow_same_cluster_derived.toml" -c "$derived_id" 2>&1 || true)
	if [[ "$result" != *"Create changefeed successfully"* ]]; then
		echo "Expected create to be allowed when the target schema follows the source schema, got:"
		echo "$result"
		exit 1
	fi

	# The table is created after the changefeed: its DDL is routed to the derived schema, and the
	# rows must land there instead of being captured again.
	run_sql "create table $src_db2.t1 (id int primary key, v varchar(16));" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists "$dst_db2.t1_routed" "$UP_TIDB_HOST" "$UP_TIDB_PORT" 90
	run_sql "insert into $src_db2.t1 values (1, 'a'), (2, 'b');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	wait_for_rows 2 "$dst_db2.t1_routed"

	# The gate also runs when a changefeed is updated.
	cdc_cli_changefeed pause -c "$derived_id"
	result=$(cdc_cli_changefeed update -c "$derived_id" \
		--config="$CUR/conf/allow_same_cluster_bad_route.toml" --no-confirm 2>&1 || true)
	if [[ "$result" != *"CDC:ErrInvalidReplicaConfig"* ]] || [[ "$result" != *"which the filter replicates"* ]]; then
		echo "Expected update to be rejected when the route target is replicated as well, got:"
		echo "$result"
		exit 1
	fi

	cdc_cli_changefeed remove -c "$derived_id"

	# 6) `allow-same-cluster` is rejected whenever the configuration cannot be proven safe: without
	# table routing, when a filter rule is not covered by any matcher, when the route target stays
	# inside the filter range, and for rule forms the gate does not support.
	result=$(cdc_cli_changefeed create --sink-uri="$UP_SINK_URI" \
		--config="$CUR/conf/allow_same_cluster_no_route.toml" -c "allow-same-cluster-no-route" 2>&1 || true)
	if [[ "$result" != *"CDC:ErrInvalidReplicaConfig"* ]] || [[ "$result" != *"requires table routing to be enabled"* ]]; then
		echo "Expected create to be rejected without table routing, got:"
		echo "$result"
		exit 1
	fi

	result=$(cdc_cli_changefeed create --sink-uri="$UP_SINK_URI" \
		--config="$CUR/conf/allow_same_cluster_narrow_route.toml" -c "allow-same-cluster-narrow-route" 2>&1 || true)
	if [[ "$result" != *"CDC:ErrInvalidReplicaConfig"* ]] || [[ "$result" != *"is not covered by any dispatch rule matcher"* ]]; then
		echo "Expected create to be rejected when the matcher is narrower than the filter rule, got:"
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

	result=$(cdc_cli_changefeed create --sink-uri="$UP_SINK_URI" \
		--config="$CUR/conf/allow_same_cluster_unsupported.toml" -c "allow-same-cluster-unsupported" 2>&1 || true)
	if [[ "$result" != *"CDC:ErrInvalidReplicaConfig"* ]] || [[ "$result" != *"does not support the filter rule"* ]]; then
		echo "Expected create to be rejected for a filter rule the gate does not support, got:"
		echo "$result"
		exit 1
	fi

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
