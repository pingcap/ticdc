#!/bin/bash
#
# Verify same-cluster replication requires database isolation.

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
	UP_PD_ENDPOINT="http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
	DOWN_SINK_URI="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"

	# 1) Create should be rejected when sink points to upstream cluster.
	if result=$(cdc_cli_changefeed create --sink-uri="$UP_SINK_URI" -c "same-up-down-create" 2>&1); then
		echo "Expected changefeed operation to be rejected, got: $result"
		exit 1
	fi
	if [[ "$result" != *"CDC:ErrSameUpstreamDownstream"* ]] || [[ "$result" != *"creating a changefeed"* ]]; then
		echo "Expected create to fail with ErrSameUpstreamDownstream, got:"
		echo "$result"
		exit 1
	fi

	# 2) Update should be rejected when updating sink to upstream cluster.
	changefeed_id="same-up-down"
	cdc_cli_changefeed create --sink-uri="$DOWN_SINK_URI" -c "$changefeed_id"
	cdc_cli_changefeed pause -c "$changefeed_id"
	ensure 30 check_changefeed_state "$UP_PD_ENDPOINT" "$changefeed_id" "stopped" "null" ""

	if result=$(cdc_cli_changefeed update -c "$changefeed_id" --sink-uri="$UP_SINK_URI" --no-confirm 2>&1); then
		echo "Expected changefeed operation to be rejected, got: $result"
		exit 1
	fi
	if [[ "$result" != *"CDC:ErrSameUpstreamDownstream"* ]] || [[ "$result" != *"updating a changefeed"* ]]; then
		echo "Expected update to fail with ErrSameUpstreamDownstream, got:"
		echo "$result"
		exit 1
	fi

	cdc_cli_changefeed remove -c "$changefeed_id"

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

	# 3) Create is allowed when the changefeed config sets `allow-same-cluster`, as long as the
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
		--config="$CUR/conf/allow_same_cluster.toml" -c "$allow_same_cluster_id" 2>&1)
	if [[ "$result" != *"Create changefeed successfully"* ]]; then
		echo "Expected create to be allowed with allow-same-cluster, got:"
		echo "$result"
		exit 1
	fi

	run_sql "insert into $src_db.t1 values (1, 'a'), (2, 'b');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	wait_for_rows 2 "$dst_db.$dst_table"
	ensure 30 check_changefeed_state "$UP_PD_ENDPOINT" "$allow_same_cluster_id" "normal" "null" ""

	# Keep data in another source database while rejecting routes that would endanger it.
	run_sql "create database allow_same_cluster_src_copy; create table allow_same_cluster_src_copy.t2 (id int primary key); insert into allow_same_cluster_src_copy.t2 values (1);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	# Create and update must reject database overlap and ambiguous schema routing.
	cdc_cli_changefeed pause -c "$allow_same_cluster_id"
	ensure 30 check_changefeed_state "$UP_PD_ENDPOINT" "$allow_same_cluster_id" "stopped" "null" ""
	original_config=$(cdc_cli_changefeed query -c "$allow_same_cluster_id" | sed '/^Command to ticdc/d' | jq -eS '.config')
	for unsafe_case in same_schema source_schema_chain all_schemas schema_ambiguity no_route narrow_route unsupported; do
		expected_error="which the filter replicates"
		case "$unsafe_case" in
		schema_ambiguity) expected_error="different target-schema expressions" ;;
		no_route) expected_error="requires table routing to be enabled" ;;
		narrow_route) expected_error="is not covered by any dispatch rule matcher" ;;
		unsupported) expected_error="does not support the filter rule" ;;
		esac
		unsafe_config="$CUR/conf/allow_same_cluster_${unsafe_case}.toml"
		if result=$(cdc_cli_changefeed create --sink-uri="$UP_SINK_URI" \
			--config="$unsafe_config" -c "reject-${unsafe_case//_/-}" 2>&1); then
			echo "Unexpected create success for $unsafe_case: $result"
			exit 1
		fi
		if [[ "$result" != *"CDC:ErrInvalidReplicaConfig"* ]] || [[ "$result" != *"$expected_error"* ]]; then
			echo "Expected create to reject $unsafe_case, got: $result"
			exit 1
		fi
		if result=$(cdc_cli_changefeed update -c "$allow_same_cluster_id" \
			--config="$unsafe_config" --no-confirm 2>&1); then
			echo "Unexpected update success for $unsafe_case: $result"
			exit 1
		fi
		if [[ "$result" != *"CDC:ErrInvalidReplicaConfig"* ]] || [[ "$result" != *"$expected_error"* ]]; then
			echo "Expected update to reject $unsafe_case, got: $result"
			exit 1
		fi
		current_config=$(cdc_cli_changefeed query -c "$allow_same_cluster_id" | sed '/^Command to ticdc/d' | jq -eS '.config')
		if [ "$current_config" != "$original_config" ]; then
			echo "Rejected update changed the configuration for $unsafe_case"
			exit 1
		fi
	done
	wait_for_rows 1 "allow_same_cluster_src_copy.t2"
	ensure 30 check_changefeed_state "$UP_PD_ENDPOINT" "$allow_same_cluster_id" "stopped" "null" ""
	# Rejected updates must preserve the safe configuration across resume.
	cdc_cli_changefeed resume -c "$allow_same_cluster_id"
	run_sql "insert into $src_db.t1 values (3, 'c');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	wait_for_rows 3 "$dst_db.$dst_table"
	ensure 30 check_changefeed_state "$UP_PD_ENDPOINT" "$allow_same_cluster_id" "normal" "null" ""

	# 4) The same configuration also routes future databases and derived table names.
	run_sql "create database isolation_src_future character set utf8mb4 collate utf8mb4_general_ci; create table isolation_src_future.t1 (id int primary key);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists "copy_isolation_src_future.t1_routed" "$UP_TIDB_HOST" "$UP_TIDB_PORT" 90
	run_sql "insert into isolation_src_future.t1 values (1);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	wait_for_rows 1 "copy_isolation_src_future.t1_routed"
	ensure 30 check_changefeed_state "$UP_PD_ENDPOINT" "$allow_same_cluster_id" "normal" "null" ""
	run_sql "alter database isolation_src_future collate utf8mb4_bin;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	wait_for_schema_count() {
		local expected=$1 predicate=$2 count=0
		for _ in $(seq 1 60); do
			count=$(mysql -h"$UP_TIDB_HOST" -P"$UP_TIDB_PORT" -uroot -N \
				-e "select count(*) from information_schema.schemata where schema_name='copy_isolation_src_future' $predicate")
			if [ "$count" == "$expected" ]; then
				return 0
			fi
			sleep 2
		done
		echo "Expected schema count $expected ($predicate), got: $count"
		return 1
	}
	wait_for_schema_count 1 "and default_collation_name='utf8mb4_bin'"
	run_sql "drop database isolation_src_future;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	wait_for_schema_count 0 ""
	wait_for_rows 3 "$src_db.t1"
	ensure 30 check_changefeed_state "$UP_PD_ENDPOINT" "$allow_same_cluster_id" "normal" "null" ""
	wait_for_rows 1 "allow_same_cluster_src_copy.t2"
	cdc_cli_changefeed remove -c "$allow_same_cluster_id"

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
