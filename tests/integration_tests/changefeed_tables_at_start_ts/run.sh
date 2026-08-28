#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

DB_NAME=changefeed_tables_at_start_ts
NO_PK_TABLE=no_pk_partition_table
ELIGIBILITY_TABLE=eligibility_partition_table
ddl_names=()
ddl_commit_ts=()
expected_table_ids=()
expected_table_counts=()
filter_checkpoint_ts=0
filter_expected_table_ids=
last_checkpoint_ts=0

mysql_upstream() {
	mysql -uroot -h"$UP_TIDB_HOST" -P"$UP_TIDB_PORT" --default-character-set utf8mb4 "$@"
}

get_ddl_commit_ts() {
	local table_name=$1
	local ddl_marker=$2
	local ddl_ts=0
	for _ in $(seq 1 30); do
		ddl_ts=$(curl -fsS "http://${UP_TIDB_HOST}:${UP_TIDB_STATUS}/ddl/history" |
			python3 -c '
import json
import sys

table_name = sys.argv[1].lower()
ddl_marker = sys.argv[2].lower()
history = json.load(sys.stdin)
timestamps = [
    entry.get("binlog", {}).get("FinishedTS", 0)
    for entry in history
    if table_name in entry.get("query", "").lower()
    and ddl_marker in entry.get("query", "").lower()
]
print(max(timestamps, default=0))
' "$table_name" "$ddl_marker")
		if [[ "$ddl_ts" =~ ^[1-9][0-9]*$ ]]; then
			echo "$ddl_ts"
			return 0
		fi
		sleep 1
	done

	echo "failed to find DDL commit ts for $table_name" >&2
	return 1
}

execute_partition_ddls() {
	run_sql_file "$CUR/data/add_partition.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	record_ddl_checkpoint add_partition pt_0000 "add partition"
	local checkpoint_index=$((${#ddl_names[@]} - 1))
	validate_checkpoint "${ddl_names[$checkpoint_index]}" "${ddl_commit_ts[$checkpoint_index]}" \
		"${expected_table_ids[$checkpoint_index]}"
	validate_checkpoint filtered_partition "$filter_checkpoint_ts" \
		"$filter_expected_table_ids" "$filter_config"

	run_sql_file "$CUR/data/drop_partition.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	record_ddl_checkpoint drop_partition pt_0001 "drop partition"
	checkpoint_index=$((${#ddl_names[@]} - 1))
	validate_checkpoint "${ddl_names[$checkpoint_index]}" "${ddl_commit_ts[$checkpoint_index]}" \
		"${expected_table_ids[$checkpoint_index]}"

	run_sql_file "$CUR/data/truncate_partition.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	record_ddl_checkpoint truncate_partition pt_0002 "truncate partition"
	checkpoint_index=$((${#ddl_names[@]} - 1))
	validate_checkpoint "${ddl_names[$checkpoint_index]}" "${ddl_commit_ts[$checkpoint_index]}" \
		"${expected_table_ids[$checkpoint_index]}"

	run_sql_file "$CUR/data/reorganize_partition.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	record_ddl_checkpoint reorganize_partition pt_0003 "reorganize partition"
	checkpoint_index=$((${#ddl_names[@]} - 1))
	validate_checkpoint "${ddl_names[$checkpoint_index]}" "${ddl_commit_ts[$checkpoint_index]}" \
		"${expected_table_ids[$checkpoint_index]}"

	# Fast reorg requires at least tidb_ddl_disk_quota bytes of local disk. The
	# distributed task path also requires fast reorg, so disable both in
	# separate sessions before opening the DDL session.
	run_sql "SET GLOBAL tidb_enable_dist_task = OFF;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "SET GLOBAL tidb_ddl_enable_fast_reorg = OFF;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql_file "$CUR/data/add_primary_key.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	record_ddl_checkpoint add_primary_key "$ELIGIBILITY_TABLE" "add primary key" false true
	checkpoint_index=$((${#ddl_names[@]} - 1))
	validate_checkpoint "${ddl_names[$checkpoint_index]}" "${ddl_commit_ts[$checkpoint_index]}" \
		"${expected_table_ids[$checkpoint_index]}"

	run_sql_file "$CUR/data/exchange_partition.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	record_ddl_checkpoint exchange_partition pt_0004 "exchange partition" false true
	checkpoint_index=$((${#ddl_names[@]} - 1))
	validate_checkpoint "${ddl_names[$checkpoint_index]}" "${ddl_commit_ts[$checkpoint_index]}" \
		"${expected_table_ids[$checkpoint_index]}"

	for i in "${!ddl_names[@]}"; do
		printf 'partition DDL checkpoint: name=%s commit_ts=%s table_count=%s\n' \
			"${ddl_names[$i]}" "${ddl_commit_ts[$i]}" "${expected_table_counts[$i]}"
	done
}

record_checkpoint() {
	local checkpoint_name=$1
	local checkpoint_ts=$2
	local include_no_pk=$3
	local include_eligibility=$4
	local table_ids
	local table_count

	if ((checkpoint_ts <= last_checkpoint_ts)); then
		echo "Checkpoint $checkpoint_name has a non-increasing commit ts: $checkpoint_ts" >&2
		return 1
	fi

	# Capture the expected physical IDs at this checkpoint, before the next DDL
	# changes the current TiDB metadata.
	table_ids=$(get_expected_table_ids "$include_no_pk" "$include_eligibility")
	table_count=$(awk -F, 'NF {print NF}' <<<"$table_ids")
	if [ -z "$table_ids" ] || [ -z "$table_count" ]; then
		echo "failed to get expected tables after $checkpoint_name" >&2
		return 1
	fi

	ddl_names+=("$checkpoint_name")
	ddl_commit_ts+=("$checkpoint_ts")
	expected_table_ids+=("$table_ids")
	expected_table_counts+=("$table_count")
	last_checkpoint_ts=$checkpoint_ts
}

record_ddl_checkpoint() {
	local ddl_name=$1
	local table_name=$2
	local ddl_marker=$3
	local include_no_pk=${4:-false}
	local include_eligibility=${5:-false}
	local ddl_ts

	ddl_ts=$(get_ddl_commit_ts "$table_name" "$ddl_marker")
	record_checkpoint "$ddl_name" "$ddl_ts" "$include_no_pk" "$include_eligibility"
	if [ "$ddl_name" = add_partition ]; then
		filter_checkpoint_ts=$ddl_ts
		filter_expected_table_ids=$(get_expected_table_ids_for_table pt_0000)
	fi
}

get_expected_table_ids() {
	local include_no_pk=${1:-false}
	local include_eligibility=${2:-false}
	local table_condition="TABLE_NAME REGEXP '^pt_[0-9]+$'"
	if [ "$include_no_pk" = true ]; then
		table_condition+=" OR TABLE_NAME = '${NO_PK_TABLE}'"
	fi
	if [ "$include_eligibility" = true ]; then
		table_condition+=" OR TABLE_NAME = '${ELIGIBILITY_TABLE}'"
	fi

	mysql_upstream -N -B -e "
		SELECT table_id
		FROM (
			-- The table-trigger dispatcher uses table ID 0 and is included in
			-- the changefeed table list together with physical table IDs.
			SELECT 0 AS table_id
			UNION ALL
			-- EXCHANGE swaps the partition ID and the non-partitioned table ID,
			-- so both information_schema tables are needed here.
			SELECT TIDB_PARTITION_ID AS table_id
			FROM information_schema.partitions
			WHERE TABLE_SCHEMA = '${DB_NAME}'
			  AND (${table_condition})
			  AND TIDB_PARTITION_ID IS NOT NULL
			UNION ALL
			SELECT TIDB_TABLE_ID AS table_id
			FROM information_schema.tables
			WHERE TABLE_SCHEMA = '${DB_NAME}'
			  AND TABLE_NAME = 'exchange_0004'
		) AS physical_tables
		ORDER BY table_id;
	" | paste -sd, -
}

get_expected_table_ids_for_table() {
	local table_name=$1
	mysql_upstream -N -B -e "
		SELECT table_id
		FROM (
			SELECT 0 AS table_id
			UNION ALL
			SELECT TIDB_PARTITION_ID AS table_id
			FROM information_schema.partitions
			WHERE TABLE_SCHEMA = '${DB_NAME}'
			  AND TABLE_NAME = '${table_name}'
			  AND TIDB_PARTITION_ID IS NOT NULL
		) AS physical_tables
		ORDER BY table_id;
	" | paste -sd, -
}

get_actual_table_ids() {
	local changefeed_id=$1
	local response
	if ! response=$(curl -fsS "http://${CDC_HOST}:${CDC_PORT}/api/v2/changefeeds/${changefeed_id}/tables?keyspace=${KEYSPACE_NAME}"); then
		return 1
	fi
	echo "$response" | jq -r '[.items[]?.table_ids[]?] | sort | join(",")'
}

check_tables() {
	local changefeed_id=$1
	local expected_ids=$2
	local expected_count=$3
	local actual_ids
	local actual_count
	actual_ids=$(get_actual_table_ids "$changefeed_id")
	actual_count=0
	if [ -n "$actual_ids" ]; then
		actual_count=$(awk -F, '{print NF}' <<<"$actual_ids")
	fi

	echo "changefeed table count: actual=$actual_count expected=$expected_count"
	if [ "$actual_count" -ne "$expected_count" ]; then
		return 1
	fi
	if [ "$actual_ids" != "$expected_ids" ]; then
		echo "changefeed physical table IDs do not match the TiDB partition IDs" >&2
		return 1
	fi
}

wait_for_dispatcher_count() {
	local changefeed_id=$1
	local expected_count=$2
	local response
	local actual_count
	# Retry every 2 seconds for up to 10 minutes.
	for _ in $(seq 1 300); do
		response=$(curl -sS "http://${CDC_HOST}:${CDC_PORT}/api/v2/changefeeds/${changefeed_id}/get_dispatcher_count?mode=0&keyspace=${KEYSPACE_NAME}" 2>/dev/null || true)
		actual_count=$(jq -r '.count // empty' <<<"$response" 2>/dev/null || true)
		if [ "$actual_count" = "$expected_count" ]; then
			return 0
		fi
		sleep 2
	done

	echo "dispatcher count did not converge: actual=$actual_count expected=$expected_count" >&2
	return 1
}

validate_checkpoint() {
	local checkpoint_name=$1
	local start_ts=$2
	local expected_ids=$3
	local config_path=${4:-}
	local expected_count
	local changefeed_name=${checkpoint_name//_/-}
	local changefeed_id="changefeed-partition-table-${changefeed_name}-$RANDOM"
	local -a create_args=(
		create
		--pd="http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
		--start-ts="$start_ts"
		--sink-uri="blackhole://"
		--changefeed-id="$changefeed_id"
		--no-confirm=true
	)

	expected_count=$(awk -F, 'NF {print NF}' <<<"$expected_ids")
	if [ -n "$config_path" ]; then
		create_args+=(--config "$config_path")
	fi

	cdc_cli_changefeed "${create_args[@]}"
	wait_for_dispatcher_count "$changefeed_id" "$expected_count"
	for _ in $(seq 1 120); do
		if check_tables "$changefeed_id" "$expected_ids" "$expected_count"; then
			break
		fi
		sleep 2
	done
	check_tables "$changefeed_id" "$expected_ids" "$expected_count"

	cdc_cli_changefeed remove \
		--pd="http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" \
		--changefeed-id="$changefeed_id"
	printf 'validated checkpoint: name=%s commit_ts=%s table_count=%s\n' \
		"$checkpoint_name" "$start_ts" "$expected_count"
}

run() {
	# The test validates the table list API and does not need a sink-specific consumer.
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf "$WORK_DIR"
	mkdir -p "$WORK_DIR"
	start_tidb_cluster --workdir "$WORK_DIR"
	run_sql "SET GLOBAL tidb_enable_exchange_partition = ON;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"

	run_sql_file "$CUR/data/prepare.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	baseline_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	baseline_force_replicate_table_ids=$(get_expected_table_ids true true)
	record_checkpoint baseline "$baseline_ts" false false

	filter_config="$CUR/conf/filter.toml"
	force_replicate_config="$CUR/conf/force_replicate.toml"

	validate_checkpoint baseline "$baseline_ts" \
		"${expected_table_ids[0]}"
	validate_checkpoint force_replicate_baseline "$baseline_ts" \
		"$baseline_force_replicate_table_ids" "$force_replicate_config"

	execute_partition_ddls

	# Restart CDC after the first schema-store initialization. The next
	# checkpoint validations must rebuild the schema from the persisted DDLs.
	cleanup_process "$CDC_BINARY"
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"
	last_checkpoint_index=$((${#ddl_names[@]} - 1))
	validate_checkpoint restart_final "${ddl_commit_ts[$last_checkpoint_index]}" \
		"${expected_table_ids[$last_checkpoint_index]}"

	cleanup_process "$CDC_BINARY"
}

trap 'stop_test "$WORK_DIR"' EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
