#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR="$OUT_DIR/$TEST_NAME"
CDC_BINARY=cdc.test
SINK_TYPE="$1"

function run() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"

	start_tidb_cluster --workdir "$WORK_DIR"

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME"

	SINK_URI="mysql://normal:123456@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"
	cdc_cli_changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"

	run_sql_file "$CUR/data/test.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	check_table_exists target_db.finish_mark_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 90
	check_sync_diff "$WORK_DIR" "$CUR/conf/diff_config.toml" 120

	check_table_not_exists source_db.users "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_db.orders "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.external_users "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.external_users_from_default "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.users_view_from_default "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.orders_column_view_from_default "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.partitioned_events_like_from_default "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists target_db.temp_table_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists target_db.cross_move_source_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists target_db.multi_rename_a_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists target_db.multi_rename_b_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists target_db.to_be_dropped_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql "SHOW CREATE VIEW target_db.user_order_view_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "user_order_view_routed"
	check_contains "users_routed"
	check_contains "orders_routed"
	run_sql "SHOW CREATE VIEW target_extra_db.users_view_from_default_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "users_view_from_default_routed"
	check_contains "target_db"
	check_contains "users_routed"
	run_sql "SHOW CREATE VIEW target_extra_db.orders_column_view_from_default_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "orders_column_view_from_default_routed"
	check_contains 'target_db`.`orders_routed`.`id'
	check_contains 'FROM `target_db`.`orders_routed`'
	check_table_not_exists source_extra_db.correlated_users_view "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql "SHOW CREATE VIEW target_extra_db.correlated_users_view_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains 'target_db`.`orders_routed`.`user_id'
	check_contains 'target_db`.`users_routed`.`id'
	# Users 2 and 4 have no orders: the result also detects a lost correlation.
	run_sql "SELECT GROUP_CONCAT(id ORDER BY id) AS matched_ids FROM source_extra_db.correlated_users_view" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_contains 'matched_ids: 1,3'
	run_sql "SELECT GROUP_CONCAT(id ORDER BY id) AS matched_ids FROM target_extra_db.correlated_users_view_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains 'matched_ids: 1,3'
	check_table_not_exists target_db.transient_view_routed "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"

	# Compare actual CTE results, including a CTE that shadows a physical table.
	sed 's/_routed//g' "$CUR/data/cte_query.sql" |
		mysql -uroot -h"$UP_TIDB_HOST" -P"$UP_TIDB_PORT" -Dsource_db -N -B >"$WORK_DIR/cte_upstream.txt"
	mysql -uroot -h"$DOWN_TIDB_HOST" -P"$DOWN_TIDB_PORT" -Dtarget_db -N -B \
		<"$CUR/data/cte_query.sql" >"$WORK_DIR/cte_downstream.txt"
	diff -u "$WORK_DIR/cte_upstream.txt" "$WORK_DIR/cte_downstream.txt"

<<<<<<< HEAD
=======
function drop_table_route_source_databases() {
	run_sql "DROP DATABASE source_extra_db" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "DROP DATABASE source_db" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
}

function verify_table_route_drop_database() {
	local target_db=${1:-target_db}
	local target_extra_db=${2:-target_extra_db}

	check_db_not_exists "$target_extra_db" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 90
	check_db_not_exists "$target_db" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 90
}

function get_table_route_dispatcher_count() {
	local changefeed_id=$1
	local addr="${CDC_HOST}:${CDC_PORT}"

	curl -s -X GET "http://${addr}/api/v2/changefeeds/${changefeed_id}/get_dispatcher_count?mode=0&keyspace=${KEYSPACE_NAME}" | jq -r '.count'
}

function render_table_route_split_config() {
	local changefeed_config=$1

	cp "$CUR/conf/changefeed.toml" "$changefeed_config"
	cat >>"$changefeed_config" <<EOF

[scheduler]
enable-table-across-nodes = true
region-threshold = 1
region-count-per-span = 10
force-split = true
EOF
}

function verify_table_route_split_effective() {
	local changefeed_id=$1
	local table_id
	local before_count
	local expected_count

	query_dispatcher_count "${CDC_HOST}:${CDC_PORT}" "$changefeed_id" -1 60
	before_count=$(get_table_route_dispatcher_count "$changefeed_id")
	if [ -z "$before_count" ] || [ "$before_count" = "null" ]; then
		echo "failed to query dispatcher count before table route split, got: $before_count"
		exit 1
	fi

	run_sql "SPLIT TABLE source_db.users BETWEEN (1) AND (100000) REGIONS 20;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	table_id=$(get_table_id "source_db" "users")
	split_table_with_retry "$table_id" "$changefeed_id" 20

	expected_count=$((before_count + 1))
	query_dispatcher_count "${CDC_HOST}:${CDC_PORT}" "$changefeed_id" "$expected_count" 60 ge
}

function check_storage_files_use_target_names() {
	local storage_dir=$1

	ensure 60 test -d "$storage_dir/target_db/users_routed/meta"
	ensure 60 test -d "$storage_dir/target_extra_db/external_users_routed/meta"

	if [ -e "$storage_dir/source_db" ] || [ -e "$storage_dir/source_extra_db" ]; then
		echo "storage table route wrote source table directories:"
		find "$storage_dir" -maxdepth 2 -type d | sort
		exit 1
	fi
}

function render_name_change_route_config() {
	local changefeed_config=$1

	cat >"$changefeed_config" <<EOF
[filter]
rules = ['$ROUTE_NAME_SOURCE_DB.*', '$ROUTE_NAME_EXTRA_DB.*']

[sink]
[[sink.dispatchers]]
matcher = ['$ROUTE_NAME_SOURCE_DB.*']
target-schema = '$ROUTE_NAME_TARGET_DB'
target-table = '{table}_routed'

[[sink.dispatchers]]
matcher = ['$ROUTE_NAME_EXTRA_DB.*']
target-schema = '$ROUTE_NAME_EXTRA_TARGET_DB'
target-table = '{table}_routed'
EOF
}

function cleanup_name_change_route_databases() {
	run_sql "DROP DATABASE IF EXISTS $ROUTE_NAME_SOURCE_DB;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "DROP DATABASE IF EXISTS $ROUTE_NAME_EXTRA_DB;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "DROP DATABASE IF EXISTS $ROUTE_NAME_TARGET_DB;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql "DROP DATABASE IF EXISTS $ROUTE_NAME_EXTRA_TARGET_DB;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
}

function ensure_downstream_contains() {
	local sql=$1
	local expected=$2
	local retry=${3:-60}

	ensure "$retry" "run_sql \"$sql\" \"$DOWN_TIDB_HOST\" \"$DOWN_TIDB_PORT\" && check_contains \"$expected\""
}

function verify_name_change_route_result() {
	check_table_exists "$ROUTE_NAME_TARGET_DB.finish_name_change_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_exists "$ROUTE_NAME_TARGET_DB.batch_05_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_exists "$ROUTE_NAME_TARGET_DB.alt_renamed_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_not_exists "$ROUTE_NAME_TARGET_DB.alt_rename_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_exists "$ROUTE_NAME_TARGET_DB.multi_a_new_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_exists "$ROUTE_NAME_TARGET_DB.multi_b_new_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_not_exists "$ROUTE_NAME_TARGET_DB.multi_a_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_not_exists "$ROUTE_NAME_TARGET_DB.multi_b_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_exists "$ROUTE_NAME_EXTRA_TARGET_DB.cross_moved_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_not_exists "$ROUTE_NAME_TARGET_DB.cross_move_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_exists "$ROUTE_NAME_TARGET_DB.swap_a_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_exists "$ROUTE_NAME_TARGET_DB.swap_b_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_not_exists "$ROUTE_NAME_TARGET_DB.drop_1_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_not_exists "$ROUTE_NAME_TARGET_DB.drop_2_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_exists "$ROUTE_NAME_TARGET_DB.recreate_old_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_exists "$ROUTE_NAME_EXTRA_TARGET_DB.extra_seed_new_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_table_not_exists "$ROUTE_NAME_EXTRA_TARGET_DB.extra_seed_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120

	ensure_downstream_contains "SELECT note FROM $ROUTE_NAME_TARGET_DB.alt_renamed_routed WHERE id = 2;" "after alter rename"
	ensure_downstream_contains "SELECT note FROM $ROUTE_NAME_TARGET_DB.multi_a_new_routed WHERE id = 2;" "multi_a_after"
	ensure_downstream_contains "SELECT note FROM $ROUTE_NAME_TARGET_DB.recreate_old_routed WHERE id = 2;" "recreated"
	ensure_downstream_contains "SELECT note FROM $ROUTE_NAME_TARGET_DB.swap_a_routed WHERE id = 2;" "swap_b_original"
	ensure_downstream_contains "SELECT note FROM $ROUTE_NAME_TARGET_DB.swap_b_routed WHERE id = 1;" "swap_a_original"
	ensure_downstream_contains "SELECT note FROM $ROUTE_NAME_EXTRA_TARGET_DB.cross_moved_routed WHERE id = 2;" "cross_move_after"
}

function run_pause_resume_name_change_case() {
	local changefeed_id=table-route-pause-resume-name-change
	local changefeed_config="$WORK_DIR/$changefeed_id.toml"
	local start_ts

	echo "[$(date)] <<<<<< run table route pause/resume name-change case >>>>>>"
	cleanup_name_change_route_databases
	render_name_change_route_config "$changefeed_config"

	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	cdc_cli_changefeed create -c "$changefeed_id" --start-ts="$start_ts" --sink-uri="$SINK_URI" --config="$changefeed_config"
	ensure 20 check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "$changefeed_id" "normal" "null" ""

	cdc_cli_changefeed pause -c "$changefeed_id"
	ensure 20 check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "$changefeed_id" "stopped" "null" ""

	run_sql_file "$CUR/data/name_change_prepare.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	cdc_cli_changefeed resume -c "$changefeed_id"
	ensure 20 check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "$changefeed_id" "normal" "null" ""
	check_table_exists "$ROUTE_NAME_TARGET_DB.batch_05_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120

	run_sql_file "$CUR/data/name_change_ddls.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	verify_name_change_route_result

	cdc_cli_changefeed remove -c "$changefeed_id"
	cleanup_name_change_route_databases
}

function get_table_route_maintainer_addr() {
	local api_addr=$1
	local changefeed_id=$2

	curl -s "http://${api_addr}/api/v2/changefeeds/${changefeed_id}?keyspace=$KEYSPACE_NAME" | jq -r '.maintainer_addr'
}

function wait_for_table_route_maintainer_move() {
	local api_addr=$1
	local changefeed_id=$2
	local old_addr=$3

	for ((i = 0; i < 30; i++)); do
		local new_addr
		new_addr=$(get_table_route_maintainer_addr "$api_addr" "$changefeed_id")
		if [ -n "$new_addr" ] && [ "$new_addr" != "null" ] && [ "$new_addr" != "$old_addr" ]; then
			echo "$new_addr"
			return 0
		fi
		sleep 2
	done
	echo "maintainer did not move from $old_addr" >&2
	return 1
}

function pick_table_route_addr_excluding() {
	local excluded_addr=$1
	local addr

	for addr in "${ROUTE_CDC_ADDRS[@]}"; do
		if [ "$addr" != "$excluded_addr" ]; then
			echo "$addr"
			return 0
		fi
	done
	return 1
}

function enable_route_write_failpoint_on_all_addrs() {
	local addr

	for addr in "${ROUTE_CDC_ADDRS[@]}"; do
		enable_failpoint --addr "$addr" --name "$ROUTE_FAILPOINT_BLOCK_BEFORE_WRITE" --expr "pause"
	done
}

function disable_route_write_failpoint_on_all_addrs_best_effort() {
	local addr

	set +e
	for addr in "${ROUTE_CDC_ADDRS[@]}"; do
		disable_failpoint --addr "$addr" --name "$ROUTE_FAILPOINT_BLOCK_BEFORE_WRITE"
	done
	set -e
}

function run_route_admission_failover_case() {
	local changefeed_id=table-route-admission-failover
	local changefeed_config="$WORK_DIR/$changefeed_id.toml"
	local maintainer_addr
	local maintainer_host
	local maintainer_port
	local maintainer_pid
	local live_api_addr
	local new_maintainer_addr
	local start_ts

	echo "[$(date)] <<<<<< run table route admission maintainer failover case >>>>>>"
	cleanup_name_change_route_databases
	render_name_change_route_config "$changefeed_config"
	cleanup_process "$CDC_BINARY"

	export GO_FAILPOINTS='github.com/pingcap/ticdc/maintainer/scheduler/StopBalanceScheduler=return(true)'
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME" --logsuffix "route-failover-0" --addr "127.0.0.1:8300"
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME" --logsuffix "route-failover-1" --addr "127.0.0.1:8301"
	export GO_FAILPOINTS=''

	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	cdc_cli_changefeed create -c "$changefeed_id" --start-ts="$start_ts" --sink-uri="$SINK_URI" --config="$changefeed_config"
	ensure 20 check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "$changefeed_id" "normal" "null" ""

	run_sql_file "$CUR/data/name_change_prepare.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists "$ROUTE_NAME_TARGET_DB.failover_old_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120

	maintainer_addr=$(get_table_route_maintainer_addr "127.0.0.1:8300" "$changefeed_id")
	if [ -z "$maintainer_addr" ] || [ "$maintainer_addr" = "null" ]; then
		echo "failed to get maintainer address for $changefeed_id" >&2
		exit 1
	fi
	live_api_addr=$(pick_table_route_addr_excluding "$maintainer_addr")

	enable_route_write_failpoint_on_all_addrs
	run_sql "RENAME TABLE $ROUTE_NAME_SOURCE_DB.failover_old TO $ROUTE_NAME_SOURCE_DB.failover_new;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	# Keep this aligned with existing DDL failover tests: wait until the DDL has reached the blocked write point.
	sleep 20

	maintainer_host=${maintainer_addr%:*}
	maintainer_port=${maintainer_addr#*:}
	maintainer_pid=$(get_cdc_pid "$maintainer_host" "$maintainer_port")
	kill_cdc_pid "$maintainer_pid"
	new_maintainer_addr=$(wait_for_table_route_maintainer_move "$live_api_addr" "$changefeed_id" "$maintainer_addr")
	echo "maintainer moved from $maintainer_addr to $new_maintainer_addr"

	disable_route_write_failpoint_on_all_addrs_best_effort
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME" --logsuffix "route-failover-restart" --addr "$maintainer_addr"

	check_table_exists "$ROUTE_NAME_TARGET_DB.failover_new_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 180
	check_table_not_exists "$ROUTE_NAME_TARGET_DB.failover_old_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 180
	run_sql "INSERT INTO $ROUTE_NAME_SOURCE_DB.failover_new VALUES (2, 'after failover');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	ensure_downstream_contains "SELECT note FROM $ROUTE_NAME_TARGET_DB.failover_new_routed WHERE id = 2;" "after failover" 90
	ensure 20 check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "$changefeed_id" "normal" "null" ""

	cdc_cli_changefeed remove -c "$changefeed_id"
	cleanup_process "$CDC_BINARY"
	cleanup_name_change_route_databases
}

function cleanup_flashback_route_databases() {
	run_sql "DROP DATABASE IF EXISTS source_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "DROP DATABASE IF EXISTS table_only_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "DROP DATABASE IF EXISTS old_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "DROP DATABASE IF EXISTS new_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "DROP DATABASE IF EXISTS target_db;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql "DROP DATABASE IF EXISTS table_only_db;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql "DROP DATABASE IF EXISTS old_target_db;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql "DROP DATABASE IF EXISTS new_target_db;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
}

function run_flashback_database_case() {
	local changefeed_id=table-route-flashback-database
	local start_ts

	echo "[$(date)] <<<<<< run table route FLASHBACK DATABASE case >>>>>>"
	cleanup_flashback_route_databases
	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	cdc_cli_changefeed create -c "$changefeed_id" --start-ts="$start_ts" --sink-uri="$SINK_URI" --config="$CUR/conf/flashback_changefeed.toml"
	ensure 20 check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "$changefeed_id" "normal" "null" ""

	run_sql "CREATE DATABASE source_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "CREATE TABLE source_db.t1 (id INT PRIMARY KEY, value VARCHAR(50));" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "INSERT INTO source_db.t1 VALUES (1, 'before flashback');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists "target_db.t1_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120

	run_sql "DROP DATABASE source_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_db_not_exists "target_db" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	run_sql "FLASHBACK DATABASE source_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists "target_db.t1_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	ensure_downstream_contains "SELECT value FROM target_db.t1_routed WHERE id = 1;" "before flashback" 90
	run_sql "INSERT INTO source_db.t1 VALUES (2, 'after flashback');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	ensure_downstream_contains "SELECT value FROM target_db.t1_routed WHERE id = 2;" "after flashback" 90

	run_sql "CREATE DATABASE table_only_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "CREATE TABLE table_only_db.t2 (id INT PRIMARY KEY, value VARCHAR(50));" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "INSERT INTO table_only_db.t2 VALUES (1, 'before table-only flashback');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists "table_only_db.t2_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120

	run_sql "DROP DATABASE table_only_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_db_not_exists "table_only_db" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	run_sql "FLASHBACK DATABASE table_only_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists "table_only_db.t2_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	ensure_downstream_contains "SELECT value FROM table_only_db.t2_routed WHERE id = 1;" "before table-only flashback" 90
	run_sql "INSERT INTO table_only_db.t2 VALUES (2, 'after table-only flashback');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	ensure_downstream_contains "SELECT value FROM table_only_db.t2_routed WHERE id = 2;" "after table-only flashback" 90

	run_sql "CREATE DATABASE old_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "CREATE TABLE old_db.t2 (id INT PRIMARY KEY, value VARCHAR(50));" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "INSERT INTO old_db.t2 VALUES (1, 'before flashback to');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists "old_target_db.t2_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120

	run_sql "DROP DATABASE old_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_db_not_exists "old_target_db" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	run_sql "FLASHBACK DATABASE old_db TO new_db;" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists "new_target_db.t2_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	check_db_not_exists "old_target_db" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 120
	ensure_downstream_contains "SELECT value FROM new_target_db.t2_routed WHERE id = 1;" "before flashback to" 90
	run_sql "INSERT INTO new_db.t2 VALUES (2, 'after flashback to');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	ensure_downstream_contains "SELECT value FROM new_target_db.t2_routed WHERE id = 2;" "after flashback to" 90
	ensure 20 check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" "$changefeed_id" "normal" "null" ""

	cdc_cli_changefeed remove -c "$changefeed_id"
	cleanup_flashback_route_databases
}

function run_mysql() {
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"

	start_tidb_cluster --workdir "$WORK_DIR"

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME"

	SINK_URI="mysql://normal:123456@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"
	local normal_changefeed_id="table-route-mysql"
	cdc_cli_changefeed create -c "$normal_changefeed_id" --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"

	run_sql_file "$CUR/data/test.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	verify_table_route_result "$WORK_DIR"
>>>>>>> 9ea68a2e7 (schemastore: support FLASHBACK DATABASE DDL (#6258))
	run_sql_file "$CUR/data/exchange_partition.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_sync_diff "$WORK_DIR" "$CUR/conf/diff_config.toml" 120

<<<<<<< HEAD
	run_sql "DROP DATABASE source_extra_db" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "DROP DATABASE source_db" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_db_not_exists target_extra_db "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 90
	check_db_not_exists target_db "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 90
=======
	local split_changefeed_id="table-route-split"
	local split_changefeed_config="$WORK_DIR/table_route_split_changefeed.toml"
	render_table_route_split_config "$split_changefeed_config"
	cdc_cli_changefeed create -c "$split_changefeed_id" --sink-uri="$SINK_URI" --config="$split_changefeed_config"

	run_sql_file "$CUR/data/test.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	verify_table_route_result "$WORK_DIR"
	verify_table_route_split_effective "$split_changefeed_id"
	run_sql "INSERT INTO source_db.users VALUES (6, 'Split', 'split@example.com');" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	check_sync_diff "$WORK_DIR" "$CUR/conf/diff_config.toml" 120
	drop_table_route_source_databases
	verify_table_route_drop_database
	cdc_cli_changefeed remove -c "$split_changefeed_id"

	run_pause_resume_name_change_case
	run_flashback_database_case
	run_route_admission_failover_case
>>>>>>> 9ea68a2e7 (schemastore: support FLASHBACK DATABASE DDL (#6258))

	cleanup_process "$CDC_BINARY"
}

trap 'stop_test "$WORK_DIR"' EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
