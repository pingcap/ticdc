#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR="$OUT_DIR/$TEST_NAME"
CDC_BINARY=cdc.test
SINK_TYPE="$1"
ROUTE_NAME_SOURCE_DB=route_name_src
ROUTE_NAME_EXTRA_DB=route_name_extra
ROUTE_NAME_TARGET_DB=route_name_target
ROUTE_NAME_EXTRA_TARGET_DB=route_name_extra_target
ROUTE_FAILPOINT_BLOCK_BEFORE_WRITE=github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockOrWaitBeforeWrite
ROUTE_CDC_ADDRS=("127.0.0.1:8300" "127.0.0.1:8301")

function verify_table_route_result() {
	local work_dir=$1
	local target_db=${2:-target_db}
	local target_extra_db=${3:-target_extra_db}
	local diff_config=${4:-$CUR/conf/diff_config.toml}

	check_table_exists "$target_db.finish_mark_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 90
	check_sync_diff "$work_dir" "$diff_config" 120

	check_table_not_exists source_db.users "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_db.orders "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.external_users "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.external_users_from_default "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.users_view_from_default "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.orders_column_view_from_default "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists source_extra_db.partitioned_events_like_from_default "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists "$target_db.temp_table_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists "$target_db.cross_move_source_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists "$target_db.multi_rename_a_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists "$target_db.multi_rename_b_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_table_not_exists "$target_db.to_be_dropped_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql "SHOW CREATE VIEW ${target_db}.user_order_view_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "user_order_view_routed"
	check_contains "users_routed"
	check_contains "orders_routed"
	run_sql "SHOW CREATE VIEW ${target_extra_db}.users_view_from_default_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "users_view_from_default_routed"
	check_contains "$target_db"
	check_contains "users_routed"
	run_sql "SHOW CREATE VIEW ${target_extra_db}.orders_column_view_from_default_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "orders_column_view_from_default_routed"
	check_contains "\`${target_db}\`.\`orders_routed\`.\`id\`"
	check_contains "FROM \`${target_db}\`.\`orders_routed\`"
	check_table_not_exists "$target_db.transient_view_routed" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
}

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

function run_mysql() {
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"

	start_tidb_cluster --workdir "$WORK_DIR"

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME"

	SINK_URI="mysql://normal:123456@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"
	local normal_changefeed_id="table-route-mysql"
	cdc_cli_changefeed create -c "$normal_changefeed_id" --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"

	run_sql_file "$CUR/data/test.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	verify_table_route_result "$WORK_DIR"
	drop_table_route_source_databases
	verify_table_route_drop_database
	cdc_cli_changefeed remove -c "$normal_changefeed_id"

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
	run_route_admission_failover_case

	cleanup_process "$CDC_BINARY"
	check_logs "$WORK_DIR"
}

function run_storage_case() {
	local protocol=$1
	local work_dir="$WORK_DIR/$protocol"
	local storage_dir="$work_dir/storage_test"
	local sink_uri="file://$storage_dir?flush-interval=5s&protocol=$protocol"
	if [ "$protocol" = "canal-json" ]; then
		sink_uri="$sink_uri&enable-tidb-extension=true"
	fi

	rm -rf "$work_dir" && mkdir -p "$work_dir"

	start_tidb_cluster --workdir "$work_dir"

	run_cdc_server --workdir "$work_dir" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME"

	cdc_cli_changefeed create --sink-uri="$sink_uri" --config="$CUR/conf/changefeed.toml"

	run_sql_file "$CUR/data/test.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	run_storage_consumer "$work_dir" "$sink_uri" "$CUR/conf/changefeed.toml" "$protocol"

	verify_table_route_result "$work_dir"
	check_storage_files_use_target_names "$storage_dir"
	drop_table_route_source_databases
	verify_table_route_drop_database

	stop_test "$work_dir"
	check_logs "$work_dir"
}

function run_storage() {
	run_storage_case canal-json
	run_storage_case csv
}

function start_schema_registry() {
	if curl -o /dev/null -s "http://127.0.0.1:8088"; then
		return
	fi

	echo 'Starting schema registry...'
	./bin/bin/schema-registry-start -daemon ./bin/etc/schema-registry/schema-registry.properties
	local i=0
	while ! curl -o /dev/null -s "http://127.0.0.1:8088"; do
		i=$((i + 1))
		if [ "$i" -gt 30 ]; then
			echo 'Failed to start schema registry'
			exit 1
		fi
		sleep 2
	done
	curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" --data '{"compatibility": "NONE"}' http://127.0.0.1:8088/config
}

function render_table_route_configs() {
	local work_dir=$1
	local target_db=$2
	local target_extra_db=$3
	local changefeed_config=$4
	local diff_config=$5

	sed -e "s/target_extra_db/${target_extra_db}/g" \
		-e "s/target_db/${target_db}/g" \
		"$CUR/conf/changefeed.toml" >"$changefeed_config"

	sed -e "s/target_extra_db/${target_extra_db}/g" \
		-e "s/target_db/${target_db}/g" \
		-e "s|/tmp/tidb_cdc_test/table_route/sync_diff/output|${work_dir}/sync_diff/output|g" \
		"$CUR/conf/diff_config.toml" >"$diff_config"
}

function kafka_sink_uri() {
	local topic_name=$1
	local protocol=$2
	local extra_params=$3
	local sink_uri="kafka://127.0.0.1:9092/${topic_name}?protocol=${protocol}&partition-num=1&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760"
	if [ "$extra_params" != "" ]; then
		sink_uri="${sink_uri}&${extra_params}"
	fi
	echo "$sink_uri"
}

function run_table_route_kafka_consumer() {
	local work_dir=$1
	local sink_uri=$2
	local changefeed_config=$3
	local schema_registry_uri=$4
	local log_suffix=$5
	local protocol_case=$6
	local downstream_uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/?safe-mode=true&batch-dml-enable=false&enable-ddl-ts=false"

	local args=(
		--log-file "$work_dir/cdc_kafka_consumer${log_suffix}.log"
		--log-level debug
		--upstream-uri "$sink_uri"
		--downstream-uri "$downstream_uri"
		--config "$changefeed_config"
	)
	if [ "$schema_registry_uri" != "" ]; then
		args+=(--schema-registry-uri "$schema_registry_uri")
	fi
	if [[ "$protocol_case" == simple_* ]]; then
		args+=(--upstream-tidb-dsn "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/?")
	fi

	cdc_kafka_consumer "${args[@]}" >>"$work_dir/cdc_kafka_consumer_stdout${log_suffix}.log" 2>&1 &
}

function run_kafka() {
	local schema_registry_uri="http://127.0.0.1:8088"
	local cases=(
		"canal_json|canal-json||enable-tidb-extension=true"
		"open_protocol|open-protocol||"
		"simple_json|simple||"
		"simple_avro|simple||encoding-format=avro"
		"avro|avro|$schema_registry_uri|enable-tidb-extension=true&avro-enable-watermark=true&avro-decimal-handling-mode=string&avro-bigint-unsigned-handling-mode=string"
		"debezium|debezium||enable-tidb-extension=true"
	)

	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	start_schema_registry
	start_tidb_cluster --workdir "$WORK_DIR"
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME"

	local case_entry
	for case_entry in "${cases[@]}"; do
		local protocol_case protocol schema_registry extra_params
		IFS='|' read -r protocol_case protocol schema_registry extra_params <<<"$case_entry"
		local work_dir="$WORK_DIR/$protocol_case"
		local target_db="target_${protocol_case}_db"
		local target_extra_db="target_${protocol_case}_extra_db"
		local changefeed_config="$work_dir/changefeed.toml"
		local diff_config="$work_dir/diff_config.toml"
		local topic_case="${protocol_case//_/-}"
		local topic_name="ticdc-table-route-${topic_case}-${RANDOM}"
		local sink_uri
		sink_uri=$(kafka_sink_uri "$topic_name" "$protocol" "$extra_params")

		mkdir -p "$work_dir"
		render_table_route_configs "$work_dir" "$target_db" "$target_extra_db" "$changefeed_config" "$diff_config"

		if [ "$schema_registry" != "" ]; then
			cdc_cli_changefeed create -c "table-route-${topic_case}" --sink-uri="$sink_uri" --config="$changefeed_config" --schema-registry="$schema_registry"
		else
			cdc_cli_changefeed create -c "table-route-${topic_case}" --sink-uri="$sink_uri" --config="$changefeed_config"
		fi
		run_table_route_kafka_consumer "$work_dir" "$sink_uri" "$changefeed_config" "$schema_registry" "_$protocol_case" "$protocol_case"
	done

	run_sql_file "$CUR/data/test.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	for case_entry in "${cases[@]}"; do
		local protocol_case protocol schema_registry extra_params
		IFS='|' read -r protocol_case protocol schema_registry extra_params <<<"$case_entry"
		local work_dir="$WORK_DIR/$protocol_case"
		verify_table_route_result "$work_dir" "target_${protocol_case}_db" "target_${protocol_case}_extra_db" "$work_dir/diff_config.toml"
	done

	drop_table_route_source_databases
	for case_entry in "${cases[@]}"; do
		local protocol_case protocol schema_registry extra_params
		IFS='|' read -r protocol_case protocol schema_registry extra_params <<<"$case_entry"
		verify_table_route_drop_database "target_${protocol_case}_db" "target_${protocol_case}_extra_db"
	done

	cleanup_process "$CDC_BINARY"
	check_logs "$WORK_DIR"
}

function run() {
	case "$SINK_TYPE" in
	mysql) run_mysql ;;
	kafka) run_kafka ;;
	storage) run_storage ;;
	*) return ;;
	esac
}

trap 'stop_test "$WORK_DIR"' EXIT
run "$@"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
