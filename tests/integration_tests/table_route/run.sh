#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR="$OUT_DIR/$TEST_NAME"
CDC_BINARY=cdc.test
SINK_TYPE="$1"

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

function run_mysql() {
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"

	start_tidb_cluster --workdir "$WORK_DIR"

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME"

	SINK_URI="mysql://normal:123456@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"
	cdc_cli_changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"

	run_sql_file "$CUR/data/test.sql" "$UP_TIDB_HOST" "$UP_TIDB_PORT"

	verify_table_route_result "$WORK_DIR"
	drop_table_route_source_databases
	verify_table_route_drop_database

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
