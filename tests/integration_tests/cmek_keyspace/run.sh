#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"

WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
MAX_RETRIES=30
REQUIRE_ENCRYPTED_KEYSPACE_START=${REQUIRE_ENCRYPTED_KEYSPACE_START:-false}

LOCAL_KMS_ADDR=127.0.0.1
LOCAL_KMS_PORT=18080
LOCAL_KMS_PID=

UPSTREAM_VALID_KEYSPACE=keyspace-foo
UPSTREAM_INVALID_KEYSPACE=keyspace-foo-invalid
DOWNSTREAM_KEYSPACE=keyspace-bar
INVALID_CMEK_ID=00000000-0000-0000-0000-000000000000

UPSTREAM_VALID_TIDB_PORT=14001
UPSTREAM_VALID_TIDB_STATUS=15001
UPSTREAM_INVALID_TIDB_PORT=14011
UPSTREAM_INVALID_TIDB_STATUS=15011
DOWNSTREAM_TIDB_PORT=14002
DOWNSTREAM_TIDB_STATUS=15002

UPSTREAM_VALID_KEYSPACE_ID=
UPSTREAM_INVALID_KEYSPACE_ID=
DOWNSTREAM_KEYSPACE_ID=

function resolve_local_kms_binary() {
	if [ -n "${LOCAL_KMS_BINARY:-}" ] && [ -x "${LOCAL_KMS_BINARY}" ]; then
		echo "${LOCAL_KMS_BINARY}"
		return
	fi

	if command -v local-kms >/dev/null 2>&1; then
		command -v local-kms
		return
	fi

	if command -v go >/dev/null 2>&1; then
		local gopath
		gopath=$(go env GOPATH 2>/dev/null || true)
		if [ -n "$gopath" ] && [ -x "$gopath/bin/local-kms" ]; then
			echo "$gopath/bin/local-kms"
			return
		fi
	fi

	return 0
}

function stop_local_kms() {
	if [ -n "${LOCAL_KMS_PID:-}" ]; then
		kill "${LOCAL_KMS_PID}" >/dev/null 2>&1 || true
		wait "${LOCAL_KMS_PID}" >/dev/null 2>&1 || true
	fi
}

function start_local_kms() {
	local local_kms_binary
	local kms_data_dir
	local_kms_binary=$(resolve_local_kms_binary)
	if [ -z "${local_kms_binary}" ]; then
		echo "skip $TEST_NAME: local-kms binary is not found"
		return 1
	fi

	kms_data_dir="$WORK_DIR/local-kms-data"
	rm -rf "$kms_data_dir"
	mkdir -p "$kms_data_dir"

	echo "use local-kms binary: ${local_kms_binary}"
	KMS_DATA_PATH="$kms_data_dir" PORT="${LOCAL_KMS_PORT}" "${local_kms_binary}" \
		>"$WORK_DIR/local-kms.log" 2>&1 &
	LOCAL_KMS_PID=$!
	check_port_available "${LOCAL_KMS_ADDR}" "${LOCAL_KMS_PORT}" "local kms is not ready yet" 60
}

function create_kms_key() {
	curl -sS -X POST \
		"http://${LOCAL_KMS_ADDR}:${LOCAL_KMS_PORT}/" \
		-H "Content-Type: application/x-amz-json-1.1" \
		-H "X-Amz-Target: TrentService.CreateKey" \
		-d '{}' | jq -r '.KeyMetadata.KeyId'
}

function create_encrypted_keyspace() {
	local pd_host=$1
	local pd_port=$2
	local keyspace_name=$3
	local cmek_id=$4
	local endpoint=$5
	local encryption_json
	local payload
	local i=0
	local keyspace_meta
	local state
	local keyspace_id

	encryption_json=$(jq -nc \
		--arg cmek_id "${cmek_id}" \
		--arg endpoint "${endpoint}" \
		'{enabled:true, cmek_id:$cmek_id, vendor:"aws", region:"eu-west-2", endpoint:$endpoint}')

	payload=$(jq -nc \
		--arg name "${keyspace_name}" \
		--arg encryption "${encryption_json}" \
		'{name:$name, config:{encryption:$encryption}}')

	curl -sS -X POST -H "Content-Type: application/json" \
		-d "${payload}" \
		"http://${pd_host}:${pd_port}/pd/api/v2/keyspaces" >/dev/null

	while [ "$i" -lt 60 ]; do
		keyspace_meta=$(curl -sS "http://${pd_host}:${pd_port}/pd/api/v2/keyspaces" |
			jq -c --arg name "${keyspace_name}" '.keyspaces[]? | select(.name == $name)' || true)
		if [ -n "${keyspace_meta}" ]; then
			state=$(echo "${keyspace_meta}" | jq -r '.state // empty')
			keyspace_id=$(echo "${keyspace_meta}" | jq -r '.id // empty')
			if [ "${state}" = "ENABLED" ] && [ -n "${keyspace_id}" ]; then
				echo "keyspace ready: $(echo "${keyspace_meta}" | jq -c '{name:.name,id:.id,state:.state,encryption:.config.encryption}')" >&2
				echo "${keyspace_id}"
				return 0
			fi
		fi
		i=$((i + 1))
		sleep 2
	done

	echo "failed to create keyspace ${keyspace_name} on ${pd_host}:${pd_port}"
	exit 1
}

function write_tidb_keyspace_config() {
	local config_path=$1
	local keyspace_name=$2
	local socket_name=$3

	cat >"${config_path}" <<EOF
keyspace-name = "${keyspace_name}"
enable-telemetry = false
socket = "${socket_name}"

[security]
enable-sem = false
EOF
}

function start_keyspace_tidb() {
	local cluster=$1
	local keyspace_name=$2
	local tidb_port=$3
	local tidb_status=$4
	local config_path=$5
	local log_dir=$6
	local data_dir=$7

	mkdir -p "${log_dir}" "${data_dir}"
	write_tidb_keyspace_config "${config_path}" "${keyspace_name}" "/tmp/tidb-${TEST_NAME}-${keyspace_name}-${tidb_port}.sock"

	if [ "${cluster}" = "upstream" ]; then
		tidb-server \
			-P "${tidb_port}" \
			-config "${config_path}" \
			--store tikv \
			--path "${UP_PD_HOST_1}:${UP_PD_PORT_1}" \
			--status "${tidb_status}" \
			--log-file "${log_dir}/tidb.log" \
			>"${log_dir}/tidb-stdout.log" \
			2>"${log_dir}/tidb-stderr.log" &
	else
		tidb-server \
			-P "${tidb_port}" \
			-config "${config_path}" \
			--store tikv \
			--path "${DOWN_PD_HOST}:${DOWN_PD_PORT}" \
			--status "${tidb_status}" \
			--log-file "${log_dir}/tidb.log" \
			>"${log_dir}/tidb-stdout.log" \
			2>"${log_dir}/tidb-stderr.log" &
	fi

	local i=0
	while [ "$i" -lt 30 ]; do
		if mysql -h"127.0.0.1" -P"${tidb_port}" -uroot --connect-timeout=2 -e "select 1;" >/dev/null 2>&1; then
			return 0
		fi
		i=$((i + 1))
		sleep 2
	done

	echo "failed to start keyspace tidb for ${keyspace_name} on port ${tidb_port}"
	echo "tidb stderr:"
	cat "${log_dir}/tidb-stderr.log" || true
	echo "tidb stdout:"
	tail -n 120 "${log_dir}/tidb-stdout.log" || true
	echo "tidb log:"
	tail -n 120 "${log_dir}/tidb.log" || true
	return 1
}

function prepare_keyspaces_and_tidb() {
	local upstream_key_id
	local downstream_key_id
	local local_kms_endpoint

	local_kms_endpoint="http://${LOCAL_KMS_ADDR}:${LOCAL_KMS_PORT}"

	upstream_key_id=$(create_kms_key)
	if [ -z "${upstream_key_id}" ] || [ "${upstream_key_id}" = "null" ]; then
		echo "failed to create upstream kms key"
		exit 1
	fi

	downstream_key_id=$(create_kms_key)
	if [ -z "${downstream_key_id}" ] || [ "${downstream_key_id}" = "null" ]; then
		echo "failed to create downstream kms key"
		exit 1
	fi

	UPSTREAM_VALID_KEYSPACE_ID=$(create_encrypted_keyspace "$UP_PD_HOST_1" "$UP_PD_PORT_1" "$UPSTREAM_VALID_KEYSPACE" "${upstream_key_id}" "${local_kms_endpoint}")
	UPSTREAM_INVALID_KEYSPACE_ID=$(create_encrypted_keyspace "$UP_PD_HOST_1" "$UP_PD_PORT_1" "$UPSTREAM_INVALID_KEYSPACE" "${INVALID_CMEK_ID}" "${local_kms_endpoint}")
	DOWNSTREAM_KEYSPACE_ID=$(create_encrypted_keyspace "$DOWN_PD_HOST" "$DOWN_PD_PORT" "$DOWNSTREAM_KEYSPACE" "${downstream_key_id}" "${local_kms_endpoint}")

	# TiKV may generate encryption meta lazily after first writes in the keyspace.
	echo "skip waiting encryption meta before first write: valid_keyspace_id=${UPSTREAM_VALID_KEYSPACE_ID}, invalid_keyspace_id=${UPSTREAM_INVALID_KEYSPACE_ID}"

	if ! start_keyspace_tidb \
		"upstream" \
		"${UPSTREAM_VALID_KEYSPACE}" \
		"${UPSTREAM_VALID_TIDB_PORT}" \
		"${UPSTREAM_VALID_TIDB_STATUS}" \
		"$WORK_DIR/upstream-${UPSTREAM_VALID_KEYSPACE}.toml" \
		"$WORK_DIR/log/upstream/tidb-${UPSTREAM_VALID_KEYSPACE}" \
		"$WORK_DIR/test_out_data/upstream/tidb-${UPSTREAM_VALID_KEYSPACE}"; then
		return 1
	fi

	if ! start_keyspace_tidb \
		"upstream" \
		"${UPSTREAM_INVALID_KEYSPACE}" \
		"${UPSTREAM_INVALID_TIDB_PORT}" \
		"${UPSTREAM_INVALID_TIDB_STATUS}" \
		"$WORK_DIR/upstream-${UPSTREAM_INVALID_KEYSPACE}.toml" \
		"$WORK_DIR/log/upstream/tidb-${UPSTREAM_INVALID_KEYSPACE}" \
		"$WORK_DIR/test_out_data/upstream/tidb-${UPSTREAM_INVALID_KEYSPACE}"; then
		return 1
	fi

	if ! start_keyspace_tidb \
		"downstream" \
		"${DOWNSTREAM_KEYSPACE}" \
		"${DOWNSTREAM_TIDB_PORT}" \
		"${DOWNSTREAM_TIDB_STATUS}" \
		"$WORK_DIR/downstream-${DOWNSTREAM_KEYSPACE}.toml" \
		"$WORK_DIR/log/downstream/tidb-${DOWNSTREAM_KEYSPACE}" \
		"$WORK_DIR/test_out_data/downstream/tidb-${DOWNSTREAM_KEYSPACE}"; then
		return 1
	fi
}

function assert_table_not_synced_for_duration() {
	local table_name=$1
	local host=$2
	local port=$3
	local rounds=$4
	local i=0

	while [ "$i" -lt "$rounds" ]; do
		if mysql -h"$host" -P"$port" -uroot -e "show create table $table_name" >/dev/null 2>&1; then
			echo "table $table_name should not be synced to downstream"
			exit 1
		fi
		i=$((i + 1))
		sleep 2
	done
}

function run_with_valid_kms() {
	local sink_uri="mysql://root@127.0.0.1:${DOWNSTREAM_TIDB_PORT}/"
	local changefeed_id=cmek-valid

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --config "$CUR/conf/cdc_valid.toml"
	run_cdc_cli changefeed -k "$UPSTREAM_VALID_KEYSPACE" create --sink-uri="$sink_uri" -c "$changefeed_id"

	run_sql "CREATE DATABASE IF NOT EXISTS cmek_valid;" "127.0.0.1" "$UPSTREAM_VALID_TIDB_PORT"
	run_sql "CREATE TABLE cmek_valid.t (id BIGINT PRIMARY KEY, name VARCHAR(64));" "127.0.0.1" "$UPSTREAM_VALID_TIDB_PORT"
	run_sql "INSERT INTO cmek_valid.t VALUES (1, 'apple'), (2, 'banana');" "127.0.0.1" "$UPSTREAM_VALID_TIDB_PORT"

	check_table_exists "cmek_valid.t" "127.0.0.1" "$DOWNSTREAM_TIDB_PORT" 90
	check_sync_diff "$WORK_DIR" "$CUR/conf/diff_config.toml" 180

	run_cdc_cli changefeed -k "$UPSTREAM_VALID_KEYSPACE" remove -c "$changefeed_id"
	cleanup_process "$CDC_BINARY"
}

function run_with_invalid_kms() {
	local sink_uri="mysql://root@127.0.0.1:${DOWNSTREAM_TIDB_PORT}/"
	local changefeed_id=cmek-invalid

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --config "$CUR/conf/cdc_invalid.toml" --logsuffix "-invalid"
	run_cdc_cli changefeed -k "$UPSTREAM_INVALID_KEYSPACE" create --sink-uri="$sink_uri" -c "$changefeed_id"

	run_sql "CREATE DATABASE IF NOT EXISTS cmek_invalid;" "127.0.0.1" "$UPSTREAM_INVALID_TIDB_PORT"
	run_sql "CREATE TABLE cmek_invalid.t (id BIGINT PRIMARY KEY, name VARCHAR(64));" "127.0.0.1" "$UPSTREAM_INVALID_TIDB_PORT"
	run_sql "INSERT INTO cmek_invalid.t VALUES (3, 'orange'), (4, 'grape');" "127.0.0.1" "$UPSTREAM_INVALID_TIDB_PORT"

	ensure "$MAX_RETRIES" "check_logs_contains \"$WORK_DIR\" \"failed to decrypt master key via KMS|failed to decrypt master key via aws kms\" \"-invalid\""
	assert_table_not_synced_for_duration "cmek_invalid.t" "127.0.0.1" "$DOWNSTREAM_TIDB_PORT" 30

	run_cdc_cli changefeed -k "$UPSTREAM_INVALID_KEYSPACE" remove -c "$changefeed_id" || true
	cleanup_process "$CDC_BINARY"
}

function run() {
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"

	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	if [ "$NEXT_GEN" != "1" ]; then
		echo "skip $TEST_NAME because NEXT_GEN is disabled"
		return
	fi

	if ! start_local_kms; then
		return
	fi

	export KEYSPACE_WAIT_REGION_SPLIT=false
	export KEYSPACE_WAIT_REGION_SPLIT_TIMEOUT=1m
	export KEYSPACE_CHECK_REGION_SPLIT_INTERVAL=2s
	export KEYSPACE_DISABLE_RAW_KV_REGION_SPLIT=true
	export KEYSPACE_ENABLE_GLOBAL_SAFE_POINT_V2=true

	stop_tidb_cluster
	start_tidb_cluster --workdir "$WORK_DIR" --pd-config "$CUR/conf/pd_config.toml"
	if ! prepare_keyspaces_and_tidb; then
		if [ "${REQUIRE_ENCRYPTED_KEYSPACE_START}" = "true" ]; then
			echo "encrypted keyspace tidb startup failed and strict mode is enabled"
			exit 1
		fi
		echo "skip $TEST_NAME: encrypted keyspace tidb startup failed in current environment"
		return
	fi

	run_with_valid_kms
	run_with_invalid_kms
}

trap 'stop_local_kms; if [ -d "$WORK_DIR" ]; then stop_test "$WORK_DIR"; fi' EXIT
run "$@"
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
