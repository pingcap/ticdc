#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

if [ "$SINK_TYPE" != "mysql" ]; then
	exit 0
fi

API="http://${CDC_HOST}:${CDC_PORT}/api/v2/changefeeds"
PD_ADDR="http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"
SINK_URI="mysql://normal:123456@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"

# Keep TSO values as decimal strings: jq arithmetic can round uint64 values.
checkpoint() {
	grep -oE '"checkpoint_ts"[[:space:]]*:[[:space:]]*[0-9]+' "$1" | grep -oE '[0-9]+$'
}

assert_paused() {
	local id=$1
	curl -sf "$API/$id?keyspace=$KEYSPACE_NAME" -o "$WORK_DIR/$id.json"
	jq -e '.state == "stopped" and (.maintainer_addr // "") == "" and ((.task_status // []) | length) == 0' "$WORK_DIR/$id.json"
	[ "$(checkpoint "$WORK_DIR/$id.json")" = "$START_TS" ]
	run_sql "SELECT count(*) AS row_count FROM create_paused.$id" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "row_count: 0"
}

# Check the coordinator-owned service safepoint, then ensure temporary creation
# entries have been removed. This proves GC protection was handed over.
assert_gc_handoff() {
	local prefix="/pd/$PD_CLUSTER_ID/gc/safe_point/service/ticdc-default-"
	local service_key value
	service_key=$(ETCDCTL_API=3 etcdctl --endpoints="$PD_ADDR" get "$prefix" --prefix --keys-only | grep -E '/ticdc-default-[0-9]+$')
	[ -n "$service_key" ] || return 1
	value=$(ETCDCTL_API=3 etcdctl --endpoints="$PD_ADDR" get "$service_key" --print-value-only)
	[ -n "$value" ] || return 1
	local safepoint
	safepoint=$(echo "$value" | grep -oE '"safe_point":[0-9]+' | grep -oE '[0-9]+$')
	[ "$safepoint" = "$((START_TS - 1))" ] || return 1
	value=$(ETCDCTL_API=3 etcdctl --endpoints="$PD_ADDR" get "${service_key}-creating-" --prefix --keys-only)
	[ -z "$value" ]
}

expect_invalid() {
	local id=$1 fields=$2 error=$3
	local status
	status=$(curl -sS -o "$WORK_DIR/error.json" -w '%{http_code}' -X POST "$API?keyspace=$KEYSPACE_NAME" \
		-H 'Content-Type: application/json' -d "{\"changefeed_id\":\"$id\",\"pause\":true,$fields}")
	[ "$status" -ge 400 ]
	grep -q "$error" "$WORK_DIR/error.json"
	status=$(curl -sS -o /dev/null -w '%{http_code}' "$API/$id?keyspace=$KEYSPACE_NAME")
	[ "$status" = 404 ]
}

run() {
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	start_tidb_cluster --workdir "$WORK_DIR"
	export GO_FAILPOINTS='github.com/pingcap/ticdc/coordinator/InjectUpdateGCTickerInterval=return(5)'
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"

	# Pre-create empty tables on both sides. Writes occur after the chosen TSO
	# and before creation, so resume must retain the original start position.
	for port in "$UP_TIDB_PORT" "$DOWN_TIDB_PORT"; do
		run_sql "CREATE DATABASE create_paused" 127.0.0.1 "$port"
		for id in api cli omitted explicit legacy; do
			run_sql "CREATE TABLE create_paused.$id (id INT PRIMARY KEY, value INT)" 127.0.0.1 "$port"
		done
	done
	START_TS=$(run_cdc_cli tso query --pd="$PD_ADDR")
	for id in api cli omitted explicit legacy; do
		run_sql "INSERT INTO create_paused.$id VALUES (1, 10)" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	done

	curl -sf -X POST "$API?keyspace=$KEYSPACE_NAME" -H 'Content-Type: application/json' \
		-d "{\"changefeed_id\":\"api\",
\"pause\":true,
\"start_ts\":$START_TS,
\"sink_uri\":\"$SINK_URI\",
\"replica_config\":{\"filter\":{\"rules\":[\"create_paused.api\"]}}}" \
		-o "$WORK_DIR/create.json"
	jq -e '.state == "stopped"' "$WORK_DIR/create.json"
	[ "$(checkpoint "$WORK_DIR/create.json")" = "$START_TS" ]
	cdc_cli_changefeed create --changefeed-id=cli --start-ts="$START_TS" --sink-uri="$SINK_URI" --config="$CUR/conf/paused.toml"

	# API omission and explicit false, plus legacy CLI TOML, keep auto-starting.
	for id in omitted explicit; do
		local pause_field=""
		if [ "$id" = explicit ]; then pause_field='"pause":false,'; fi
		curl -sf -X POST "$API?keyspace=$KEYSPACE_NAME" -H 'Content-Type: application/json' \
			-d "{\"changefeed_id\":\"$id\",
$pause_field\"start_ts\":$START_TS,
\"sink_uri\":\"$SINK_URI\",
\"replica_config\":{\"filter\":{\"rules\":[\"create_paused.$id\"]}}}"
	done
	cdc_cli_changefeed create --changefeed-id=legacy --start-ts="$START_TS" --sink-uri="$SINK_URI" --config="$CUR/conf/legacy.toml"
	for id in omitted explicit legacy; do
		ensure 30 check_changefeed_state "$PD_ADDR" "$id" normal null ""
	done

	expect_invalid future "\"sink_uri\":\"$SINK_URI\",\"start_ts\":18446744073709551615" ErrAPIInvalidParam
	expect_invalid target "\"sink_uri\":\"$SINK_URI\",\"start_ts\":$START_TS,\"target_ts\":$START_TS" ErrTargetTsBeforeStartTs
	expect_invalid sink '"sink_uri":""' ErrSinkURIInvalid

	# Observe several scheduler ticks; an immediate query alone can miss startup.
	for i in $(seq 1 10); do
		assert_paused api
		assert_paused cli
		sleep 1
	done
	# The classic PD service safepoint API does not apply to next-gen barriers.
	if [ "$NEXT_GEN" = 0 ]; then
		PD_CLUSTER_ID=$(curl -sf "$PD_ADDR/pd/api/v1/cluster" | grep -oE '"id"[[:space:]]*:[[:space:]]*[0-9]+' | grep -oE '[0-9]+$')
		export PD_ADDR PD_CLUSTER_ID START_TS
		export -f assert_gc_handoff
		ensure 30 assert_gc_handoff
	fi

	cleanup_process "$CDC_BINARY"
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"
	ensure 30 check_changefeed_state "$PD_ADDR" api stopped null ""
	for i in $(seq 1 5); do
		assert_paused api
		assert_paused cli
		sleep 1
	done
	for id in api cli; do
		run_sql "INSERT INTO create_paused.$id VALUES (2, 20)" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
		cdc_cli_changefeed resume --changefeed-id="$id"
		ensure 30 check_changefeed_state "$PD_ADDR" "$id" normal null ""
	done
	check_sync_diff "$WORK_DIR" "$CUR/conf/diff_config.toml"
	cleanup_process "$CDC_BINARY"
}

trap 'stop_test $WORK_DIR' EXIT
run
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
