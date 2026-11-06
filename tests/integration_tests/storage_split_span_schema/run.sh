#!/bin/bash
# Regression test for https://github.com/pingcap/ticdc/issues/6658.
# Move one idle span before either span has written a schema file, then write
# the moved span first. The other span must reuse its schema without losing DML.

set -euo pipefail
shopt -s nullglob

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
CHANGEFEED_ID=storage-split-span-schema
STORAGE_DIR=$WORK_DIR/storage_test
TABLE_DIR=$STORAGE_DIR/storage_split_span_schema/t
API="http://127.0.0.1:8300/api/v2/changefeeds/$CHANGEFEED_ID"

if [ "$SINK_TYPE" != "storage" ]; then
	exit 0
fi

wait_for() {
	for ((attempt = 0; attempt < 60; attempt++)); do
		if "$@"; then
			return 0
		fi
		sleep 1
	done
	echo "Timed out waiting for $*" >&2
	return 1
}

dispatcher_log() {
	grep -F '"new dispatcher created"' "$WORK_DIR/cdc$1.log" |
		grep -F "tableID: $table_id," | tail -n 1
}

captures_ready() {
	curl -fsS http://127.0.0.1:8300/api/v2/captures | jq -e '.items | length == 2' >/dev/null
}

initial_dispatchers_ready() {
	local first second
	first=$(dispatcher_log 0) || return 1
	second=$(dispatcher_log 1) || return 1
	[[ $first == *"[startTs=$start_ts]"* && $second == *"[startTs=$start_ts]"* ]]
}

checkpoint_advanced() {
	local checkpoint
	checkpoint=$(curl -fsS "$API?keyspace=$KEYSPACE_NAME" | jq -er '.checkpoint_ts') || return 1
	# Leave a full physical second between startTs values, including jq's TSO rounding.
	((checkpoint > start_ts + 262144000))
}

moved_dispatcher_ready() {
	local entry
	entry=$(dispatcher_log 0) || return 1
	[[ $entry == *"[dispatcherID=$moved_id]"* ]] || return 1
	moved_start_ts=$(echo "$entry" | sed -E 's/.*\[startTs=([0-9]+)\].*/\1/')
	[[ $moved_start_ts =~ ^[0-9]+$ ]] && ((moved_start_ts > start_ts))
}

schema_ready() {
	local files=("$TABLE_DIR/meta"/schema_*.json)
	[[ ${#files[@]} == 1 && ${files[0]} == "$TABLE_DIR/meta/schema_${moved_start_ts}_"*.json ]]
}

stored_row_ready() {
	local id=$1 dispatcher_id=$2
	local directory="$TABLE_DIR/$moved_start_ts"
	local index="$directory/meta/CDC_${dispatcher_id}.index"
	[[ -f $index ]] || return 1
	local file
	file=$(tr -d '\r\n' <"$index")
	[[ -f $directory/$file ]] || return 1
	jq -e --arg id "$id" 'select(.type == "INSERT") | .data[] | select((.id | tostring) == $id)' \
		"$directory/$file" >/dev/null
}

downstream_rows_ready() {
	local rows
	rows=$(mysql -h "$DOWN_TIDB_HOST" -P "$DOWN_TIDB_PORT" -uroot -N -B \
		-e 'SELECT GROUP_CONCAT(id ORDER BY id) FROM storage_split_span_schema.t') || return 1
	[[ $rows == "-1,1" ]]
}

run() {
	rm -rf "$WORK_DIR"
	mkdir -p "$WORK_DIR"
	SKIP_TIFLASH=1 start_tidb_cluster --workdir "$WORK_DIR"

	# Create the table before the changefeed so no DDL schema masks the bug.
	local ddl='CREATE DATABASE storage_split_span_schema; CREATE TABLE storage_split_span_schema.t (id INT PRIMARY KEY CLUSTERED);'
	run_sql "$ddl" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "$ddl" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql 'SPLIT TABLE storage_split_span_schema.t BY (0);' "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	table_id=$(get_table_id storage_split_span_schema t)

	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --logsuffix 0 \
		--addr 127.0.0.1:8300 --config "$CUR/conf/server.toml"
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --logsuffix 1 \
		--addr 127.0.0.1:8301 --config "$CUR/conf/server.toml"
	wait_for captures_ready
	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	local sink_uri="file://$STORAGE_DIR?flush-interval=1s&enable-tidb-extension=true"
	cdc_cli_changefeed create -c "$CHANGEFEED_ID" --start-ts "$start_ts" \
		--sink-uri "$sink_uri" --config "$CUR/conf/changefeed.toml"
	# The count includes the table-trigger dispatcher in addition to the two spans.
	query_dispatcher_count 127.0.0.1:8300 "$CHANGEFEED_ID" 3 60 eq
	wait_for initial_dispatchers_ready

	local first second retained_id moved_key retained_key
	first=$(dispatcher_log 0)
	second=$(dispatcher_log 1)
	retained_id=$(echo "$first" | sed -E 's/.*\[dispatcherID=([^]]+)\].*/\1/')
	moved_id=$(echo "$second" | sed -E 's/.*\[dispatcherID=([^]]+)\].*/\1/')
	local first_key second_key
	first_key=$(echo "$first" | sed -E 's/.*startKey: ([0-9a-f]+),.*/\1/')
	second_key=$(echo "$second" | sed -E 's/.*startKey: ([0-9a-f]+),.*/\1/')
	[[ $retained_id =~ ^[0-9]+$ && $moved_id =~ ^[0-9]+$ && $retained_id != "$moved_id" ]]
	[[ $first_key =~ ^[0-9a-f]+$ && $second_key =~ ^[0-9a-f]+$ && $first_key != "$second_key" ]]
	if [[ $first_key < $second_key ]]; then
		retained_key=-1
		moved_key=1
	else
		retained_key=1
		moved_key=-1
	fi

	wait_for checkpoint_advanced
	# Only the span on node 1 moves; node 0 keeps its original dispatcher.
	move_split_table_with_retry 127.0.0.1:8300 "$table_id" "$CHANGEFEED_ID" 10
	wait_for moved_dispatcher_ready
	local files=("$TABLE_DIR/meta"/schema_*.json)
	[[ ${#files[@]} == 0 ]]

	# The newer startTs must materialize the first schema before the old span writes.
	run_sql "INSERT INTO storage_split_span_schema.t VALUES ($moved_key);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	wait_for schema_ready
	wait_for stored_row_ready "$moved_key" "$moved_id"
	run_sql "INSERT INTO storage_split_span_schema.t VALUES ($retained_key);" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	wait_for stored_row_ready "$retained_key" "$retained_id"
	schema_ready

	# Consume only after both indexed files are durable, then check both records.
	cdc_storage_consumer --log-file "$WORK_DIR/consumer.log" --upstream-uri "$sink_uri" \
		--downstream-uri "mysql://root@$DOWN_TIDB_HOST:$DOWN_TIDB_PORT/?enable-ddl-ts=false" \
		--config "$CUR/conf/changefeed.toml" >"$WORK_DIR/consumer_stdout.log" 2>&1 &
	wait_for downstream_rows_ready
	cleanup_process "$CDC_BINARY"
}

trap 'stop_test "$WORK_DIR"' EXIT
run
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
