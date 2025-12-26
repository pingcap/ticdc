#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

CDC_ADDRS=("127.0.0.1:8300" "127.0.0.1:8301" "127.0.0.1:8302")
FAILPOINT_NAME="github.com/pingcap/ticdc/downstreamadapter/dispatcher/NotReadyToCloseDispatcher"

function get_capture_id_by_addr() {
	local api_addr=$1
	local target_addr=$2
	curl -s "http://${api_addr}/api/v2/captures" | jq -r --arg addr "$target_addr" '.items[] | select(.address==$addr) | .id' | head -n1
}

function get_table_node_id() {
	local api_addr=$1
	local changefeed_id=$2
	local table_id=$3
	curl -s "http://${api_addr}/api/v2/changefeeds/${changefeed_id}/tables?keyspace=$KEYSPACE_NAME" |
		jq -r --argjson tid "$table_id" '.items[] | select(.table_ids | index($tid)) | .node_id' | head -n1
}

function get_maintainer_addr() {
	local api_addr=$1
	local changefeed_id=$2
	curl -s "http://${api_addr}/api/v2/changefeeds/${changefeed_id}?keyspace=$KEYSPACE_NAME" | jq -r '.maintainer_addr'
}

function wait_for_table_on_addr() {
	local api_addr=$1
	local changefeed_id=$2
	local table_id=$3
	local target_addr=$4
	for ((i = 0; i < 30; i++)); do
		local target_id
		target_id=$(get_capture_id_by_addr "$api_addr" "$target_addr")
		if [ -z "$target_id" ] || [ "$target_id" == "null" ]; then
			sleep 2
			continue
		fi
		local node_id
		node_id=$(get_table_node_id "$api_addr" "$changefeed_id" "$table_id")
		if [ "$node_id" == "$target_id" ]; then
			return 0
		fi
		sleep 2
	done
	echo "table $table_id not moved to $target_addr" >&2
	return 1
}

function wait_for_maintainer_move() {
	local api_addr=$1
	local changefeed_id=$2
	local old_addr=$3
	for ((i = 0; i < 30; i++)); do
		local new_addr
		new_addr=$(get_maintainer_addr "$api_addr" "$changefeed_id")
		if [ -n "$new_addr" ] && [ "$new_addr" != "null" ] && [ "$new_addr" != "$old_addr" ]; then
			echo "$new_addr"
			return 0
		fi
		sleep 2
	done
	echo "maintainer did not move from $old_addr" >&2
	return 1
}

function pick_addr_excluding() {
	local exclude1=$1
	local exclude2=$2
	for addr in "${CDC_ADDRS[@]}"; do
		if [ "$addr" != "$exclude1" ] && [ "$addr" != "$exclude2" ]; then
			echo "$addr"
			return 0
		fi
	done
	return 1
}

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --pd $pd_addr --logsuffix 1 --addr "127.0.0.1:8300"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --pd $pd_addr --logsuffix 2 --addr "127.0.0.1:8301"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --pd $pd_addr --logsuffix 3 --addr "127.0.0.1:8302"

	TOPIC_NAME="ticdc-move-table-maintainer-failover-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac
	changefeed_id=$(cdc_cli_changefeed create --pd=$pd_addr --start-ts=$start_ts --sink-uri="$SINK_URI" | grep '^ID:' | head -n1 | awk '{print $2}')
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	run_sql "CREATE DATABASE move_table_maintainer_failover;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE move_table_maintainer_failover.t1(id INT PRIMARY KEY, val VARCHAR(20));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO move_table_maintainer_failover.t1 VALUES (1, 'a'), (2, 'b');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_table_exists "move_table_maintainer_failover.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	table_id=$(get_table_id "move_table_maintainer_failover" "t1")

	api_addr=${CDC_ADDRS[0]}
	maintainer_addr=$(get_maintainer_addr "$api_addr" "$changefeed_id")
	if [ -z "$maintainer_addr" ] || [ "$maintainer_addr" == "null" ]; then
		echo "failed to get maintainer address" >&2
		exit 1
	fi

	origin_addr=$(pick_addr_excluding "$maintainer_addr" "")
	target_addr=$(pick_addr_excluding "$maintainer_addr" "$origin_addr")
	if [ -z "$origin_addr" ] || [ -z "$target_addr" ]; then
		echo "failed to select origin or target addr" >&2
		exit 1
	fi

	api_addr=$origin_addr
	origin_id=$(get_capture_id_by_addr "$api_addr" "$origin_addr")
	current_node_id=$(get_table_node_id "$api_addr" "$changefeed_id" "$table_id")
	if [ "$current_node_id" != "$origin_id" ]; then
		move_table_with_retry "$origin_addr" $table_id "$changefeed_id" 10
		wait_for_table_on_addr "$api_addr" "$changefeed_id" "$table_id" "$origin_addr"
	fi

	enable_failpoint --addr "$origin_addr" --name "$FAILPOINT_NAME" --expr "return(true)"

	move_table_with_retry "$target_addr" $table_id "$changefeed_id" 10
	wait_for_table_on_addr "$api_addr" "$changefeed_id" "$table_id" "$origin_addr"

	maintainer_host=${maintainer_addr%:*}
	maintainer_port=${maintainer_addr#*:}
	maintainer_pid=$(get_cdc_pid "$maintainer_host" "$maintainer_port")
	kill_cdc_pid "$maintainer_pid"
	new_maintainer_addr=$(wait_for_maintainer_move "$api_addr" "$changefeed_id" "$maintainer_addr")
	echo "maintainer moved to $new_maintainer_addr"

	disable_failpoint --addr "$origin_addr" --name "$FAILPOINT_NAME"

	wait_for_table_on_addr "$api_addr" "$changefeed_id" "$table_id" "$target_addr"

	run_sql "ALTER TABLE move_table_maintainer_failover.t1 ADD COLUMN c2 INT DEFAULT 0;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "UPDATE move_table_maintainer_failover.t1 SET c2 = 1 WHERE id = 1;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO move_table_maintainer_failover.t1 VALUES (3, 'c', 2);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run "$@"
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
