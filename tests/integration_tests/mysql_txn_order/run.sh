#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"
WORK_DIR="$OUT_DIR/$TEST_NAME"
CDC_BINARY=cdc.test
SINK_TYPE=$1

function check_partition_error() {
	cdc_cli_changefeed query -c "$1" >"$WORK_DIR/$1-status.json"
	grep -q 'txn-order-tables does not support partitioned table' "$WORK_DIR/$1-status.json"
}

function check_stopped() {
	cdc_cli_changefeed query -c update-order >"$WORK_DIR/update-status.json"
	grep -q '"state": "stopped"' "$WORK_DIR/update-status.json"
}

export -f check_partition_error check_stopped
export WORK_DIR

function run() {
	if [ "$SINK_TYPE" != mysql ]; then
		return
	fi
	rm -rf "$WORK_DIR" && mkdir -p "$WORK_DIR"
	start_tidb_cluster --workdir "$WORK_DIR"
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY" --cluster-id "$KEYSPACE_NAME"
	go build -o "$WORK_DIR/check_order" "$CUR/main.go"
	for port in "$UP_TIDB_PORT" "$DOWN_TIDB_PORT"; do
		run_sql 'CREATE DATABASE mysql_txn_order;
CREATE TABLE mysql_txn_order.t (id INT PRIMARY KEY, v INT NOT NULL);
INSERT INTO mysql_txn_order.t VALUES (1,0),(2,0);' 127.0.0.1 "$port"
	done
	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	SINK_URI="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/?worker-count=4&max-txn-row=64&safe-mode=false&batch-dml-enable=false"
	cdc_cli_changefeed create -c ordered --start-ts="$start_ts" --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"
	curl -fsS "http://127.0.0.1:8300/api/v2/changefeeds/ordered?keyspace=$KEYSPACE_NAME" >"$WORK_DIR/config.json"
	jq -e '.config.sink.mysql_config.txn_order_tables | index("mysql_txn_order.t") != null' "$WORK_DIR/config.json"
	jq -e '.config.scheduler.enable_table_across_nodes == false' "$WORK_DIR/config.json"
	"$WORK_DIR/check_order" --upstream "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/mysql_txn_order" \
		--downstream "root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/mysql_txn_order"

	# A partitioned table is allowed until its name enters the ordered set.
	run_sql 'CREATE TABLE mysql_txn_order.unordered_p (id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2;' "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	check_table_exists mysql_txn_order.unordered_p "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	run_sql 'RENAME TABLE mysql_txn_order.unordered_p TO mysql_txn_order.ordered_p;' "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	ensure 30 check_partition_error ordered

	if cdc_cli_changefeed create -c invalid-partition --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml" >"$WORK_DIR/create.log" 2>&1; then
		echo 'ordered partitioned table was accepted at creation'
		exit 1
	fi
	grep -q 'txn-order-tables does not support partitioned table' "$WORK_DIR/create.log"

	sed '/txn-order-tables/d' "$CUR/conf/changefeed.toml" >"$WORK_DIR/unordered.toml"
	cdc_cli_changefeed create -c update-order --sink-uri="$SINK_URI" --config="$WORK_DIR/unordered.toml"
	cdc_cli_changefeed pause -c update-order
	ensure 30 check_stopped
	if cdc_cli_changefeed update -c update-order --no-confirm --config="$CUR/conf/changefeed.toml" >"$WORK_DIR/update.log" 2>&1; then
		echo 'ordered partitioned table was accepted during update'
		exit 1
	fi
	grep -q 'txn-order-tables does not support partitioned table' "$WORK_DIR/update.log"
}

trap 'stop_test "$WORK_DIR"' EXIT
run "$@"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
