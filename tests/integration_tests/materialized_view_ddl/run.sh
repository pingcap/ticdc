#!/bin/bash

set -euo pipefail

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$CUR/../_utils/test_prepare"

WORK_DIR="$OUT_DIR/$TEST_NAME"
CDC_BINARY=cdc.test
SINK_TYPE="$1"
DB_NAME=materialized_view_ddl

function prepare_mysql_sentinels() {
	run_sql "CREATE DATABASE IF NOT EXISTS ${DB_NAME};
		CREATE TABLE ${DB_NAME}.mv_incremental (id INT PRIMARY KEY, marker VARCHAR(32));
		INSERT INTO ${DB_NAME}.mv_incremental VALUES (1, 'incremental-sentinel');" \
		"$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
}

function check_mysql_sentinels() {
	run_sql "SELECT marker FROM ${DB_NAME}.mv_incremental WHERE id = 1;" \
		"$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	check_contains "incremental-sentinel"
}

function run() {
	if [ "$SINK_TYPE" != "mysql" ] && [ "$SINK_TYPE" != "kafka" ]; then
		echo "skip materialized view DDL test for ${SINK_TYPE} sink"
		return
	fi

	rm -rf "$WORK_DIR"
	mkdir -p "$WORK_DIR"
	start_tidb_cluster --workdir "$WORK_DIR"

	run_sql "CREATE DATABASE IF NOT EXISTS ${DB_NAME};" "$UP_TIDB_HOST" "$UP_TIDB_PORT"
	run_sql "CREATE DATABASE IF NOT EXISTS ${DB_NAME};" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"

	if [ "$SINK_TYPE" = "mysql" ]; then
		prepare_mysql_sentinels
	fi

	local start_ts
	start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	run_cdc_server --workdir "$WORK_DIR" --binary "$CDC_BINARY"

	local sink_uri
	local changefeed_id="materialized-view-ddl"
	if [ "$SINK_TYPE" = "kafka" ]; then
		local topic_name="materialized-view-ddl-$RANDOM"
		sink_uri="kafka://127.0.0.1:9092/${topic_name}?protocol=simple"
	else
		sink_uri="mysql://normal:123456@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/"
	fi

	cdc_cli_changefeed create \
		--start-ts="$start_ts" \
		--sink-uri="$sink_uri" \
		--changefeed-id="$changefeed_id" \
		--config="$CUR/conf/changefeed.toml"

	if [ "$SINK_TYPE" = "kafka" ]; then
		cdc_kafka_consumer \
			--upstream-uri="$sink_uri" \
			--downstream-uri="mysql://root@${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT}/?safe-mode=true&batch-dml-enable=false&enable-ddl-ts=false" \
			--upstream-tidb-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/?" \
			--config="$CUR/conf/changefeed.toml" \
			--log-file="$WORK_DIR/cdc_kafka_consumer.log" 2>&1 &
	fi

	run_sql "CREATE TABLE ${DB_NAME}.base_incremental (a INT NOT NULL PRIMARY KEY, b INT NOT NULL);
		CREATE MATERIALIZED VIEW LOG ON ${DB_NAME}.base_incremental (a, b);
		CREATE MATERIALIZED VIEW ${DB_NAME}.mv_incremental (a, s, cnt)
		AS SELECT a, SUM(b), COUNT(1) FROM ${DB_NAME}.base_incremental GROUP BY a;" \
		"$UP_TIDB_HOST" "$UP_TIDB_PORT"

	if [ "$SINK_TYPE" = "kafka" ]; then
		check_table_exists "${DB_NAME}.mv_incremental" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 200
	else
		check_mysql_sentinels
	fi

	run_sql "INSERT INTO ${DB_NAME}.base_incremental VALUES (1, 10);
		REFRESH MATERIALIZED VIEW ${DB_NAME}.mv_incremental COMPLETE OUT OF PLACE;
		DROP MATERIALIZED VIEW ${DB_NAME}.mv_incremental;
		DROP MATERIALIZED VIEW LOG ON ${DB_NAME}.base_incremental;
		CREATE TABLE ${DB_NAME}.finish_mark (id INT PRIMARY KEY);" \
		"$UP_TIDB_HOST" "$UP_TIDB_PORT"

	check_table_exists "${DB_NAME}.finish_mark" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 200
	if [ "$SINK_TYPE" = "kafka" ]; then
		check_table_not_exists "${DB_NAME}.mv_incremental" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
	else
		check_mysql_sentinels
	fi

	cleanup_process "$CDC_BINARY"
}

trap 'stop_test "$WORK_DIR"' EXIT
run
check_logs "$WORK_DIR"
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
