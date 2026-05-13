#!/bin/bash

# [DESCRIPTION]:
#   Verify that kafka-consumer syncpoint records describe a consistent upstream/downstream snapshot pair.
# [STEP]:
#   1. Create a Kafka changefeed and consume it into downstream TiDB with --enable-syncpoint.
#   2. Generate DML workload while the consumer periodically writes tidb_cdc.consumer_syncpoint_v1.
#   3. Pick the latest consumer syncpoint and compare upstream primary_ts with downstream secondary_ts.

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

DB_NAME=kafka_consumer_syncpoint
TABLE_NAME=t
CHANGEFEED_ID=kafka-consumer-syncpoint
CONSUMER_GROUP_ID=kafka-consumer-syncpoint-consumer

function deployConfig() {
	local primaryTs=$1
	local secondaryTs=$2

	cat $CUR/conf/diff_config_part1.toml >$WORK_DIR/diff_config.toml
	echo "    snapshot = \"$primaryTs\"" >>$WORK_DIR/diff_config.toml
	cat $CUR/conf/diff_config_part2.toml >>$WORK_DIR/diff_config.toml
	echo "    snapshot = \"$secondaryTs\"" >>$WORK_DIR/diff_config.toml
}

function runWorkload() {
	run_sql "CREATE DATABASE $DB_NAME;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE $DB_NAME.$TABLE_NAME(id INT PRIMARY KEY, v INT, c VARCHAR(64));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "$DB_NAME.$TABLE_NAME" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120

	for i in $(seq 1 20); do
		run_sql "INSERT INTO $DB_NAME.$TABLE_NAME VALUES ($i, $i, 'value-$i');" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		sleep 1
	done

	for i in $(seq 1 10); do
		run_sql "UPDATE $DB_NAME.$TABLE_NAME SET v = v + 100 WHERE id = $i;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		sleep 1
	done

	run_sql "CREATE TABLE $DB_NAME.finish_mark(id INT PRIMARY KEY);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "$DB_NAME.finish_mark" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
}

function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		echo "[$(date)] kafka_consumer_syncpoint only runs with kafka sink"
		return
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	run_sql "SET GLOBAL tidb_enable_external_ts_read = on;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-kafka-consumer-syncpoint-$RANDOM"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=simple"
	DOWNSTREAM_URI="mysql://root@127.0.0.1:3306/?safe-mode=true&batch-dml-enable=false&enable-ddl-ts=false"

	cdc_cli_changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml" -c "$CHANGEFEED_ID"
	sleep 5

	cdc_kafka_consumer \
		--upstream-uri "$SINK_URI" \
		--downstream-uri "$DOWNSTREAM_URI" \
		--upstream-tidb-dsn="root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/?" \
		--config="$CUR/conf/changefeed.toml" \
		--consumer-group-id="$CONSUMER_GROUP_ID" \
		--enable-syncpoint \
		--syncpoint-interval=2s \
		--syncpoint-retention=1h \
		--log-file "$WORK_DIR/cdc_kafka_consumer.log" \
		--log-level debug >>"$WORK_DIR/cdc_kafka_consumer_stdout.log" 2>&1 &

	runWorkload

	ensure 60 "mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -N -s -e \"SELECT COUNT(*) FROM tidb_cdc.consumer_syncpoint_v1 WHERE consumer_id='$CONSUMER_GROUP_ID' AND topic='$TOPIC_NAME' AND secondary_ts <> '0';\" | grep -E '^[1-9][0-9]*$'"

	syncpointRow=$(mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -N -s -e "SELECT primary_ts, secondary_ts FROM tidb_cdc.consumer_syncpoint_v1 WHERE consumer_id='$CONSUMER_GROUP_ID' AND topic='$TOPIC_NAME' AND secondary_ts <> '0' ORDER BY CAST(primary_ts AS UNSIGNED) DESC LIMIT 1;")
	primaryTs=$(echo "$syncpointRow" | awk '{print $1}')
	secondaryTs=$(echo "$syncpointRow" | awk '{print $2}')
	echo "[$(date)] check kafka consumer syncpoint primary_ts: $primaryTs, secondary_ts: $secondaryTs"

	run_sql "SET GLOBAL tidb_enable_external_ts_read = off;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	deployConfig "$primaryTs" "$secondaryTs"
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml 60
	check_sync_diff $WORK_DIR $CUR/conf/diff_config_final.toml 60

	killall cdc_kafka_consumer || true
	cleanup_process $CDC_BINARY
}

trap 'killall cdc_kafka_consumer || true; stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
