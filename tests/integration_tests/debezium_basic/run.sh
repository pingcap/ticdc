#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run_handling_modes() {
	local mode database topic sink_uri config_file diff_config
	for mode in default base64 bytes base64-url-safe hex; do
		database="debezium_${mode//-/_}"
		topic="ticdc-debezium-handling-$mode-$RANDOM"
		config_file="$WORK_DIR/handling-$mode.toml"
		diff_config="$WORK_DIR/diff-$mode.toml"
		sink_uri="kafka://127.0.0.1:9092/$topic?protocol=debezium&enable-tidb-extension=true&partition-num=1"
		cat >"$config_file" <<EOF
[filter]
rules = ['$database.*']
[sink.debezium]
decimal-handling-mode = "string"
bigint-unsigned-handling-mode = "string"
EOF
		if [ "$mode" = "base64" ]; then
			# Cover TOML configuration as well as sink URI parameters.
			echo 'binary-handling-mode = "base64"' >>"$config_file"
		elif [ "$mode" != "default" ]; then
			# The URI must override the TOML setting in both producer and consumer.
			echo 'binary-handling-mode = "base64"' >>"$config_file"
			sink_uri="$sink_uri&debezium-binary-handling-mode=$mode"
		fi
		if [ "$mode" = "hex" ]; then
			# Exercise the numeric URI overrides through the CLI/API path too.
			sed -i -e 's/decimal-handling-mode = "string"/decimal-handling-mode = "double"/' \
				-e 's/bigint-unsigned-handling-mode = "string"/bigint-unsigned-handling-mode = "long"/' "$config_file"
			sink_uri="$sink_uri&debezium-decimal-handling-mode=string&debezium-bigint-unsigned-handling-mode=string"
		fi

		cdc_cli_changefeed create -c "debezium-$mode" --sink-uri="$sink_uri" --config="$config_file"
		run_kafka_consumer "$WORK_DIR" "$sink_uri" "$config_file" "" "-$mode"
		sed "s/debezium_modes/$database/g" "$CUR/data/handling_modes.sql" >"$WORK_DIR/handling-$mode.sql"
		run_sql_file "$WORK_DIR/handling-$mode.sql" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

		# Wait for this table's four inserts, one update and one delete. Events
		# from different tables do not have a global ordering guarantee.
		kafka_dump --topic "$topic" --timeout 90s --until-table handling_modes --until-count 6 >"$WORK_DIR/handling-$mode.jsonl"
		python3 "$CUR/check_handling_modes.py" "$WORK_DIR/handling-$mode.jsonl" "$mode"
		check_table_exists "$database.handling_modes" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
		sed -e "s/test[.][?][*]/$database.*/" \
			-e "s|debezium_basic/output|debezium_basic/output-$mode|" \
			"$CUR/conf/diff_config.toml" >"$diff_config"
		check_sync_diff "$WORK_DIR" "$diff_config"
		cdc_cli_changefeed remove -c "debezium-$mode"
		cleanup_process cdc_kafka_consumer
	done
}

# use kafka-consumer with debezium decoder to sync data from kafka to mysql
function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	# clean up environment
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	# start tidb cluster
	start_tidb_cluster --workdir $WORK_DIR

	TOPIC_NAME="ticdc-debezium-basic-$RANDOM"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=debezium&enable-tidb-extension=true"

	cdc_cli_changefeed create -c debezium-basic --sink-uri="$SINK_URI" --config=$CUR/conf/changefeed.toml
	sleep 5 # wait for changefeed to start
	# determine the sink uri and run corresponding consumer
	run_kafka_consumer $WORK_DIR $SINK_URI $CUR/conf/changefeed.toml

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE test.finish_mark1 (a int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.finish_mark1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	run_sql_file $CUR/data/data_gbk.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE test.finish_mark2 (a int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.finish_mark2 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	echo "Starting build checksum checker..."
	cd $CUR/../../utils/checksum_checker
	if [ ! -f ./checksum_checker ]; then
		GO111MODULE=on go build
	fi
	# wait the dml event replicate the downstream
	sleep 10
	run_sql "CREATE TABLE test.finish_mark3 (a int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.finish_mark3 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	./checksum_checker --upstream-uri "root@tcp(${UP_TIDB_HOST}:${UP_TIDB_PORT})/" --downstream-uri "root@tcp(${DOWN_TIDB_HOST}:${DOWN_TIDB_PORT})/" --databases "test" --config="$CUR/conf/changefeed.toml"

	cdc_cli_changefeed remove -c debezium-basic
	cleanup_process cdc_kafka_consumer
	run_handling_modes

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
