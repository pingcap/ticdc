#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run_with_fast_create_table() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	run_sql "set global tidb_enable_fast_create_table=on" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_cdc_server_with_guard --max-restarts 3 --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-batch-add-table-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	local start_ts
	local retry_table
	if [ "$SINK_TYPE" == "mysql" ]; then
		# Generate the batch before starting replication so its first table can
		# already exist downstream when the original batch is replayed.
		start_ts=$(run_cdc_cli_tso_query "$UP_PD_HOST_1" "$UP_PD_PORT_1")
	else
		cdc_cli_changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"
	fi
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" "$CUR/conf/changefeed.toml" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "$CUR/conf/changefeed.toml" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI --config "$CUR/conf/changefeed.toml" ;;
	esac

	## to generate batch create ddl. In changefeed.toml, we filter test.t_1
	for ((i = 1; i <= 100; i++)); do
		mysql -h ${UP_TIDB_HOST} -P ${UP_TIDB_PORT} -u root -D "test" -e "create table t_$i (a int primary key , b int)" &
	done

	for ((i = 1; i <= 100; i++)); do
		check_table_exists test.t_$i ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	if [ "$SINK_TYPE" == "mysql" ]; then
		# Require a real ActionCreateTables job, excluding the filtered table.
		local batch_query
		batch_query=$(mysql -h "$UP_TIDB_HOST" -P "$UP_TIDB_PORT" -u root -N -B --raw -e \
			"SELECT QUERY FROM information_schema.ddl_jobs
			 WHERE DB_NAME = 'test' AND JOB_TYPE = 'create tables'
			 AND QUERY NOT REGEXP 't_1([^0-9]|$)'
			 ORDER BY JOB_ID LIMIT 1;")
		if [ -z "$batch_query" ]; then
			echo "expected a batch CREATE TABLE job without the filtered table"
			exit 1
		fi
		printf '%s\n' "$batch_query" >"$WORK_DIR/batch_create_retry.log"
		local first_query=${batch_query%%;*}
		local remaining_query=${batch_query#*;}
		if [[ "$remaining_query" =~ (t_[0-9]+) ]]; then
			retry_table=${BASH_REMATCH[1]}
		else
			echo "expected another CREATE TABLE after the first statement"
			exit 1
		fi
		run_sql "USE test; $first_query;" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT"
		cdc_cli_changefeed create --start-ts="$start_ts" --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"
	fi

	for ((i = 1; i <= 100; i++)); do
		mysql -h ${UP_TIDB_HOST} -P ${UP_TIDB_PORT} -u root -D "test" -e "insert into t_$i values(1,2)"
	done

	if [ "$SINK_TYPE" == "mysql" ]; then
		# Error 1050 for the first CREATE must not skip this later CREATE or its DML.
		check_table_exists "test.$retry_table" "$DOWN_TIDB_HOST" "$DOWN_TIDB_PORT" 30
		ensure 10 "run_sql 'SELECT COUNT(*) AS replicated_rows FROM test.$retry_table WHERE a = 1 AND b = 2;' \
			$DOWN_TIDB_HOST $DOWN_TIDB_PORT && check_contains 'replicated_rows: 1'"
		check_logs_contains "$WORK_DIR" "Execute DDL failed, but error can be ignored"
	fi

	check_table_exists test.t_100 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists batch_add_table.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	# check the ddl of this table is skipped
	check_table_not_exists test.t_1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_cdc_server_guard --workdir "$WORK_DIR"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300
	check_cdc_server_guard --workdir "$WORK_DIR"

	stop_cdc_server_guards
	cleanup_process $CDC_BINARY
}

function run_without_fast_create_table() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	run_sql "set global tidb_enable_fast_create_table=off" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_cdc_server_with_guard --max-restarts 3 --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-batch-add-table-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	cdc_cli_changefeed create --sink-uri="$SINK_URI"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists batch_add_table.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_cdc_server_guard --workdir "$WORK_DIR"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300
	check_cdc_server_guard --workdir "$WORK_DIR"

	stop_cdc_server_guards
	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run_without_fast_create_table $*
stop_tidb_cluster
run_with_fast_create_table $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
