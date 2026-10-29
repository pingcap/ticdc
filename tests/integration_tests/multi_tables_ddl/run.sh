#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# start the s3 server
export MINIO_ACCESS_KEY=cdcs3accesskey
export MINIO_SECRET_KEY=cdcs3secretkey
export MINIO_BROWSER=off
export AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$MINIO_SECRET_KEY
export S3_ENDPOINT=127.0.0.1:24927
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
pkill -9 minio || true
bin/minio server --address $S3_ENDPOINT "$WORK_DIR/s3" &
MINIO_PID=$!
check_minio $S3_ENDPOINT

stop_minio() {
	if [ $MINIO_PID -ne 0 ]; then
		kill -2 $MINIO_PID || true
	fi
}

stop() {
	stop_minio
	stop_test $WORK_DIR
}

function test_system_schema_rename() {
	local rename_type
	local marker=0
	for rename_type in single multiple; do
		local changefeed_id="system-rename-$rename_type"
		local rename_start_ts
		rename_start_ts=$(run_cdc_cli_tso_query $UP_PD_HOST_1 $UP_PD_PORT_1)
		cdc_cli_changefeed create -c=$changefeed_id --start-ts=$rename_start_ts \
			--sink-uri="$SINK_URI" --config="$CUR/conf/system-rename.toml"

		run_sql "CREATE TABLE mysql.ticdc_system_${rename_type}_a (id INT PRIMARY KEY);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		if [ "$rename_type" == "single" ]; then
			run_sql "RENAME TABLE mysql.ticdc_system_single_a TO system_schema_rename.selected_single;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		else
			run_sql "CREATE TABLE mysql.ticdc_system_multiple_b (id INT PRIMARY KEY) PARTITION BY HASH(id) PARTITIONS 2;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
			run_sql "RENAME TABLE mysql.ticdc_system_multiple_a TO system_schema_rename.selected_multiple_a,
				mysql.ticdc_system_multiple_b TO system_schema_rename.selected_multiple_b;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		fi
		do_retry 20 2 check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" $changefeed_id "failed" "ErrSyncRenameTableFailed" ""
		cdc_cli_changefeed remove -c=$changefeed_id

		# A feed that excludes the renamed tables must continue replicating data.
		marker=$((marker + 1))
		run_sql "INSERT INTO multi_tables_ddl_test.finish_mark VALUES ($marker);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
		ensure 10 "run_sql 'SELECT COUNT(*) AS marker_count FROM multi_tables_ddl_test.finish_mark WHERE id = $marker;' \
			${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_contains 'marker_count: 1'"
		check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" $cf_normal "normal" "null" ""
		check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" $cf_err1 "normal" "null" ""
		run_sql "DROP TABLE IF EXISTS system_schema_rename.selected_single,
			system_schema_rename.selected_multiple_a, system_schema_rename.selected_multiple_b;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
}

function run() {
	if [ "$SINK_TYPE" == "storage" ]; then
		return
	fi
	# TODO(dongmen): enable pulsar in the future.
	if [ "$SINK_TYPE" == "pulsar" ]; then
		exit 0
	fi

	start_tidb_cluster --workdir $WORK_DIR

	bin/minio server --address $S3_ENDPOINT "$WORK_DIR/s3" &
	MINIO_PID=$!
	i=0
	while ! curl -o /dev/null -v -s "http://$S3_ENDPOINT/"; do
		i=$(($i + 1))
		if [ $i -gt 30 ]; then
			echo 'Failed to start minio'
			exit 1
		fi
		sleep 2
	done
	s3cmd --access_key=$MINIO_ACCESS_KEY --secret_key=$MINIO_SECRET_KEY --host=$S3_ENDPOINT --host-bucket=$S3_ENDPOINT --no-ssl mb s3://logbucket

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query $UP_PD_HOST_1 $UP_PD_PORT_1)

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	# Regression for #2983: system-table rename must be safe even without a changefeed.
	run_sql "CREATE DATABASE system_schema_rename;
		CREATE TABLE mysql.ticdc_system_no_feed (id INT PRIMARY KEY);
		RENAME TABLE mysql.ticdc_system_no_feed TO system_schema_rename.no_feed;
		ALTER TABLE system_schema_rename.no_feed ADD COLUMN value INT;
		DROP TABLE system_schema_rename.no_feed;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	TOPIC_NAME_1="ticdc-multi-tables-ddl-test-normal-$RANDOM"
	TOPIC_NAME_2="ticdc-multi-tables-ddl-test-error-1-$RANDOM"
	TOPIC_NAME_3="ticdc-multi-tables-ddl-test-error-2-$RANDOM"

	case $SINK_TYPE in
	*) ;;
	esac

	cf_normal="test-normal"
	cf_err1="test-error-1"
	cf_err2="test-error-2"

	case $SINK_TYPE in
	"kafka")
		SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME_1?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760"
		cdc_cli_changefeed create -c=$cf_normal --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/normal.toml"

		SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME_2?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760"
		cdc_cli_changefeed create -c=$cf_err1 --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/error-1.toml"

		SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME_3?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760"
		cdc_cli_changefeed create -c=$cf_err2 --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/error-2.toml"

		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME_1?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" "$CUR/conf/normal.toml"
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME_2?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" "$CUR/conf/error-1.toml"
		run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME_3?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" "$CUR/conf/error-2.toml"
		;;
	*)
		SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
		cdc_cli_changefeed create -c=$cf_normal --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/normal.toml"
		cdc_cli_changefeed create -c=$cf_err1 --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/error-1.toml"
		cdc_cli_changefeed create -c=$cf_err2 --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/error-2.toml"
		;;
	esac

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.t55 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.t66 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.t7 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.t88 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.rename_mix_normal_1_done ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.rename_mix_part_1_done ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.rename_mix_normal_2_done ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists multi_tables_ddl_test.rename_mix_part_2_done ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists multi_tables_ddl_test.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	echo "check table exists success"

	# changefeed test-error will not report an error, "multi_tables_ddl_test.t555 to multi_tables_ddl_test.t55" part will be skipped.
	run_sql "rename table multi_tables_ddl_test.t7 to multi_tables_ddl_test.t77, multi_tables_ddl_test.t555 to multi_tables_ddl_test.t55;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" $cf_normal "normal" "null" ""
	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" $cf_err1 "normal" "null" ""
	do_retry 10 2 check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" $cf_err2 "failed" "ErrSyncRenameTableFailed" ""

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 60

	test_system_schema_rename

	cleanup_process $CDC_BINARY
}

trap stop EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
