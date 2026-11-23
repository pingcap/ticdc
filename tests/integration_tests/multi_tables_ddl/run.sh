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

function check_view_result() {
	ensure 20 "run_sql \"$1\" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_contains \"$2\""
}

function test_view_ddls() {
	# Check progress after excluded view DDLs, then wait for each downstream object.
	ensure 30 "run_sql 'SELECT COUNT(*) AS marker_count FROM view_ddl_test.finish_mark WHERE id = 100;' \
		${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_contains 'marker_count: 1'"
	for view in single_done mixed_1_done mixed_2_done mixed_3_done mixed_4_done ignored_view_5 ignored_view_6; do
		ensure 10 "run_sql 'SELECT COUNT(*) AS view_count FROM view_ddl_test.rename_view_$view WHERE id = 7;' \
			${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_contains 'view_count: 1'"
	done
	for table in table_1_done table_2_done table_5_done table_6_done; do
		ensure 10 "run_sql 'SELECT COUNT(*) AS row_count FROM view_ddl_test.rename_view_$table WHERE id IN (1, 2);' \
			${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_contains 'row_count: 2'"
	done
	for table in ignored_table_3 ignored_table_4; do
		ensure 10 "run_sql 'SELECT COUNT(*) AS row_count FROM view_ddl_test.rename_view_$table WHERE id = 1;' \
			${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_contains 'row_count: 1'"
	done
	run_sql "SELECT COUNT(*) AS stale_count FROM information_schema.tables
		WHERE table_schema = 'view_ddl_test' AND table_name IN (
		'rename_view_single', 'rename_view_mixed_1', 'rename_view_mixed_2',
		'rename_view_mixed_3', 'rename_view_mixed_4', 'rename_view_table_1',
		'rename_view_table_2', 'rename_view_table_5', 'rename_view_table_6',
		'rename_view_ignored_table_3_done', 'rename_view_ignored_table_4_done',
		'rename_view_ignored_view_5_done', 'rename_view_ignored_view_6_done');" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains 'stale_count: 0'
	run_sql "SELECT COUNT(*) AS excluded_count FROM information_schema.schemata WHERE schema_name = 'unselected_db';" \
		${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains 'excluded_count: 0'

	run_sql "DROP VIEW view_ddl_test.rename_view_single_done;
		INSERT INTO view_ddl_test.finish_mark VALUES (101);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	ensure 30 "run_sql 'SELECT COUNT(*) AS marker_count FROM view_ddl_test.finish_mark WHERE id = 101;' \
		${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_contains 'marker_count: 1'"
	check_table_not_exists view_ddl_test.rename_view_single_done ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# Views present at initialization must be cleaned up when dropped or replaced.
	run_sql "DROP VIEW view_ddl_test.snapshot_drop;
		CREATE TABLE view_ddl_test.snapshot_drop(id INT PRIMARY KEY);
		INSERT INTO view_ddl_test.snapshot_drop VALUES (1);
		CREATE TABLE view_ddl_test.snapshot_copy LIKE view_ddl_test.snapshot_drop;
		INSERT INTO view_ddl_test.snapshot_copy VALUES (2);
		CREATE OR REPLACE VIEW view_ddl_test.snapshot_replace AS SELECT id, val + 100 AS val FROM view_ddl_test.base;
		RENAME TABLE view_ddl_test.snapshot_rename TO view_ddl_test.snapshot_renamed;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_view_result "SELECT id FROM view_ddl_test.snapshot_copy;" "id: 2"
	check_view_result "SELECT val FROM view_ddl_test.snapshot_replace WHERE id = 1;" "val: 110"
	check_view_result "SELECT val FROM view_ddl_test.snapshot_renamed WHERE id = 1;" "val: 10"
	check_table_not_exists view_ddl_test.snapshot_rename ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# Verify each definition before the next DDL so a missed replacement cannot pass.
	run_sql "CREATE VIEW view_ddl_test.incremental AS SELECT id, val FROM view_ddl_test.base;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_view_result "SELECT val FROM view_ddl_test.incremental WHERE id = 1;" "val: 10"
	run_sql "CREATE OR REPLACE VIEW view_ddl_test.incremental AS SELECT id, val + 200 AS val FROM view_ddl_test.base;" \
		${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_view_result "SELECT val FROM view_ddl_test.incremental WHERE id = 1;" "val: 210"
	run_sql "RENAME TABLE view_ddl_test.incremental TO view_ddl_test.incremental_renamed;
		INSERT INTO view_ddl_test.base VALUES (2, 20);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_view_result "SELECT val FROM view_ddl_test.incremental_renamed WHERE id = 2;" "val: 220"
	check_table_not_exists view_ddl_test.incremental ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "DROP VIEW view_ddl_test.incremental_renamed, view_ddl_test.snapshot_replace,
		view_ddl_test.snapshot_renamed;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	for view in incremental_renamed snapshot_replace snapshot_renamed; do
		check_table_not_exists view_ddl_test.$view ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	done

	# Database recovery must restore view definitions without scheduling view IDs.
	run_sql "CREATE DATABASE view_recover_test;
		CREATE TABLE view_recover_test.base(id INT PRIMARY KEY, val INT);
		INSERT INTO view_recover_test.base VALUES (1, 10);
		CREATE VIEW view_recover_test.v AS SELECT id, val + 100 AS val FROM view_recover_test.base;" \
		${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_view_result "SELECT val FROM view_recover_test.v WHERE id = 1;" "val: 110"
	run_sql "DROP DATABASE view_recover_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_db_not_exists view_recover_test ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "FLASHBACK DATABASE view_recover_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_view_result "SELECT val FROM view_recover_test.v WHERE id = 1;" "val: 110"
	run_sql "INSERT INTO view_recover_test.base VALUES (2, 20);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_view_result "SELECT val FROM view_recover_test.v WHERE id = 2;" "val: 120"

	local view_ids
	view_ids=$(mysql -uroot -h${UP_TIDB_HOST} -P${UP_TIDB_PORT} -N -B -e \
		"SELECT JSON_ARRAYAGG(TIDB_TABLE_ID) FROM information_schema.tables
		WHERE table_schema IN ('view_ddl_test', 'view_recover_test') AND table_type = 'VIEW';")
	curl -fsS "http://${CDC_HOST}:${CDC_PORT}/api/v2/changefeeds/$cf_normal/tables?keyspace=${KEYSPACE_NAME}" |
		jq -e --argjson views "$view_ids" \
			'[.items[].table_ids[]] as $tables | all($views[]; . as $id | $tables | index($id) | not)'

	run_sql "DROP DATABASE view_recover_test; DROP DATABASE view_ddl_test;
		INSERT INTO multi_tables_ddl_test.finish_mark VALUES (1000);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_db_not_exists view_recover_test ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_db_not_exists view_ddl_test ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_view_result "SELECT COUNT(*) AS marker_count FROM multi_tables_ddl_test.finish_mark WHERE id = 1000;" "marker_count: 1"
	check_changefeed_state "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}" $cf_normal "normal" "null" ""
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

	# Seed and mirror views before the start TS to exercise snapshot metadata.
	local view_schema="CREATE DATABASE view_ddl_test;
		CREATE TABLE view_ddl_test.base(id INT PRIMARY KEY, val INT);
		INSERT INTO view_ddl_test.base VALUES (1, 10);
		CREATE VIEW view_ddl_test.snapshot_drop AS SELECT id, val FROM view_ddl_test.base;
		CREATE VIEW view_ddl_test.snapshot_replace AS SELECT id, val FROM view_ddl_test.base;
		CREATE VIEW view_ddl_test.snapshot_rename AS SELECT id, val FROM view_ddl_test.base;"
	run_sql "$view_schema" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "$view_schema" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

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
	"storage" | "pulsar")
		if [ "$SINK_TYPE" == "pulsar" ]; then
			run_pulsar_cluster $WORK_DIR normal
		fi
		local index topic changefeed config
		for index in 1 2 3; do
			case $index in
			1)
				topic=$TOPIC_NAME_1
				changefeed=$cf_normal
				config="$CUR/conf/normal.toml"
				;;
			2)
				topic=$TOPIC_NAME_2
				changefeed=$cf_err1
				config="$CUR/conf/error-1.toml"
				;;
			3)
				topic=$TOPIC_NAME_3
				changefeed=$cf_err2
				config="$CUR/conf/error-2.toml"
				;;
			esac
			if [ "$SINK_TYPE" == "storage" ]; then
				SINK_URI="file://$WORK_DIR/storage_test/$topic?flush-interval=5s&protocol=csv"
			else
				SINK_URI="pulsar://127.0.0.1:6650/$topic?protocol=canal-json&enable-tidb-extension=true"
			fi
			cdc_cli_changefeed create -c=$changefeed --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$config"
			if [ "$SINK_TYPE" == "storage" ]; then
				run_storage_consumer $WORK_DIR "$SINK_URI" "$config" $index
			else
				log_suffix=$index run_pulsar_consumer --upstream-uri "$SINK_URI" --config "$config"
			fi
		done
		;;
	*)
		# FLASHBACK DATABASE in the view lifecycle test requires elevated privileges.
		SINK_URI="mysql://root@127.0.0.1:3306/"
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
	test_view_ddls

	cleanup_process $CDC_BINARY
}

trap stop EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
