#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function wait_ignore_sql_ddl() {
	local marker=$1
	local target_ts
	target_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
	ensure 20 "cdc_cli_changefeed query --server='127.0.0.1:8300' --changefeed-id=event-filter |
		grep -v 'Command to ticdc' | jq -e '.checkpoint_tso > $target_ts'"
	# Insert the marker after the sink has handled the DDLs, then wait for MQ and
	# storage consumers to apply the preceding events before checking absences.
	run_sql "INSERT INTO event_filter.sql_query_barrier VALUES ($marker);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	ensure 20 "run_sql 'SELECT COUNT(*) AS marker_count FROM event_filter.sql_query_barrier WHERE id = $marker;' \
		${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_contains 'marker_count: 1'"
}

function test_rewritten_query_filter() {
	run_sql_file $CUR/data/prepare_ignore_sql_rewrite.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	wait_ignore_sql_ddl 0
	run_sql_file $CUR/data/test_ignore_sql_rewrite.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	wait_ignore_sql_ddl 1

	run_sql "SELECT COUNT(*) AS retained_objects FROM information_schema.tables
		WHERE table_schema = 'event_filter' AND table_name IN
		('sql_rename_raw', 'sql_alter_rename_raw', 'sql_view_rename_raw',
		'sql_multi_ignored', 'sql_drop_ignored', 'sql_drop_view_ignored');" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains 'retained_objects: 6'
	run_sql "SELECT COUNT(*) AS renamed_objects FROM information_schema.tables
		WHERE table_schema = 'event_filter' AND table_name IN
		('sql_rename_restored_done', 'sql_alter_rename_restored_done',
		'sql_view_rename_restored_done', 'sql_multi_kept_done');" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains 'renamed_objects: 4'
	run_sql "SELECT COUNT(*) AS unexpected_objects FROM information_schema.tables
		WHERE table_schema = 'event_filter' AND table_name IN
		('sql_rename_raw_done', 'sql_alter_rename_raw_done', 'sql_view_rename_raw_done', 'sql_multi_ignored_done',
		'sql_rename_restored', 'sql_alter_rename_restored', 'sql_view_rename_restored', 'sql_multi_kept',
		'sql_drop_kept', 'sql_drop_view_kept');" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains 'unexpected_objects: 0'

	run_sql "SELECT
		(SELECT id FROM event_filter.sql_exchange_ignored) AS ignored_partition,
		(SELECT id FROM event_filter.sql_exchange_ignored_normal) AS ignored_normal,
		(SELECT id FROM event_filter.sql_exchange_kept) AS kept_partition,
		(SELECT id FROM event_filter.sql_exchange_kept_normal) AS kept_normal;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains 'ignored_partition: 1'
	check_contains 'ignored_normal: 2'
	check_contains 'kept_partition: 2'
	check_contains 'kept_normal: 1'

	run_sql "SELECT COUNT(*) AS ignored_creates FROM information_schema.tables
		WHERE table_schema = 'filter' AND table_name IN ('sql_like_ignored', 'sql_view_ignored');" \
		${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains 'ignored_creates: 0'
	check_table_exists filter.sql_like_kept ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "SELECT (SELECT id FROM filter.sql_view_kept) AS view_id,
		(SELECT id FROM filter.sql_view_replace_ignored) AS ignored_replacement,
		(SELECT id FROM filter.sql_view_replace_kept) AS kept_replacement;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains 'view_id: 7'
	check_contains 'ignored_replacement: 7'
	check_contains 'kept_replacement: 17'

	run_sql "SET GLOBAL tidb_enable_fast_create_table = ON;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	local pids=()
	local i kind pid
	for i in $(seq 1 8); do
		for kind in ignored kept; do
			mysql -h${UP_TIDB_HOST} -P${UP_TIDB_PORT} -uroot -D event_filter \
				-e "CREATE TABLE sql_batch_${kind}_$i (id INT PRIMARY KEY);" &
			pids+=("$!")
		done
	done
	for pid in "${pids[@]}"; do
		wait "$pid"
	done
	run_sql "SET GLOBAL tidb_enable_fast_create_table = OFF;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	wait_ignore_sql_ddl 2
	run_sql "SELECT COUNT(*) AS kept_creates FROM information_schema.tables
		WHERE table_schema = 'event_filter' AND table_name LIKE 'sql_batch_kept_%';" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains 'kept_creates: 8'
	run_sql "SELECT COUNT(*) AS ignored_creates FROM information_schema.tables
		WHERE table_schema = 'event_filter' AND table_name LIKE 'sql_batch_ignored_%';" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains 'ignored_creates: 0'
}

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	start_tidb_cluster --workdir $WORK_DIR

	export GO_FAILPOINTS=''
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --addr "127.0.0.1:8300" --pd "http://${UP_PD_HOST_1}:${UP_PD_PORT_1}"

	TOPIC_NAME="ticdc-event-filter-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	# create changefeed
	cdc_cli_changefeed create --changefeed-id=event-filter --sink-uri="$SINK_URI" \
		--server="127.0.0.1:8300" --config=$CUR/conf/cf.toml

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" "$CUR/conf/cf.toml" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "$CUR/conf/cf.toml" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI --config "$CUR/conf/cf.toml" ;;
	esac

	# CDC forwards CREATE TABLE ... LIKE ... as-is, so the downstream must have the
	# referenced table available for the DDL to succeed.
	run_sql "CREATE DATABASE IF NOT EXISTS filtered_like_src;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "CREATE TABLE IF NOT EXISTS filtered_like_src.t_src_like (id INT PRIMARY KEY, val INT);" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# make suer table t1 is deleted in upstream and exists in downstream
	check_table_not_exists "event_filter.t1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "event_filter.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_normal" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_truncate" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_alter" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_name" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_name1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_name2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_name3" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_virtual" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_like_from_filtered" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	sleep 20

	# check those rows that are not filtered are synced to downstream
	run_sql "select count(1) from event_filter.t1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 2"
	run_sql "select count(2) from event_filter.t1 where id=1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(2): 1"
	run_sql "select count(3) from event_filter.t1 where id=2;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(3): 0"
	run_sql "select count(4) from event_filter.t1 where id=3;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(4): 0"
	run_sql "select count(5) from event_filter.t1 where id=4;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(5): 1"

	# Add these checks after the existing checks for t_virtual
	# check virtual column table filtering with virtual column condition
	run_sql "select count(1) from event_filter.t_virtual;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 3" # Only 3 rows should pass the filter

	run_sql "select count(1) from event_filter.t_virtual where id=1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 1"

	run_sql "select count(1) from event_filter.t_virtual where id=2;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 0" # filtered by id=2

	run_sql "select count(1) from event_filter.t_virtual where id=3;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 0" # filtered by category and is_discounted

	run_sql "select count(1) from event_filter.t_virtual where id=4;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 1"

	run_sql "select count(1) from event_filter.t_virtual where id=5;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 0" # filtered by is_discounted

	run_sql "select count(1) from event_filter.t_virtual where id=6;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 0" # filtered by is_discounted

	run_sql "select count(1) from event_filter.t_virtual where id=7;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 1"

	# Verify that rows with is_discounted=true are filtered
	run_sql "select count(1) from event_filter.t_virtual where quantity > 10;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 0" # All discounted items should be filtered

	run_sql "select count(1) from event_filter.t_like_from_filtered;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 1"

	run_sql "select count(1) from event_filter.t_like_from_filtered where id=1 and val=200;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "count(1): 1"

	run_sql "TRUNCATE TABLE event_filter.t_truncate;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql_file $CUR/data/test_truncate.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "ALTER TABLE event_filter.t_alter MODIFY t_bigint BIGINT;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql_file $CUR/data/test_alter.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "RENAME TABLE event_filter.t_name TO event_filter.t_rename;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "RENAME TABLE event_filter.t_name1 TO event_filter.t_rename1;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "RENAME TABLE event_filter.t_name2 TO event_filter.t_rename2;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "RENAME TABLE event_filter.t_name3 TO event_filter.t_rename3;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "RENAME TABLE event_filter.t_rename4 TO event_filter.t_rename5;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_rename" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_rename1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_rename2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_rename3" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_rename5" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql_file $CUR/data/test_rename.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/test_create.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE event_filter.t_create (id INT PRIMARY KEY, val INT);" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.t_create" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "create table event_filter.finish_mark(id int primary key);" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "event_filter.finish_mark" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# check table t_normal is replicated
	sleep 10
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	run_sql "create database foo;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_db_exists "foo" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	sleep 10
	check_db_not_exists "foo" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# only capture table filter.t1, so filter.t2 should not be replicated
	run_sql "create database if not exists filter;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "create database if not exists filter;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "create table filter.t1 (id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "filter.t1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	run_sql "create table filter.t2 (id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists "filter.t2" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	sleep 10
	check_table_not_exists "filter.t2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# Regression for #6774: ignore-sql must match the original ordinary DDL text.
	run_sql_file $CUR/data/test_ignore_sql.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	ensure 20 "run_sql \"SELECT COUNT(*) AS finished FROM information_schema.statistics
		WHERE table_schema = 'event_filter' AND index_name = 'ignore_sql_done';\" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT};
		check_contains 'finished: 4'"

	run_sql "SELECT COUNT(*) AS ignored_columns FROM information_schema.columns
		WHERE table_schema = 'event_filter' AND table_name = 't_ignore_sql_original' AND column_name = 'c';" \
		${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "ignored_columns: 0"
	run_sql "SELECT COUNT(*) AS replicated_columns FROM information_schema.columns
		WHERE table_schema = 'event_filter' AND table_name IN
		('t_ignore_sql_quoted', 't_ignore_sql_keyword', 't_ignore_sql_whitespace') AND column_name = 'c';" \
		${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_contains "replicated_columns: 3"

	test_rewritten_query_filter

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
