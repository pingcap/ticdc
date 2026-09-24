#!/bin/bash

set -e

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

# Use the Simple decoder to verify checksums and sync data from Kafka to MySQL.
function check_sparse_checksums() {
	# Row checksums are stored when rows are written. Changing the global setting
	# only affects new sessions, so seed historical rows in a separate connection.
	run_sql "set global tidb_enable_row_level_checksum=false" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE test.checksum_sparse (id INT PRIMARY KEY CLUSTERED, v INT);
		INSERT INTO test.checksum_sparse VALUES (1,10),(3,30),(5,50),(7,70),(9,90);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "set global tidb_enable_row_level_checksum=true" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# One transaction mixes historical deletes without checksums with inserts and
	# an update (two physical rows, one logical checksum).
	run_sql "BEGIN;
		DELETE FROM test.checksum_sparse WHERE id IN (1,3,7,9);
		INSERT INTO test.checksum_sparse VALUES (2,20),(4,40),(8,80);
		UPDATE test.checksum_sparse SET v=51 WHERE id=5;
		COMMIT;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.checksum_sparse ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	run_sql "CREATE TABLE test.checksum_sparse_done (id INT PRIMARY KEY);
		INSERT INTO test.checksum_sparse_done VALUES (1);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.checksum_sparse_done ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200

	# Create the marker only after all DML reaches downstream. With one partition,
	# the dump then contains every DML before the marker, regardless of DDL scheduling.
	# Inspect the payload too: sync_diff alone cannot detect lost checksums.
	kafka_dump --topic "$TOPIC_NAME" --until-table checksum_sparse_done >"$WORK_DIR/checksum_sparse.json"
	python3 "$CUR/check_checksum.py" "$WORK_DIR/checksum_sparse.json"
}

function run() {
	if [ "$SINK_TYPE" != "kafka" ]; then
		return
	fi

	# clean up environment
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	# start tidb cluster
	start_tidb_cluster --workdir $WORK_DIR

	# upstream TiDB disable the cluster index
	run_sql "set global tidb_enable_clustered_index=0;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# upstream tidb cluster enable row level checksum
	run_sql "set global tidb_enable_row_level_checksum=true" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	TOPIC_NAME="ticdc-simple-basic-$RANDOM"

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY \
		--failpoint 'github.com/pingcap/ticdc/pkg/messaging/ForceMarshalBatchDMLEvent=return(true)'

	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=simple&partition-num=1"
	cdc_cli_changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml" -c "simple-basic"
	sleep 5 # wait for changefeed to start

	# Recompute checksums from decoded rows. The optional upstream snapshot check
	# can disagree with historical row checksums after this case's schema changes.
	cdc_kafka_consumer --upstream-uri $SINK_URI --downstream-uri="mysql://root@127.0.0.1:3306/?safe-mode=true&batch-dml-enable=false&enable-ddl-ts=false" --config="$CUR/conf/changefeed.toml" --log-file $WORK_DIR/cdc_kafka_consumer.log 2>&1 &

	# pre execute some ddls
	run_sql_file $CUR/data/pre_ddl.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.finish_mark_for_ddl ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200

	# pause and resume changefeed makes sure changefeed sending bootstrap events
	# when it is resumed, so the data after pause can be decoded correctly
	TOPIC_NAME="ticdc-simple-basic-$RANDOM"
	SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=simple&partition-num=1"
	cdc_cli_changefeed pause -c "simple-basic"
	cdc_cli_changefeed update -c "simple-basic" --sink-uri=$SINK_URI --config="$CUR/conf/changefeed.toml" --no-confirm
	cdc_cli_changefeed resume -c "simple-basic"
	cdc_kafka_consumer --upstream-uri $SINK_URI --downstream-uri="mysql://root@127.0.0.1:3306/?safe-mode=true&batch-dml-enable=false&enable-ddl-ts=false" \
		--config="$CUR/conf/changefeed.toml" --log-file $WORK_DIR/cdc_kafka_consumer_resume.log 2>&1 &

	check_sparse_checksums

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	run_sql "CREATE TABLE test.finish_mark1 (a int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.finish_mark1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	run_sql_file $CUR/data/data_gbk.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE test.finish_mark2 (a int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_table_exists test.finish_mark2 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 200
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
