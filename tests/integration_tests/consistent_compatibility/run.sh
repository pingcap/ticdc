#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"

stop() {
	# to distinguish whether the test failed in the DML synchronization phase or the DDL synchronization phase
	echo $(mysql -h${DOWN_TIDB_HOST} -P${DOWN_TIDB_PORT} -uroot -e "SELECT count(*) FROM consistent_compatibility.usertable;")
	stop_test $WORK_DIR
}

function run() {
	# we only support eventually consistent replication with MySQL sink
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi

	# This test is only for old architecture
	if [ "$NEXT_GEN" = 1 ]; then
		return
	fi

	start_tidb_cluster --workdir $WORK_DIR

	run_sql "set @@global.tidb_enable_exchange_partition=on" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# Remove TICDC_NEWARCH to start with old architecture
	echo "Starting with old arch"
	export TICDC_NEWARCH=false

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix consistent_compatibility.server1

	SINK_URI="mysql://normal:123456@127.0.0.1:3306/"
	changefeed_id=$(cdc_cli_changefeed create --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml" | grep '^ID:' | head -n1 | awk '{print $2}')

	run_sql "CREATE DATABASE consistent_compatibility CHARACTER SET utf8 COLLATE utf8_unicode_ci" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	go-ycsb load mysql -P $CUR/conf/workload -p mysql.host=${UP_TIDB_HOST} -p mysql.port=${UP_TIDB_PORT} -p mysql.user=root -p mysql.db=consistent_compatibility
	run_sql "CREATE TABLE consistent_compatibility.usertable1 like consistent_compatibility.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE consistent_compatibility.usertable2 like consistent_compatibility.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE consistent_compatibility.usertable3 like consistent_compatibility.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE TABLE consistent_compatibility.usertable_bak like consistent_compatibility.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	for i in {1..100}; do
		run_sql "CREATE TABLE IF NOT EXISTS consistent_compatibility.table_$i (id INT AUTO_INCREMENT PRIMARY KEY, data VARCHAR(255));" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done
	check_table_exists "consistent_compatibility.usertable1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists "consistent_compatibility.usertable2" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
	check_table_exists "consistent_compatibility.usertable3" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
	check_table_exists "consistent_compatibility.usertable_bak" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
	for i in {1..100}; do
		check_table_exists "consistent_compatibility.table_$i" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 120
	done
	sleep 5

	# case 1:
	# global ddl tests -> ActionRenameTable
	# table ddl tests -> ActionDropTable
	run_sql "DROP TABLE consistent_compatibility.usertable1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "RENAME TABLE consistent_compatibility.usertable_bak TO consistent_compatibility.usertable1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO consistent_compatibility.usertable1 SELECT * FROM consistent_compatibility.usertable limit 10" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "RENAME TABLE consistent_compatibility.usertable1 TO consistent_compatibility.usertable1_1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT IGNORE INTO consistent_compatibility.usertable1_1 SELECT * FROM consistent_compatibility.usertable" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# case 2:
	# global ddl tests -> ActionCreateSchema, ActionModifySchemaCharsetAndCollate
	# table ddl tests -> ActionMultiSchemaChange, ActionAddColumn, ActionDropColumn, ActionModifyTableCharsetAndCollate
	run_sql "CREATE DATABASE consistent_compatibility1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "ALTER DATABASE consistent_compatibility CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "ALTER TABLE consistent_compatibility.usertable2 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO consistent_compatibility.usertable2 SELECT * FROM consistent_compatibility.usertable limit 20" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "ALTER TABLE consistent_compatibility.usertable2 DROP COLUMN FIELD0" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "ALTER TABLE consistent_compatibility.usertable2 ADD COLUMN dummy varchar(30)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# case 3:
	# global ddl tests -> ActionDropSchema, ActionRenameTables
	# table ddl tests -> ActionModifyColumn
	run_sql "DROP DATABASE consistent_compatibility1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "ALTER TABLE consistent_compatibility.usertable3 CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "RENAME TABLE consistent_compatibility.usertable2 to consistent_compatibility.usertable2_1, consistent_compatibility.usertable3 TO consistent_compatibility.usertable3_1" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "ALTER TABLE consistent_compatibility.usertable3_1 MODIFY COLUMN FIELD1 varchar(100)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "INSERT INTO consistent_compatibility.usertable3_1 SELECT * FROM consistent_compatibility.usertable limit 31" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# case 4:
	# rename multiple tables
	for i in {1..100}; do
		run_sql "INSERT INTO consistent_compatibility.table_$i (data) VALUES ('insert_$(date +%s)_${RANDOM}'"
		new_table_name="table_$(($i + 500))"
		run_sql "RENAME TABLE consistent_compatibility.table_$i TO consistent_compatibility.$new_table_name;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done

	run_sql_file $CUR/data/data.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "CREATE table consistent_compatibility.check1(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# to ensure row changed events have been replicated to TiCDC
	sleep 60

	storage_path="file://$WORK_DIR/redo"
	tmp_download_path=$WORK_DIR/cdc_data/redo/$changefeed_id
	current_tso=$(run_cdc_cli_tso_query $UP_PD_HOST_1 $UP_PD_PORT_1)
	ensure 300 check_redo_resolved_ts $changefeed_id $current_tso $storage_path $tmp_download_path/meta
	export GO_FAILPOINTS=''
	cleanup_process $CDC_BINARY
	export TICDC_NEWARCH=true

	rts=$(cdc redo meta --storage="$storage_path" --tmp-dir="$tmp_download_path" | grep -oE "resolved-ts:[0-9]+" | awk -F: '{print $2}')
	sed "s/<placeholder>/$rts/g" $CUR/conf/diff_config.toml >$WORK_DIR/diff_config.toml

	cat $WORK_DIR/diff_config.toml
	cdc redo apply --log-level debug --tmp-dir="$tmp_download_path/apply" \
		--storage="$storage_path" \
		--sink-uri="mysql://normal:123456@127.0.0.1:3306/" >$WORK_DIR/cdc_redo.log
	check_sync_diff $WORK_DIR $WORK_DIR/diff_config.toml 200
}

trap stop EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
