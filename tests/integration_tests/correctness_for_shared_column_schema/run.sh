#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function print_upstream_table_schemas_before_changefeed() {
	echo "[$(date)] print upstream table schemas before creating changefeed"

	local listTablesSQL
	listTablesSQL="SELECT CONCAT('SHOW CREATE TABLE \`', TABLE_SCHEMA, '\`.\`', TABLE_NAME, '\`;')
FROM information_schema.tables
WHERE TABLE_TYPE = 'BASE TABLE'
  AND LOWER(TABLE_SCHEMA) NOT IN (
    'information_schema',
    'performance_schema',
    'metrics_schema',
    'inspection_schema',
    'inspection_result',
    'cluster_info',
    'mysql',
    'sys'
  )
ORDER BY TABLE_SCHEMA, TABLE_NAME;"

	local showCreateTableSQLs
	showCreateTableSQLs=$(mysql -uroot -h${UP_TIDB_HOST} -P${UP_TIDB_PORT} --default-character-set utf8mb4 -N -e "$listTablesSQL")
	if [ -z "$showCreateTableSQLs" ]; then
		echo "[$(date)] no upstream user tables found before creating changefeed"
		return
	fi

	while IFS= read -r showCreateSQL; do
		[ -z "$showCreateSQL" ] && continue
		run_sql "$showCreateSQL" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	done <<<"$showCreateTableSQLs"
}

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR
	stop_tidb_cluster
	start_tidb_cluster --workdir $WORK_DIR

	# Prepare identical initial data on both upstream and downstream.
	# The downstream schema is created directly, while the upstream goes through DDL evolution.
	run_sql_file $CUR/data/upstream_prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/downstream_prepare.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# Start from a TSO after the initial setup to avoid replicating the bootstrap DDL/DML.
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-correctness-for-shared-column-schema-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	print_upstream_table_schemas_before_changefeed
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	run_sql_file $CUR/data/upstream_dml.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300
	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
