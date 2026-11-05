#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function check_recover_schema_tables() {
	ensure 30 "check_db_exists recover_schema_test ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}"
	ensure 30 "run_sql 'use recover_schema_test; show tables;' ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} && check_contains 'included1' && check_contains 'included2' && check_not_contains 'filtered'"
}

function test_recover_schema() {
	run_sql "drop database if exists recover_schema_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "create database recover_schema_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "create table recover_schema_test.included1 (id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "create table recover_schema_test.filtered (id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql "create table recover_schema_test.included2 (id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_recover_schema_tables

	run_sql "drop database recover_schema_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	ensure 30 "check_db_not_exists recover_schema_test ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}"

	# Current TiDB stores only a snapshot TS in the recover-schema job.
	run_sql "flashback database recover_schema_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	ensure 30 "grep -q 'verified recover schema job uses snapshot TS' $WORK_DIR/cdc.log"
	check_recover_schema_tables

	run_sql "drop database recover_schema_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	ensure 30 "check_db_not_exists recover_schema_test ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}"

	# Simulate the old TiDB job format, which embeds all recovered table infos.
	check_cdc_server_guard --workdir "$WORK_DIR"
	stop_cdc_server_guards
	cdc_pid=$(get_cdc_pid "$CDC_HOST" "$CDC_PORT")
	kill_cdc_pid $cdc_pid
	export GO_FAILPOINTS='github.com/pingcap/ticdc/logservice/schemastore/forceRecoverSchemaJobWithTableInfo=return(true)'
	run_cdc_server_with_guard --max-restarts 3 --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "-recover-schema" --data-dir "$WORK_DIR/cdc_data"
	export GO_FAILPOINTS=''

	run_sql "flashback database recover_schema_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	ensure 30 "grep -q 'forced recover schema job to use embedded table infos' $WORK_DIR/cdc-recover-schema.log"
	check_recover_schema_tables

	run_sql "drop database recover_schema_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	ensure 30 "check_db_not_exists recover_schema_test ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}"
}

function run() {
	# storage and pulsar is not supported yet.
	if [ "$SINK_TYPE" == "storage" ]; then
		return
	fi

	# TODO(dongmen): enable pulsar in the future.
	if [ "$SINK_TYPE" == "pulsar" ]; then
		exit 0
	fi

	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	export GO_FAILPOINTS='github.com/pingcap/ticdc/logservice/schemastore/verifyRecoverSchemaJobWithSnapshotTS=return(true)'
	run_cdc_server_with_guard --max-restarts 3 --workdir $WORK_DIR --binary $CDC_BINARY
	export GO_FAILPOINTS=''

	# this test contains `recover table`, which requires super privilege, so we
	# can't use the normal user
	TOPIC_NAME="ticdc-common-1-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar+ssl://127.0.0.1:6651/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://root@127.0.0.1:3306/" ;;
	esac

	if [ "$SINK_TYPE" == "pulsar" ]; then
		cat <<EOF >>$WORK_DIR/pulsar_test.toml
        [sink.pulsar-config]
        tls-trust-certs-file-path="${WORK_DIR}/ca.cert.pem"
        auth-tls-private-key-path="${WORK_DIR}/broker_client.key-pk8.pem"
        auth-tls-certificate-path="${WORK_DIR}/broker_client.cert.pem"
EOF
		cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config=$WORK_DIR/pulsar_test.toml
	else
		cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --config="$CUR/conf/changefeed.toml"
	fi

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI --config $WORK_DIR/pulsar_test.toml --ca "${WORK_DIR}/ca.cert.pem" --auth-tls-private-key-path "${WORK_DIR}/broker_client.key-pk8.pem" --auth-tls-certificate-path="${WORK_DIR}/broker_client.cert.pem" ;;
	esac

	test_recover_schema

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/test_v5.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/test_finish.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists common_1.v1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists common_1.users ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists common.v ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists common_1.recover_and_insert ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists common_1.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_cdc_server_guard --workdir "$WORK_DIR" --logsuffix "-recover-schema"
	check_cdc_server_guard --workdir "$WORK_DIR"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	check_cdc_server_guard --workdir "$WORK_DIR" --logsuffix "-recover-schema"
	check_cdc_server_guard --workdir "$WORK_DIR"

	stop_cdc_server_guards
	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
