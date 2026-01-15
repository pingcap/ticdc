#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

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

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

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
		cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	fi

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI --ca "${WORK_DIR}/ca.cert.pem" --auth-tls-private-key-path "${WORK_DIR}/broker_client.key-pk8.pem" --auth-tls-certificate-path="${WORK_DIR}/broker_client.cert.pem" ;;
	esac

	run_sql_file $CUR/data/test.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/test_v5.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/test_finish.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists common_1.v1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists common_1.recover_and_insert ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists common_1.finish_mark ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
