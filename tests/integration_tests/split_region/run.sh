#!/bin/bash

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-split-region-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR oauth
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	if [ "$SINK_TYPE" == "pulsar" ]; then
		cat <<EOF >>$WORK_DIR/pulsar_test.toml
            [sink.pulsar-config.oauth2]
            oauth2-issuer-url="http://localhost:9096"
            oauth2-audience="cdc-api-uri"
            oauth2-client-id="1234"
            oauth2-private-key="${WORK_DIR}/credential.json"
EOF
	else
		echo "" >$WORK_DIR/pulsar_test.toml
	fi
	cdc_cli_changefeed create -c split-region --start-ts=$start_ts --sink-uri="$SINK_URI" --config $WORK_DIR/pulsar_test.toml >"$WORK_DIR/create-output.txt" 2>&1
	case $SINK_TYPE in
	kafka) run_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" $WORK_DIR/pulsar_test.toml ;;
	storage | pulsar) run_consumer "$WORK_DIR" "$SINK_URI" $WORK_DIR/pulsar_test.toml ;;
	esac

	# sync_diff can't check non-exist table, so we check expected tables are created in downstream first
	check_table_exists split_region.test1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_table_exists split_region.test2 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	if [ "$SINK_TYPE" = pulsar ]; then
		# The original Pulsar TOML names must remain accepted by the CLI, and
		# an ordinary update must preserve the OAuth private key hidden by GET.
		cdc_cli_changefeed pause -c split-region
		echo 'memory-quota = 2097152' >"$WORK_DIR/update.toml"
		cdc_cli_changefeed update -c split-region --config="$WORK_DIR/update.toml" --no-confirm >"$WORK_DIR/update-output.txt" 2>&1
		cdc_cli_changefeed resume -c split-region
	fi

	# split table into 5 regions, run some other DMLs and check data is synchronized to downstream
	run_sql "split table split_region.test1 between (1) and (100000) regions 50;"
	run_sql "split table split_region.test2 between (1) and (100000) regions 50;"
	run_sql_file $CUR/data/increment.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
	if [ "$SINK_TYPE" = pulsar ]; then
		cdc_cli_changefeed pause -c split-region
		local api="http://${CDC_HOST}:${CDC_PORT}/api/v2/changefeeds/split-region?keyspace=$KEYSPACE_NAME"
		curl -fsS "$api" -o "$WORK_DIR/pulsar-query.json"
		jq -e '.config.memory_quota == 2097152 and
			(.config.sink.pulsar_config.oauth2 | has("oauth2-private-key") | not)' "$WORK_DIR/pulsar-query.json" >/dev/null
		jq '{replica_config:(.config | .memory_quota=3145728)}' "$WORK_DIR/pulsar-query.json" >"$WORK_DIR/pulsar-update.json"
		curl -fsS -X PUT -H 'Content-Type: application/json' --data-binary "@$WORK_DIR/pulsar-update.json" \
			"$api" -o "$WORK_DIR/pulsar-update-output.json"
		cdc_cli_changefeed resume -c split-region
		run_sql 'INSERT INTO split_region.test1(id, val) VALUES (1073747000, 42);' "$UP_TIDB_HOST" "$UP_TIDB_PORT"
		check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml
		if grep -Fq "$WORK_DIR/credential.json" "$WORK_DIR"/*output* "$WORK_DIR/pulsar-query.json" "$WORK_DIR/cdc.log" "$WORK_DIR/stdout.log"; then
			echo "Pulsar OAuth private key leaked in product output"
			exit 1
		fi
	fi

	cleanup_process $CDC_BINARY
}

trap 'stop_test $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
