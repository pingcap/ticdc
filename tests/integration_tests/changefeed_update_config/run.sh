#!/bin/bash
## this case is used to verify the new changefeed config afer update changefeed take effect. Such as filter or split table

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# start two cdc servers to enable multi-dispatcher scenario
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301"

	# initialize upstream data with one split table
	run_sql_file $CUR/data/pre.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/pre.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}
}

function create_changefeed() {
	TOPIC_NAME="ticdc-changefeed-update-config-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://root:@127.0.0.1:3306/" ;;
	esac

	# create with initial filter config [*.*]
	cdc_cli_changefeed create --sink-uri="$SINK_URI" -c "test" --config="$CUR/conf/changefeed_init.toml"

	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac
}

function write_data_round() {
	# write some rows to ensure replication
	run_sql "insert into split_region.test1(id) values (101),(102),(103),(104),(105)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
}

function write_data_round2() {
	# write some rows to ensure replication
	run_sql "insert into split_region.test1(id) values (1101),(1102),(1013),(1104),(1015)" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
}

function check_dispatcher_gt_two() {
	local ipAddr="127.0.0.1:8300"
	local changefeedID="test"
	local retry=60
	local count=0
	while [[ $count -lt $retry ]]; do
		ans=$(curl -s -X GET http://"${ipAddr}"/api/v2/changefeeds/"${changefeedID}"/get_dispatcher_count?mode=0\&keyspace=$KEYSPACE_NAME || true)
		echo $ans
		value=$(echo $ans | jq -r '.count')
		if [[ "$value" != "null" && "$value" -gt 2 ]]; then
			return 0
		fi
		sleep 2
		count=$((count + 1))
	done
	echo "dispatcher count not greater than 2 after retries"
	return 1
}

function main() {
	prepare
	create_changefeed

	# sync part of data under default config
	write_data_round

	# pause, update config (add filter [!test.*] with scheduler), then resume
	cdc_cli_changefeed pause -c "test"
	cdc_cli_changefeed update -c "test" --config="$CUR/conf/changefeed.toml" --no-confirm
	cdc_cli_changefeed resume -c "test"

	# record current tso, then create a new upstream table to be filtered
	current_tso=$(run_cdc_cli_tso_query $UP_PD_HOST_1 $UP_PD_PORT_1)
	run_sql "create database if not exists test; create table if not exists test.t1(id int primary key);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	# continue to write data after resume to advance checkpoint
	write_data_round2

	# wait until checkpoint tso exceeds the recorded tso
	retry=60
	cnt=0
	while [[ $cnt -lt $retry ]]; do
		checkpoint=$(cdc_cli_changefeed query -c "test" | grep -v "Command to ticdc" | jq -r '.checkpoint_tso')
		if [[ "$checkpoint" != "null" && "$checkpoint" -gt "$current_tso" ]]; then
			break
		fi
		sleep 2
		cnt=$((cnt + 1))
	done
	if [[ $cnt -ge $retry ]]; then
		echo "checkpoint tso did not exceed current tso in time: checkpoint=$checkpoint, current_tso=$current_tso"
		exit 1
	fi

	# ensure downstream does NOT have filtered table
	check_table_not_exists test.t1 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	# check data consistency
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 60

	# check dispatcher count > 2
	check_dispatcher_gt_two

	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
main $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
