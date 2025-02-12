# this script is test all the basic ddls when the table is split into 
# multiple dispatchers in multi cdc server
# TODO:This script need to add kafka-class sink

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1
check_time=60

function prepare() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300"
    run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301"

	TOPIC_NAME="ticdc-ddl_split_table-$RANDOM"

    # to make the table multi regions, to help create multiple dispatchers for the table
    run_sql_file $CUR/data/pre.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql_file $CUR/data/pre.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 500_

	SINK_URI="mysql://root:@127.0.0.1:3306/" 
    #SINK_URI="kafka://127.0.0.1:9094/$TOPIC_NAME?protocol=open-protocol&partition-num=1&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" 
	# case $SINK_TYPE in
	# kafka) SINK_URI="kafka://127.0.0.1:9094/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	# storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	# pulsar)
	# 	run_pulsar_cluster $WORK_DIR normal
	# 	SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
	# 	;;
	# *) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	# esac

    sleep 10

	run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c "test" --config="$CUR/conf/changefeed.toml"

    #run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9094/$TOPIC_NAME?protocol=open-protocol&partition-num=1&version=${KAFKA_VERSION}&max-message-bytes=10485760" 

	# case $SINK_TYPE in
	# kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	# storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	# pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	# esac
}

trap stop_tidb_cluster EXIT

prepare $*
## execute ddls
run_sql_file $CUR/data/ddls.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
# ## insert some datas
run_sql_file $CUR/data/dmls.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 30
cleanup_process $CDC_BINARY

check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"