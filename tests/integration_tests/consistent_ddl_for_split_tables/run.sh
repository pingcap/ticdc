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

function main() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301"

	TOPIC_NAME="ticdc-consistent-ddl-split-table-$RANDOM"

	# to make the table multi regions, to help create multiple dispatchers for the table
	run_sql_file $CUR/data/pre.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	run_sql_file $CUR/data/pre.sql ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 500_

	SINK_URI="mysql://root:@127.0.0.1:3306/"

	sleep 10

	run_cdc_cli changefeed create --sink-uri="$SINK_URI" -c "test" --config="$CUR/conf/changefeed.toml"

	## execute ddls
	run_sql_file $CUR/data/ddls.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	# ## insert some datas
	run_sql_file $CUR/data/dmls.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

	sleep 10
	current_tso=$(run_cdc_cli_tso_query $UP_PD_HOST_1 $UP_PD_PORT_1)
	sleep 120
	changefeed_id="test"
	storage_path="file://$WORK_DIR/redo"
	tmp_download_path=$WORK_DIR/cdc_data/redo/$changefeed_id
	ensure 100 check_redo_checkpoint_ts $changefeed_id $current_tso $storage_path $tmp_download_path/meta

	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 30
	cleanup_process $CDC_BINARY
}

trap stop_tidb_cluster EXIT
main $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
