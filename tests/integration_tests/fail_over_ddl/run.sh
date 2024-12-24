#!/bin/bash
# This case is used to test the fail-over with ddl events of TiCDC.
# we start two TiCDC servers, and use failpoint to block the block event ddl execution of different situations
# and do restart to test the fail-over.

# There are multiple cases to test:
## 1.(CASE-A) when dispatchers are all meet the block event ddl, and report the status to maintainer, and maintainer ask table trigger to write ddl, table trigger write the ddl, but not response to maintainer, then the two node are both.

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function failOverCaseA() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	# record tso before we create tables to skip the system table DDLs
	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	## server 1
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0" --addr "127.0.0.1:8300"
	cdc_pid_1=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
	## server 2
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1" --addr "127.0.0.1:8301"
	
	TOPIC_NAME="ticdc-failover-ddl-test-two-node-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac
	run_cdc_cli changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR "kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

    run_sql "drop database if exists fail_over_ddl_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "create database fail_over_ddl_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "create table fail_over_ddl_test.test1 (id int primary key, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "create table fail_over_ddl_test.test2 (id int primary key, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}
    run_sql "create table fail_over_ddl_test.test3 (id int primary key, val int);" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    check_table_exists fail_over_ddl_test.test3 ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT}

    export GO_FAILPOINTS='github.com/pingcap/ticdc/downstreamadapter/dispatcher/BlockReportAfterWrite=return(true)'

    export GO_FAILPOINTS='github.com/pingcap/ticdc/downstreamadapter/dispatcher/HandleEventsSlowly=return(true)'

    run_sql "drop database fail_over_ddl_test;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

    ## make ddl must reach the place and report to maintainer, and get the write status, and block in the place that report to maintainer
	sleep 100
	
    kill_cdc_pid $cdc_pid_1
	cdc_pid_2=$(ps -C $CDC_BINARY -o pid= | awk '{print $1}')
    kill_cdc_pid $cdc_pid_2

    export GO_FAILPOINTS=''

    # restart cdc server
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "0-1" --addr "127.0.0.1:8300"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY --logsuffix "1-1" --addr "127.0.0.1:8301"

    run_sql "show databases;" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} &&
		check_not_contains "fail_over_test"

    ## continue to write ddl and dml to test the cdc server is working well
    run_sql_file $CUR/data/prepare.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}
	
    check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

	cleanup_process $CDC_BINARY
} 

trap stop_tidb_cluster EXIT
failOverCaseA $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
