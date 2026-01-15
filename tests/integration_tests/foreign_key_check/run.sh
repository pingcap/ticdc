#!/bin/bash
# This test is to detect that SQL statements that violate FK can also be successfully synchronized to the
# downstream MySQL class with FK detection enabled.
# This is because during the synchronization process, CDC may generate some intermediate SQL statements that violate FK.

# we first disable foreign key check in upstream, and create two tables with FK constraints.
# then we insert some data to the tables, and check the data is synchronized to the downstream.

set -xeu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

if [ "$SINK_TYPE" != "mysql" ]; then
	return
fi

rm -rf $WORK_DIR && mkdir -p $WORK_DIR
start_tidb_cluster --workdir $WORK_DIR
trap 'stop_test $WORK_DIR' EXIT
run_sql "set global foreign_key_checks=0;" ${UP_TIDB_HOST} ${UP_TIDB_PORT}

start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})
run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

SINK_URI="mysql://root@127.0.0.1:3306/?max-txn-row=1"
cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" --changefeed-id="test-1"

run_sql_file $CUR/data/pre.sql ${UP_TIDB_HOST} ${UP_TIDB_PORT}

check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml

cleanup_process $CDC_BINARY
