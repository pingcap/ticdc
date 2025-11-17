#!/usr/bin/env bash
# Copyright 2025 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $CUR/../_utils/test_prepare
WORK_DIR=$OUT_DIR/$TEST_NAME
CDC_BINARY=cdc.test
SINK_TYPE=$1

function kill_cdc_and_restart() {
	pd_addr=$1
	work_dir=$2
	cdc_binary=$3
	MAX_RETRIES=10
	cdc_pid=$(get_cdc_pid "$CDC_HOST" "$CDC_PORT")

	kill_cdc_pid $cdc_pid
	run_cdc_server --workdir $work_dir --binary $cdc_binary --pd $pd_addr
}

function run_restart_and_check() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_sql "CREATE DATABASE restart_changefeed;"
	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-restart-changefeed-$RANDOM"
	case $SINK_TYPE in
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c "changefeed-restart"

	run_sql "CREATE TABLE restart_changefeed.finish_mark_1 (a int primary key);"
	check_table_exists "restart_changefeed.finish_mark_1" ${DOWN_TIDB_HOST} ${DOWN_TIDB_PORT} 300
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300

	run_sql "INSERT INTO restart_changefeed.finish_mark_1 VALUES (1);"
	check_sync_diff $WORK_DIR $CUR/conf/diff_config.toml 300

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	kill_cdc_and_restart $pd_addr $WORK_DIR $CDC_BINARY

	changefeed_data=$(cdc_cli_changefeed query -c changefeed-restart -s 2>/dev/null)
	if [ -z "$changefeed_data" ]; then
		exit 1
	fi
	state=$(echo "$changefeed_data" | jq -r ".state")
	if [ $state != "normal" ]; then
		exit 1
	fi

	cleanup_process $CDC_BINARY
	stop_tidb_cluster
}

function run_pause_restart_resume() {
	if [ "$SINK_TYPE" != "mysql" ]; then
		return
	fi
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	start_ts=$(run_cdc_cli_tso_query ${UP_PD_HOST_1} ${UP_PD_PORT_1})

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-restart-changefeed-$RANDOM"
	case $SINK_TYPE in
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/?max-txn-row=1" ;;
	esac
	cdc_cli_changefeed create --start-ts=$start_ts --sink-uri="$SINK_URI" -c "changefeed-restart"

	cdc_cli_changefeed pause -c changefeed-restart

	pd_addr="http://$UP_PD_HOST_1:$UP_PD_PORT_1"
	kill_cdc_and_restart $pd_addr $WORK_DIR $CDC_BINARY

	changefeed_data=$(cdc_cli_changefeed query -c changefeed-restart -s 2>/dev/null)
	if [ -z "$changefeed_data" ]; then
		exit 1
	fi
	state=$(echo "$changefeed_data" | jq -r ".state")
	if [ $state != "stopped" ]; then
		exit 1
	fi

	cdc_cli_changefeed resume -c changefeed-restart
	changefeed_data=$(cdc_cli_changefeed query -c changefeed-restart -s 2>/dev/null)
	if [ -z "$changefeed_data" ]; then
		exit 1
	fi
	state=$(echo "$changefeed_data" | jq -r ".state")
	if [ $state != "normal" ]; then
		exit 1
	fi

	cleanup_process $CDC_BINARY
	stop_tidb_cluster
}

trap 'stop_tidb_cluster; collect_logs $WORK_DIR' EXIT
run_restart_and_check $*
run_pause_restart_resume $*

check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
