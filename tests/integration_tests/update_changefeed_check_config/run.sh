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

function run() {
	rm -rf $WORK_DIR && mkdir -p $WORK_DIR

	start_tidb_cluster --workdir $WORK_DIR

	cd $WORK_DIR

	run_cdc_server --workdir $WORK_DIR --binary $CDC_BINARY

	TOPIC_NAME="ticdc-autorandom-test-$RANDOM"
	case $SINK_TYPE in
	kafka) SINK_URI="kafka://127.0.0.1:9092/$TOPIC_NAME?protocol=open-protocol&partition-num=4&kafka-version=${KAFKA_VERSION}&max-message-bytes=10485760" ;;
	storage) SINK_URI="file://$WORK_DIR/storage_test/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true" ;;
	pulsar)
		run_pulsar_cluster $WORK_DIR normal
		SINK_URI="pulsar://127.0.0.1:6650/$TOPIC_NAME?protocol=canal-json&enable-tidb-extension=true"
		;;
	*) SINK_URI="mysql://normal:123456@127.0.0.1:3306/" ;;
	esac

	CFID=cfid

	cdc_cli_changefeed create --sink-uri="$SINK_URI" -c "$CFID"
	case $SINK_TYPE in
	kafka) run_kafka_consumer $WORK_DIR $SINK_URI ;;
	storage) run_storage_consumer $WORK_DIR $SINK_URI "" "" ;;
	pulsar) run_pulsar_consumer --upstream-uri $SINK_URI ;;
	esac

	default=$(
		cat <<EOF
	{
		"balance_score_threshold": 20,
		"enable_splittable_check": false,
		"enable_table_across_nodes": false,
		"max_traffic_percentage": 1.25,
		"min_traffic_percentage": 0.8,
		"region_count_per_span": 100,
		"region_count_refresh_interval": 300000000000,
		"region_threshold": 10000,
		"scheduling_task_count_per_node": 20,
		"write_key_threshold": 0,
		"force_split": false
	}
EOF
	)
	sorted_default=$(echo "$default" | jq '. as $input | to_entries | sort_by(.key) | from_entries')
	echo "sorted_default:$sorted_default"

	cf_info=$(curl "http://$CDC_HOST:$CDC_PORT/api/v2/changefeeds/$CFID?keyspace=$KEYSPACE_NAME")
	scheduler_orig=$(echo "$cf_info" | jq '.config.scheduler')
	sorted_scheduler_orig=$(echo "$scheduler_orig" | jq '. as $input | to_entries | sort_by(.key) | from_entries')
	echo "sorted_scheduler_orig: $sorted_scheduler_orig"

	equal_res=$(jq -n --argjson obj1 "$sorted_default" --argjson obj2 "$sorted_scheduler_orig" '$obj1 == $obj2')
	if [ "$equal_res" != true ]; then
		exit 1
	fi

	# pause the changefeed
	curl -XPOST "http://$CDC_HOST:$CDC_PORT/api/v2/changefeeds/$CFID/pause?keyspace=$KEYSPACE_NAME"
	# set `enable_splittable_check` to true
	curl -XPUT "http://$CDC_HOST:$CDC_PORT/api/v2/changefeeds/$CFID?keyspace=$KEYSPACE_NAME" -d '{"replica_config": {"scheduler": {"enable_table_across_nodes": true}}}'

	if [ "$SINK_TYPE" = mysql ]; then
		enable_default=$(echo "$sorted_default" | jq '.enable_table_across_nodes = true | .enable_splittable_check = true')
	else
		enable_default=$(echo "$sorted_default" | jq '.enable_table_across_nodes = true')
	fi
	echo "enable_default:$enable_default"

	cf_info=$(curl "http://$CDC_HOST:$CDC_PORT/api/v2/changefeeds/$CFID?keyspace=$KEYSPACE_NAME")
	scheduler_orig=$(echo "$cf_info" | jq '.config.scheduler')
	sorted_scheduler_orig=$(echo "$scheduler_orig" | jq '. as $input | to_entries | sort_by(.key) | from_entries')
	echo "sorted_scheduler_orig: $sorted_scheduler_orig"
	equal_res=$(jq -n --argjson obj1 "$enable_default" --argjson obj2 "$sorted_scheduler_orig" '$obj1 == $obj2')
	if [ "$equal_res" != true ]; then
		exit 1
	fi

	cleanup_process $CDC_BINARY
}

trap 'stop_tidb_cluster; collect_logs $WORK_DIR' EXIT
run $*
check_logs $WORK_DIR
echo "[$(date)] <<<<<< run test case $TEST_NAME success! >>>>>>"
