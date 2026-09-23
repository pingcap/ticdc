#!/bin/bash
# Copyright 2022 PingCAP, Inc.
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

cd "$(dirname "${BASH_SOURCE[0]}")"/..

MOCKGEN="tools/bin/mockgen"

if [ ! -f "$MOCKGEN" ]; then
	echo "${MOCKGEN} does not exist, please run 'make tools/bin/mockgen' first"
	exit 1
fi

pids=()

wait_for_batch() {
	local pid failed=0
	for pid in "${pids[@]}"; do
		wait "$pid" || failed=1
	done
	pids=()
	if [ "$failed" -ne 0 ]; then
		exit 1
	fi
}

run_mockgen() {
	"$MOCKGEN" "$@" &
	pids+=("$!")
	if [ "${#pids[@]}" -eq 4 ]; then
		wait_for_batch
	fi
}

run_mockgen -source coordinator/changefeed/changefeed_db_backend.go -destination coordinator/changefeed/mock/changefeed_db_backend.go
run_mockgen -source pkg/etcd/etcd.go -destination pkg/etcd/etcd_mock.go -package etcd
run_mockgen -source pkg/etcd/client.go -destination pkg/etcd/client_mock.go -package etcd
run_mockgen -source pkg/api/v2/tso.go -destination pkg/api/v2/mock/tso_mock.go -package mock
run_mockgen -source pkg/api/v2/unsafe.go -destination pkg/api/v2/mock/unsafe_mock.go -package mock
run_mockgen -source pkg/api/v2/status.go -destination pkg/api/v2/mock/status_mock.go -package mock
run_mockgen -source pkg/api/v2/capture.go -destination pkg/api/v2/mock/capture_mock.go -package mock
run_mockgen -source pkg/api/v2/processor.go -destination pkg/api/v2/mock/processor_mock.go -package mock
run_mockgen -source pkg/api/v2/changefeed.go -destination pkg/api/v2/mock/changefeed_mock.go -package mock
run_mockgen -source pkg/api/v2/api_client.go -destination pkg/api/v2/mock/api_client_mock.go -package mock
run_mockgen -source logservice/logpuller/debug.go -destination logservice/logpuller/mock/debug_info_provider.go -package mock
run_mockgen -source pkg/sink/codec/simple/marshaller.go -destination pkg/sink/codec/simple/mock/marshaller.go
run_mockgen -source pkg/sink/kafka/admin_client.go -destination pkg/sink/kafka/admin_client_mock.go -package kafka
run_mockgen -source pkg/sink/kafka/factory.go -destination pkg/sink/kafka/factory_mock.go -package kafka
run_mockgen -source pkg/sink/kafka/admin.go -destination pkg/sink/kafka/sarama_admin_mock.go -package kafka
run_mockgen -source pkg/sink/kafka/sarama_sync_producer.go -destination pkg/sink/kafka/sarama_sync_producer_mock.go -package kafka
run_mockgen -source downstreamadapter/sink/topicmanager/topic_manager.go -destination downstreamadapter/sink/topicmanager/topic_manager_mock.go -package topicmanager
run_mockgen -source pkg/keyspace/keyspace_manager.go -destination pkg/keyspace/keyspace_manager_mock.go -package keyspace
run_mockgen -source pkg/txnutil/gc/gc_manager.go -destination pkg/txnutil/gc/gc_manager_mock.go -package gc
run_mockgen -source pkg/txnutil/gc/gc_client.go -destination pkg/txnutil/gc/gc_client_mock.go -package gc
run_mockgen -source pkg/redo/writer/writer.go -destination pkg/redo/writer/writer_mock.go -package writer
run_mockgen -source downstreamadapter/sink/sink.go -destination downstreamadapter/sink/mock/sink_mock.go -package mock
run_mockgen -destination pkg/messaging/mock/message_center_mock.go -package mock github.com/pingcap/ticdc/pkg/messaging MessageCenter

wait_for_batch
