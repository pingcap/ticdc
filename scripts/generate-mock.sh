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

"$MOCKGEN" -source coordinator/changefeed/changefeed_db_backend.go -destination coordinator/changefeed/mock/changefeed_db_backend.go
"$MOCKGEN" -source pkg/etcd/etcd.go -destination pkg/etcd/etcd_mock.go -package etcd
"$MOCKGEN" -source pkg/etcd/client.go -destination pkg/etcd/client_mock.go -package etcd
"$MOCKGEN" -source pkg/api/v2/tso.go -destination pkg/api/v2/mock/tso_mock.go -package mock
"$MOCKGEN" -source pkg/api/v2/unsafe.go -destination pkg/api/v2/mock/unsafe_mock.go -package mock
"$MOCKGEN" -source pkg/api/v2/status.go -destination pkg/api/v2/mock/status_mock.go -package mock
"$MOCKGEN" -source pkg/api/v2/capture.go -destination pkg/api/v2/mock/capture_mock.go -package mock
"$MOCKGEN" -source pkg/api/v2/processor.go -destination pkg/api/v2/mock/processor_mock.go -package mock
"$MOCKGEN" -source pkg/api/v2/changefeed.go -destination pkg/api/v2/mock/changefeed_mock.go -package mock
"$MOCKGEN" -source pkg/sink/codec/simple/marshaller.go -destination pkg/sink/codec/simple/mock/marshaller.go
"$MOCKGEN" -source pkg/keyspace/keyspace_manager.go -destination pkg/keyspace/keyspace_manager_mock.go -package keyspace
