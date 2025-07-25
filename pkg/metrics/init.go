// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/prometheus/client_golang/prometheus"
)

func InitMetrics(registry *prometheus.Registry) {
	InitServerMetrics(registry)
	InitSchedulerMetrics(registry)
	InitChangefeedMetrics(registry)
	InitDispatcherMetrics(registry)
	InitMessagingMetrics(registry)
	InitSinkMetrics(registry)
	InitPullerMetrics(registry)
	InitEventStoreMetrics(registry)
	InitSchemaStoreMetrics(registry)
	InitEventServiceMetrics(registry)
	InitMaintainerMetrics(registry)
	InitCoordinatorMetrics(registry)
	InitLogPullerMetrics(registry)
	common.InitCommonMetrics(registry)
	InitDynamicStreamMetrics(registry)
	kafka.InitMetrics(registry)
	codec.InitMetrics(registry)
	gc.InitMetrics(registry)
	metrics.InitCloudStorageMetrics(registry)
}
