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
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/prometheus/client_golang/prometheus"
)

func InitMetrics(registry *prometheus.Registry) {
	initServerMetrics(registry)
	initSchedulerMetrics(registry)
	initChangefeedMetrics(registry)
	initLogCoordinatorMetrics(registry)
	initDispatcherMetrics(registry)
	initMessagingMetrics(registry)
	initSinkMetrics(registry)
	initPullerMetrics(registry)
	initEventStoreMetrics(registry)
	initSchemaStoreMetrics(registry)
	initEventServiceMetrics(registry)
	initMaintainerMetrics(registry)
	initCoordinatorMetrics(registry)
	initLogPullerMetrics(registry)
	common.InitCommonMetrics(registry)
	initDynamicStreamMetrics(registry)

	kafka.InitMetrics(registry)
	gc.InitMetrics(registry)
	metrics.InitCloudStorageMetrics(registry)
	InitRedoMetrics(registry)
}

func getKeyspaceLabel() string {
	if kerneltype.IsNextGen() {
		return "keyspace_name"
	}
	return "namespace"
}
