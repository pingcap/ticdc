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
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/prometheus/client_golang/prometheus"
)

// RegisterMetrics registers the metrics defined in this package.
func RegisterMetrics(registry *prometheus.Registry) {
	initServerMetrics(registry)
	initSchedulerMetrics(registry)
	initChangefeedMetrics(registry)
	initDispatcherMetrics(registry)
	initSinkMetrics(registry)
	initEventStoreMetrics(registry)
	initEventServiceMetrics(registry)
	initMaintainerMetrics(registry)
	initCoordinatorMetrics(registry)
	initLogPullerMetrics(registry)
	initDynamicStreamMetrics(registry)
	initCaptureWriteLeaseMetrics(registry)

	initRedoMetrics(registry)
	initDDLMetrics(registry)
}

// GetKeyspaceLabel returns the keyspace label name used by TiCDC metrics.
func GetKeyspaceLabel() string {
	if kerneltype.IsNextGen() {
		return "keyspace_name"
	}
	return "namespace"
}
