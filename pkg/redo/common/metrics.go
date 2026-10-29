// Copyright 2026 PingCAP, Inc.
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

package common

import (
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "ticdc"
	subsystem = "redo"
)

// RedoWorkerBusyRatio records the busy time of redo writer workers.
var RedoWorkerBusyRatio = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "worker_busy_ratio",
		Help:      "Busy time in seconds for redo writer workers.",
	}, []string{metrics.GetKeyspaceLabel(), "changefeed", "type"})

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(RedoWorkerBusyRatio)
}
