// Copyright 2020 PingCAP, Inc.
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

package dispatcher

import (
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// handleDDLHistogram records the handling time of a DDL,
// which includes the time of executing the DDL and waiting for the DDL to be resolved.
var handleDDLHistogram = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "ddl",
		Name:      "handle_duration",
		Help:      "Bucketed histogram of handling time (s) of a ddl.",
		Buckets:   prometheus.ExponentialBuckets(0.01, 2, 18),
	}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(handleDDLHistogram)
}
