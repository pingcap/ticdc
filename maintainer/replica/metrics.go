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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package replica

import (
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// checker related
var splitSpanCheckDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "ticdc",
		Subsystem: "maintainer",
		Name:      "split_span_check_duration",
		Help:      "Bucketed histogram of split span check time (s).",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms~524s
	}, []string{metrics.GetKeyspaceLabel(), "changefeed", "group_id"})

// RegisterMetrics registers the metrics owned by this package.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(splitSpanCheckDuration)
}
