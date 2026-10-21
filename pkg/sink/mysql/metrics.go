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

package mysql

import (
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	execDMLEventRowsAffectedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "dml_event_affected_row_count",
			Help:      "Total count of affected rows.",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed", "count_type", "row_type"},
	)

	activeActiveConflictSkipRowsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "sink",
			Name:      "active_active_conflict_skip_rows_total",
			Help:      "Total number of rows skipped due to last-write-wins conflict resolution in TiDB active-active replication.",
		}, []string{metrics.GetKeyspaceLabel(), "changefeed"})

	captureLastWriteAdmissionTimestamp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "server",
			Name:      "capture_last_write_admission_timestamp_seconds",
			Help:      "Unix timestamp of the most recent downstream write admitted by this capture.",
		})
)

// RegisterMetrics registers MySQL sink metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(captureLastWriteAdmissionTimestamp)
	registry.MustRegister(execDMLEventRowsAffectedCounter)
	registry.MustRegister(activeActiveConflictSkipRowsCounter)
}

// DeleteDMLEventRowsAffectedMetrics deletes affected-row metric series for a MySQL sink.
func DeleteDMLEventRowsAffectedMetrics(changefeedID common.ChangeFeedID) {
	execDMLEventRowsAffectedCounter.DeletePartialMatch(prometheus.Labels{
		metrics.GetKeyspaceLabel(): changefeedID.Keyspace(),
		"changefeed":               changefeedID.Name(),
	})
}
