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
	"strings"
	"sync"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/prometheus/client_golang/prometheus"
)

// execDMLEventRowsAffectedCounter records the affected row counts reported by
// the downstream MySQL, which is a MySQL-sink-specific metric.
var execDMLEventRowsAffectedCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "ticdc",
		Subsystem: "sink",
		Name:      "dml_event_affected_row_count",
		Help:      "Total count of affected rows.",
	}, []string{getKeyspaceLabel(), "changefeed", "count_type", "row_type"})

// InitMetrics registers the MySQL sink metrics.
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(execDMLEventRowsAffectedCounter)
}

// affectedRowsRecorder accumulates affected row statistics for one changefeed.
type affectedRowsRecorder struct {
	keyspace        string
	changefeed      string
	rowsAffectedMap sync.Map
}

func newAffectedRowsRecorder(changefeedID common.ChangeFeedID) *affectedRowsRecorder {
	return &affectedRowsRecorder{
		keyspace:   changefeedID.Keyspace(),
		changefeed: changefeedID.Name(),
	}
}

func (r *affectedRowsRecorder) recordTotalRowsAffected(actualRowsAffected, expectedRowsAffected int64) {
	r.getRowsAffected("actual", "total").Add(float64(actualRowsAffected))
	r.getRowsAffected("expected", "total").Add(float64(expectedRowsAffected))
}

func (r *affectedRowsRecorder) recordRowsAffected(rowsAffected int64, rowType common.RowType) {
	r.getRowsAffected("actual", rowType.String()).Add(float64(rowsAffected))
	r.getRowsAffected("expected", rowType.String()).Add(1)
	r.recordTotalRowsAffected(rowsAffected, 1)
}

func (r *affectedRowsRecorder) getRowsAffected(countType, rowType string) prometheus.Counter {
	key := countType + "-" + rowType
	counter, loaded := r.rowsAffectedMap.Load(key)
	if !loaded {
		counter := execDMLEventRowsAffectedCounter.WithLabelValues(r.keyspace, r.changefeed, countType, rowType)
		r.rowsAffectedMap.Store(key, counter)
		return counter
	}
	return counter.(prometheus.Counter)
}

// close removes the per-changefeed metric series.
func (r *affectedRowsRecorder) close() {
	r.rowsAffectedMap.Range(func(key, value any) bool {
		countTypeAndRowType := key.(string)
		splitTypes := strings.Split(countTypeAndRowType, "-")
		execDMLEventRowsAffectedCounter.DeleteLabelValues(r.keyspace, r.changefeed, splitTypes[0], splitTypes[1])
		return true
	})
}

func getKeyspaceLabel() string {
	if kerneltype.IsNextGen() {
		return "keyspace_name"
	}
	return "namespace"
}
