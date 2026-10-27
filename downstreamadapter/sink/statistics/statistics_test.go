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

package statistics

import (
	"errors"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func resetMetrics() {
	execBatchHistogram.Reset()
	execBatchWriteBytesHistogram.Reset()
	executionErrorCounter.Reset()
	execDDLHistogram.Reset()
	execDDLRunningGauge.Reset()
	execDDLCounter.Reset()
}

func requireMetricHasLabel(
	t *testing.T,
	collector prometheus.Collector,
	labelName string,
	labelValue string,
) {
	t.Helper()

	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, metricFamilies)

	for _, metricFamily := range metricFamilies {
		for _, metric := range metricFamily.Metric {
			for _, label := range metric.Label {
				if label.GetName() == labelName && label.GetValue() == labelValue {
					return
				}
			}
		}
	}
	require.Failf(t, "metric label not found", "%s=%q", labelName, labelValue)
}

func TestStatisticsLifecycle(t *testing.T) {
	resetMetrics()
	t.Cleanup(resetMetrics)

	changefeedID := common.NewChangefeedID4Test("test-keyspace", "statistics-lifecycle")
	stats := New(changefeedID, 123)

	ddlErr := errors.New("ddl failed")
	stats.RecordDMLBatch(2, 10)
	stats.RecordDMLError()
	require.NoError(t, stats.RecordDDLExecution("CREATE TABLE", func() error { return nil }))
	require.ErrorIs(t, stats.RecordDDLExecution("ALTER TABLE", func() error { return ddlErr }), ddlErr)

	require.Equal(t, 1, testutil.CollectAndCount(execBatchHistogram))
	requireMetricHasLabel(t, execBatchHistogram, "keyspace_id", "123")
	require.Equal(t, 1, testutil.CollectAndCount(execBatchWriteBytesHistogram))
	require.Equal(t, 1, testutil.CollectAndCount(execDDLRunningGauge))
	// Only the succeeded DDL observes the duration.
	require.Equal(t, 1, testutil.CollectAndCount(execDDLHistogram))
	require.Equal(t, 1, testutil.CollectAndCount(execDDLCounter))
	// One ddl error and one dml error.
	require.Equal(t, 2, testutil.CollectAndCount(executionErrorCounter))

	stats.Close()

	require.Zero(t, testutil.CollectAndCount(execBatchHistogram))
	require.Zero(t, testutil.CollectAndCount(execBatchWriteBytesHistogram))
	require.Zero(t, testutil.CollectAndCount(execDDLRunningGauge))
	require.Zero(t, testutil.CollectAndCount(execDDLHistogram))
	require.Zero(t, testutil.CollectAndCount(execDDLCounter))
	require.Zero(t, testutil.CollectAndCount(executionErrorCounter))

	// Recording after Close must not recreate the deleted series.
	stats.RecordDMLBatch(1, 1)
	stats.RecordDMLError()
	require.Zero(t, testutil.CollectAndCount(execBatchHistogram))
	require.Zero(t, testutil.CollectAndCount(execBatchWriteBytesHistogram))
	require.Zero(t, testutil.CollectAndCount(executionErrorCounter))
}
