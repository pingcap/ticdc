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
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestExecBatchHistogramKeyspaceIDLabel(t *testing.T) {
	execBatchHistogram.Reset()
	t.Cleanup(execBatchHistogram.Reset)

	statistics := NewStatistics(
		common.NewChangefeedID4Test("test-keyspace", "batch-row-count-keyspace-id"),
		123,
	)
	statistics.RecordDMLResult(2, 10, nil)

	require.Equal(t, 1, testutil.CollectAndCount(execBatchHistogram))
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(execBatchHistogram, execBatchWriteBytesHistogram, totalWriteBytesCounter)
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, metricFamilies, 3)
	for _, family := range metricFamilies {
		require.Len(t, family.Metric, 1)
		labels := make(map[string]string)
		for _, label := range family.Metric[0].Label {
			labels[label.GetName()] = label.GetValue()
		}
		expected := map[string]string{
			getKeyspaceLabel(): "test-keyspace",
			"changefeed":       "batch-row-count-keyspace-id",
		}
		if family.GetName() == "ticdc_sink_batch_row_count" {
			expected["keyspace_id"] = "123"
		}
		require.Equal(t, expected, labels)
	}

	statistics.Close()
	require.Equal(t, 0, testutil.CollectAndCount(execBatchHistogram))
}

func TestRecordDMLResult(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "success"
		if failed {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			stats := NewStatistics(common.NewChangefeedID4Test("test", t.Name()), 0)
			t.Cleanup(stats.Close)
			require.Zero(t, testutil.ToFloat64(stats.metricExecDMLCnt))
			require.Zero(t, testutil.ToFloat64(stats.metricTotalWriteBytesCnt))
			require.Zero(t, testutil.ToFloat64(stats.metricExecErrCntForDML))

			var result error
			if failed {
				result = errors.New("delivery failed")
			}
			stats.RecordDMLResult(2, 10, result)

			batchMetric := &dto.Metric{}
			require.NoError(t, stats.metricExecBatchHis.(prometheus.Metric).Write(batchMetric))
			bytesMetric := &dto.Metric{}
			require.NoError(t, stats.metricExecBatchBytesHis.(prometheus.Metric).Write(bytesMetric))
			if failed {
				require.Zero(t, testutil.ToFloat64(stats.metricExecDMLCnt))
				require.Zero(t, testutil.ToFloat64(stats.metricTotalWriteBytesCnt))
				require.Equal(t, float64(1), testutil.ToFloat64(stats.metricExecErrCntForDML))
				require.Zero(t, batchMetric.GetHistogram().GetSampleCount())
				require.Zero(t, bytesMetric.GetHistogram().GetSampleCount())
			} else {
				require.Equal(t, float64(2), testutil.ToFloat64(stats.metricExecDMLCnt))
				require.Equal(t, float64(10), testutil.ToFloat64(stats.metricTotalWriteBytesCnt))
				require.Zero(t, testutil.ToFloat64(stats.metricExecErrCntForDML))
				require.Equal(t, uint64(1), batchMetric.GetHistogram().GetSampleCount())
				require.Equal(t, float64(2), batchMetric.GetHistogram().GetSampleSum())
				require.Equal(t, uint64(1), bytesMetric.GetHistogram().GetSampleCount())
				require.Equal(t, float64(10), bytesMetric.GetHistogram().GetSampleSum())
			}
		})
	}
}

func TestRecordDDLExecution(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "success"
		if failed {
			name = "failure"
		}
		t.Run(name, func(t *testing.T) {
			stats := NewStatistics(common.NewChangefeedID4Test("test", t.Name()), 0)
			t.Cleanup(stats.Close)
			counter := execDDLCounter.WithLabelValues("test", t.Name(), "add index")

			var result error
			if failed {
				result = errors.New("DDL failed")
			}
			err := stats.RecordDDLExecution(func() (string, error) {
				require.Equal(t, float64(1), testutil.ToFloat64(stats.metricExecDDLRunningCnt))
				require.Zero(t, testutil.ToFloat64(counter))
				return "add index", result
			})
			require.Equal(t, result, err)
			require.Zero(t, testutil.ToFloat64(stats.metricExecDDLRunningCnt))
			metric := &dto.Metric{}
			require.NoError(t, stats.metricExecDDLHis.(prometheus.Metric).Write(metric))
			if failed {
				require.Zero(t, testutil.ToFloat64(counter))
				require.Equal(t, float64(1), testutil.ToFloat64(stats.metricExecErrCntForDDL))
				require.Zero(t, metric.GetHistogram().GetSampleCount())
			} else {
				require.Equal(t, float64(1), testutil.ToFloat64(counter))
				require.Zero(t, testutil.ToFloat64(stats.metricExecErrCntForDDL))
				require.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
			}
		})
	}
}

func TestCloseDeletesMetricSeries(t *testing.T) {
	stats := NewStatistics(common.NewChangefeedID4Test("test", t.Name()), 0)
	registry := prometheus.NewPedanticRegistry()
	InitMetrics(registry)
	stats.RecordDMLResult(2, 10, nil)
	for _, ddlType := range []string{"create table", "drop table"} {
		require.NoError(t, stats.RecordDDLExecution(func() (string, error) {
			return ddlType, nil
		}))
	}
	stats.Close()
	stats.RecordDMLResult(2, 10, nil)
	stats.RecordDMLResult(0, 0, errors.New("delivery failed"))
	executed := false
	require.NoError(t, stats.RecordDDLExecution(func() (string, error) {
		executed = true
		return "add index", nil
	}))
	require.True(t, executed)
	stats.Close()
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.Empty(t, metricFamilies)
}
