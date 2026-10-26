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
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	registry.MustRegister(execBatchHistogram)
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, metricFamilies, 1)
	require.Len(t, metricFamilies[0].Metric, 1)
	labels := make(map[string]string)
	for _, label := range metricFamilies[0].Metric[0].Label {
		labels[label.GetName()] = label.GetValue()
	}
	require.Equal(t, "123", labels["keyspace_id"])

	statistics.Close()
	require.Equal(t, 0, testutil.CollectAndCount(execBatchHistogram))
}

func TestTrackDMLEvent(t *testing.T) {
	stats := NewStatistics(common.NewChangefeedID4Test("test", t.Name()), 0)
	t.Cleanup(stats.Close)
	event := &commonEvent.DMLEvent{Length: 2, ApproximateSize: 10}
	called := false
	event.AddPostFlushFunc(func() { called = true })

	stats.TrackDMLEvent(event)
	require.Zero(t, testutil.ToFloat64(stats.metricExecDMLCnt))
	require.False(t, called)
	event.PostFlush()
	require.Equal(t, float64(2), testutil.ToFloat64(stats.metricExecDMLCnt))
	require.Equal(t, float64(10), testutil.ToFloat64(stats.metricTotalWriteBytesCnt))
	require.True(t, called)
}
