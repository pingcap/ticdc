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
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestTrackDMLEvent(t *testing.T) {
	stats := NewStatistics(common.NewChangefeedID4Test("test", t.Name()), 0)
	t.Cleanup(stats.Close)
	event := &commonEvent.DMLEvent{Length: 2, ApproximateSize: 10}
	called := false
	event.AddPostFlushFunc(func() { called = true })

	stats.TrackDMLEvent(event)
	metric := &dto.Metric{}
	require.NoError(t, stats.metricExecBatchHis.(prometheus.Metric).Write(metric))
	require.Zero(t, metric.GetHistogram().GetSampleCount())
	require.NoError(t, stats.metricExecBatchBytesHis.(prometheus.Metric).Write(metric))
	require.Zero(t, metric.GetHistogram().GetSampleCount())
	require.False(t, called)

	event.Length = 5
	event.ApproximateSize = 100
	event.PostFlush()
	require.NoError(t, stats.metricExecBatchHis.(prometheus.Metric).Write(metric))
	require.Equal(t, float64(2), metric.GetHistogram().GetSampleSum())
	require.NoError(t, stats.metricExecBatchBytesHis.(prometheus.Metric).Write(metric))
	require.Equal(t, float64(10), metric.GetHistogram().GetSampleSum())
	require.True(t, called)
}
