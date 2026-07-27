// Copyright 2025 PingCAP, Inc.
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

package eventservice

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/metrics"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestBigTxnMetricTracker(t *testing.T) {
	t.Run("count split txn once", func(t *testing.T) {
		beforeHistogramCount, beforeHistogramSum := readBigTxnSizeMetric(t)
		beforeCounter := readBigTxnCountMetric(t)

		tracker := bigTxnMetricTracker{}
		tracker.addFragment(100, 200, 70, 50)

		histogramCount, histogramSum := readBigTxnSizeMetric(t)
		require.Equal(t, beforeHistogramCount, histogramCount)
		require.Equal(t, beforeHistogramSum, histogramSum)
		require.Equal(t, beforeCounter, readBigTxnCountMetric(t))

		tracker.finishTxn(100, 200, 30, 50)

		histogramCount, histogramSum = readBigTxnSizeMetric(t)
		require.Equal(t, beforeHistogramCount+1, histogramCount)
		require.Equal(t, beforeHistogramSum+100, histogramSum)
		require.Equal(t, beforeCounter+1, readBigTxnCountMetric(t))
		require.Nil(t, tracker.pending)
	})

	t.Run("flush previous txn", func(t *testing.T) {
		beforeHistogramCount, beforeHistogramSum := readBigTxnSizeMetric(t)
		beforeCounter := readBigTxnCountMetric(t)

		tracker := bigTxnMetricTracker{}
		tracker.addFragment(100, 200, 70, 50)
		tracker.addFragment(101, 201, 80, 50)

		histogramCount, histogramSum := readBigTxnSizeMetric(t)
		require.Equal(t, beforeHistogramCount+1, histogramCount)
		require.Equal(t, beforeHistogramSum+70, histogramSum)
		require.Equal(t, beforeCounter+1, readBigTxnCountMetric(t))
		require.Equal(t, &pendingBigTxnMetric{
			startTs:    101,
			commitTs:   201,
			rawKVBytes: 80,
		}, tracker.pending)
	})

	t.Run("flush at scan end", func(t *testing.T) {
		beforeHistogramCount, beforeHistogramSum := readBigTxnSizeMetric(t)
		beforeCounter := readBigTxnCountMetric(t)

		tracker := bigTxnMetricTracker{}
		tracker.addFragment(100, 200, 70, 50)
		tracker.flush()

		histogramCount, histogramSum := readBigTxnSizeMetric(t)
		require.Equal(t, beforeHistogramCount+1, histogramCount)
		require.Equal(t, beforeHistogramSum+70, histogramSum)
		require.Equal(t, beforeCounter+1, readBigTxnCountMetric(t))
		require.Nil(t, tracker.pending)
	})
}

func readBigTxnSizeMetric(t *testing.T) (uint64, float64) {
	t.Helper()

	metric := &dto.Metric{}
	require.NoError(t, metrics.EventServiceBigTxnSize.Write(metric))
	histogram := metric.GetHistogram()
	require.NotNil(t, histogram)
	return histogram.GetSampleCount(), histogram.GetSampleSum()
}

func readBigTxnCountMetric(t *testing.T) float64 {
	t.Helper()

	metric := &dto.Metric{}
	require.NoError(t, metrics.EventServiceBigTxnCount.Write(metric))
	counter := metric.GetCounter()
	require.NotNil(t, counter)
	return counter.GetValue()
}
