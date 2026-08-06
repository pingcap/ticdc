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

package metrics

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestWatermarkLagCollectorCollectsAtomicPairs(t *testing.T) {
	collector := newWatermarkLagCollector()
	gauge := collector.WithLabelValues("default", "atomic-watermark")
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(collector)

	done := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for {
			select {
			case <-done:
				return
			default:
				gauge.Set(0.9, 0.3)
				gauge.Set(0.2, 0.1)
			}
		}
	}()

	for range 1_000 {
		metricFamilies, err := registry.Gather()
		require.NoError(t, err)
		var checkpointLag, resolvedLag float64
		for _, family := range metricFamilies {
			switch family.GetName() {
			case "ticdc_maintainer_checkpoint_ts_lag":
				checkpointLag = family.Metric[0].Gauge.GetValue()
			case "ticdc_maintainer_resolved_ts_lag":
				resolvedLag = family.Metric[0].Gauge.GetValue()
			}
		}
		require.LessOrEqual(t, resolvedLag, checkpointLag)
	}
	close(done)
	writer.Wait()

	collector.DeleteLabelValues("default", "atomic-watermark")
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.Empty(t, metricFamilies)
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

func TestResetOwnerChangefeedMetrics(t *testing.T) {
	ResetOwnerChangefeedMetrics()
	t.Cleanup(ResetOwnerChangefeedMetrics)

	keyspace := "default"
	changefeed := "reset-owner-changefeed-metrics"
	keyspaceID := "123"

	ChangefeedStatusGauge.WithLabelValues(keyspace, changefeed, keyspaceID).Set(1)
	ChangefeedErrorInfoGauge.WithLabelValues(keyspace, changefeed, "failed", "1000", "CDC:ErrTest", "test").Set(1)
	ChangefeedCheckpointTsGauge.WithLabelValues(keyspace, changefeed).Set(100)
	ChangefeedCheckpointTsLagGauge.WithLabelValues(keyspace, changefeed, keyspaceID).Set(10)
	ChangefeedDownstreamInfoGauge.WithLabelValues(keyspace, changefeed, "mysql/tidb").Set(1)

	require.Equal(t, 1, testutil.CollectAndCount(ChangefeedStatusGauge))
	require.Equal(t, 1, testutil.CollectAndCount(ChangefeedErrorInfoGauge))
	require.Equal(t, 1, testutil.CollectAndCount(ChangefeedCheckpointTsGauge))
	require.Equal(t, 1, testutil.CollectAndCount(ChangefeedCheckpointTsLagGauge))
	require.Equal(t, 1, testutil.CollectAndCount(ChangefeedDownstreamInfoGauge))
	requireMetricHasLabel(t, ChangefeedStatusGauge, "keyspace_id", keyspaceID)
	requireMetricHasLabel(t, ChangefeedCheckpointTsLagGauge, "keyspace_id", keyspaceID)

	ResetOwnerChangefeedMetrics()

	require.Equal(t, 0, testutil.CollectAndCount(ChangefeedStatusGauge))
	require.Equal(t, 0, testutil.CollectAndCount(ChangefeedErrorInfoGauge))
	require.Equal(t, 0, testutil.CollectAndCount(ChangefeedCheckpointTsGauge))
	require.Equal(t, 0, testutil.CollectAndCount(ChangefeedCheckpointTsLagGauge))
	require.Equal(t, 0, testutil.CollectAndCount(ChangefeedDownstreamInfoGauge))
}
