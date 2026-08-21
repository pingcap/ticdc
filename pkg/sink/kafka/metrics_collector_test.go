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

package kafka

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/require"
)

func TestCollectBrokerThrottleTime(t *testing.T) {
	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "throttle-time")

	registry := metrics.NewRegistry()
	firstBroker := metrics.NewHistogram(metrics.NewUniformSample(10))
	firstBroker.Update(10)
	firstBroker.Update(50)
	require.NoError(t, registry.Register(
		getBrokerMetricName(throttleTimeMetricNamePrefix, "1"), firstBroker))

	secondBroker := metrics.NewHistogram(metrics.NewUniformSample(10))
	secondBroker.Update(40)
	require.NoError(t, registry.Register(
		getBrokerMetricName(throttleTimeMetricNamePrefix, "2"), secondBroker))

	collector := saramaMetricsCollector{
		changefeedID: changefeedID,
		brokers:      map[int32]struct{}{1: {}, 2: {}},
		registry:     registry,
	}
	collector.collectBrokerMetrics()

	require.Equal(t, 0.03, testutil.ToFloat64(throttleTimeGauge.WithLabelValues(
		changefeedID.Keyspace(), changefeedID.Name(), "1", avg)))
	require.Equal(t, 0.05, testutil.ToFloat64(throttleTimeGauge.WithLabelValues(
		changefeedID.Keyspace(), changefeedID.Name(), "1", p99)))
	require.Equal(t, 0.04, testutil.ToFloat64(throttleTimeGauge.WithLabelValues(
		changefeedID.Keyspace(), changefeedID.Name(), "2", avg)))
	require.Equal(t, 0.04, testutil.ToFloat64(throttleTimeGauge.WithLabelValues(
		changefeedID.Keyspace(), changefeedID.Name(), "2", p99)))

	collector.cleanupMetrics()
	require.False(t, throttleTimeGauge.DeleteLabelValues(
		changefeedID.Keyspace(), changefeedID.Name(), "1", avg))
	require.False(t, throttleTimeGauge.DeleteLabelValues(
		changefeedID.Keyspace(), changefeedID.Name(), "1", p99))
	require.False(t, throttleTimeGauge.DeleteLabelValues(
		changefeedID.Keyspace(), changefeedID.Name(), "2", avg))
	require.False(t, throttleTimeGauge.DeleteLabelValues(
		changefeedID.Keyspace(), changefeedID.Name(), "2", p99))
}
