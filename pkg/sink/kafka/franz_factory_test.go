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

package kafka

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	kafkafranz "github.com/pingcap/ticdc/pkg/sink/kafka/franz"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestFranzFactoryMetricsCollectorType(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "franz-metrics-type")
	f := &franzFactory{
		changefeedID: changefeedID,
		metricsHook:  kafkafranz.NewMetricsHook(),
	}
	collector := f.MetricsCollector(nil)

	_, ok := collector.(*franzMetricsCollector)
	require.True(t, ok)
}

func TestFranzMetricsCollectorCollectMetrics(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "franz-metrics")
	hook := kafkafranz.NewMetricsHook()
	collector := &franzMetricsCollector{
		changefeedID: changefeedID,
		hook:         hook,
	}
	t.Cleanup(func() {
		collector.cleanupMetrics()
	})

	hook.RecordBrokerWrite(1, 128, nil)
	hook.RecordProduceBatchWritten(8, 400, 200)

	collector.collectMetrics()

	keyspace := changefeedID.Keyspace()
	changefeed := changefeedID.Name()
	require.Equal(t, float64(1), testutil.ToFloat64(requestsInFlightGauge.WithLabelValues(keyspace, changefeed, "1")))
	require.Greater(t, testutil.ToFloat64(compressionRatioGauge.WithLabelValues(keyspace, changefeed, avg)), 0.0)
	require.Greater(t, testutil.ToFloat64(recordsPerRequestGauge.WithLabelValues(keyspace, changefeed, avg)), 0.0)
}
