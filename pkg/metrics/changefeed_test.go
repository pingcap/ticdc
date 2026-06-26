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
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestResetOwnerChangefeedMetrics(t *testing.T) {
	ResetOwnerChangefeedMetrics()
	t.Cleanup(ResetOwnerChangefeedMetrics)

	keyspace := "default"
	changefeed := "reset-owner-changefeed-metrics"

	ChangefeedStatusGauge.WithLabelValues(keyspace, changefeed).Set(1)
	ChangefeedErrorInfoGauge.WithLabelValues(keyspace, changefeed, "failed", "1000", "CDC:ErrTest", "test").Set(1)
	ChangefeedCheckpointTsGauge.WithLabelValues(keyspace, changefeed).Set(100)
	ChangefeedCheckpointTsLagGauge.WithLabelValues(keyspace, changefeed).Set(10)
	ChangefeedDownstreamInfoGauge.WithLabelValues(keyspace, changefeed, "mysql/tidb").Set(1)

	require.Equal(t, 1, testutil.CollectAndCount(ChangefeedStatusGauge))
	require.Equal(t, 1, testutil.CollectAndCount(ChangefeedErrorInfoGauge))
	require.Equal(t, 1, testutil.CollectAndCount(ChangefeedCheckpointTsGauge))
	require.Equal(t, 1, testutil.CollectAndCount(ChangefeedCheckpointTsLagGauge))
	require.Equal(t, 1, testutil.CollectAndCount(ChangefeedDownstreamInfoGauge))

	ResetOwnerChangefeedMetrics()

	require.Equal(t, 0, testutil.CollectAndCount(ChangefeedStatusGauge))
	require.Equal(t, 0, testutil.CollectAndCount(ChangefeedErrorInfoGauge))
	require.Equal(t, 0, testutil.CollectAndCount(ChangefeedCheckpointTsGauge))
	require.Equal(t, 0, testutil.CollectAndCount(ChangefeedCheckpointTsLagGauge))
	require.Equal(t, 0, testutil.CollectAndCount(ChangefeedDownstreamInfoGauge))
}
