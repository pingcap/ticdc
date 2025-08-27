// Copyright 2024 PingCAP, Inc.
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

package txnsink

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestProgressTrackerMetrics(t *testing.T) {
	// Create a new registry for testing
	registry := prometheus.NewRegistry()
	metrics.InitSinkMetrics(registry)

	// Create progress tracker with monitoring
	pt := NewProgressTrackerWithMonitor("test_namespace", "test_changefeed")
	defer pt.Close()

	// Add some pending transactions
	pt.AddPendingTxn(1000, 900)
	pt.AddPendingTxn(2000, 1900)

	// Update checkpoint ts
	pt.UpdateCheckpointTs(500)

	// Wait a bit for the monitor to run
	time.Sleep(2 * time.Second)

	// Check if the metric is registered and has a value
	metricValue := testutil.ToFloat64(metrics.TxnSinkProgressLagGauge.WithLabelValues("test_namespace", "test_changefeed"))
	require.Greater(t, metricValue, float64(0), "Progress lag metric should be greater than 0")

	// Verify the metric is working correctly
	require.NotZero(t, metricValue, "Progress lag metric should not be zero")
}
