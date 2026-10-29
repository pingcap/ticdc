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

package lake

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestSpoolMetricsLifecycle(t *testing.T) {
	registry := prometheus.NewRegistry()
	InitMetrics(registry)
	id := common.NewChangeFeedIDWithName("lake-metrics-test", "default")
	spoolMetrics := newSpoolMetrics(id)
	t.Cleanup(spoolMetrics.Close)
	spoolMetrics.MemoryBytes.Set(10)
	spoolMetrics.DiskBytes.Set(20)
	spoolMetrics.PendingPostEnqueue.Set(1)
	spoolMetrics.DiskQuotaWaiters.Set(1)
	spoolMetrics.SegmentCount.Set(1)
	spoolMetrics.DiskQuotaWait.Observe(0.01)
	spoolMetrics.LoadedBytes.Observe(1024)
	spoolMetrics.RotatedCount.Inc()

	families, err := registry.Gather()
	require.NoError(t, err)
	names := make([]string, 0, len(families))
	for _, family := range families {
		names = append(names, family.GetName())
	}
	require.ElementsMatch(t, []string{
		"ticdc_lake_buffer_bytes", "ticdc_lake_buffer_entries",
		"ticdc_lake_spool_wait_seconds", "ticdc_lake_spool_load_bytes",
		"ticdc_lake_spool_rotations_total",
	}, names)

	spoolMetrics.Close()
	families, err = registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "changefeed" {
					require.NotEqual(t, id.Name(), label.GetValue(), "closed feed metric remains in %s", family.GetName())
				}
			}
		}
	}
}
