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

package pulsar

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestGetMetricRegistryReturnsSharedRegistryConcurrently(t *testing.T) {
	restoreMetricRegistryForTest(t, nil)

	const workerCount = 32
	results := make(chan *prometheus.Registry, workerCount)

	var wg sync.WaitGroup
	for range workerCount {
		wg.Go(func() {
			results <- getMetricRegistry()
		})
	}

	wg.Wait()
	close(results)

	var shared *prometheus.Registry
	for registry := range results {
		require.NotNil(t, registry)
		if shared == nil {
			shared = registry
			continue
		}
		require.Same(t, shared, registry)
	}
	require.Same(t, prometheus.DefaultRegisterer.(*prometheus.Registry), shared)
}

func TestGetMetricRegistryReturnsPreconfiguredRegistry(t *testing.T) {
	registry := prometheus.NewRegistry()
	restoreMetricRegistryForTest(t, nil)
	RegisterMetrics(registry)

	require.Same(t, registry, getMetricRegistry())
}

func restoreMetricRegistryForTest(t *testing.T, registry *prometheus.Registry) {
	t.Helper()

	metricRegistryMu.Lock()
	original := metricRegistry
	metricRegistry = registry
	metricRegistryMu.Unlock()

	t.Cleanup(func() {
		metricRegistryMu.Lock()
		metricRegistry = original
		metricRegistryMu.Unlock()
	})
}
