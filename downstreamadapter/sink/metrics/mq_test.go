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

package metrics

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestGetMQMetricRegistryReturnsSharedRegistryConcurrently(t *testing.T) {
	restoreMQMetricRegistryForTest(t, nil)

	const workerCount = 32
	results := make(chan *prometheus.Registry, workerCount)

	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			results <- GetMQMetricRegistry()
		}()
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

func TestGetMQMetricRegistryReturnsPreconfiguredRegistry(t *testing.T) {
	registry := prometheus.NewRegistry()
	restoreMQMetricRegistryForTest(t, registry)

	require.Same(t, registry, GetMQMetricRegistry())
}

func restoreMQMetricRegistryForTest(t *testing.T, registry *prometheus.Registry) {
	t.Helper()

	mqServerRegistryMu.Lock()
	original := mqServerRegistry
	mqServerRegistry = registry
	mqServerRegistryMu.Unlock()

	t.Cleanup(func() {
		mqServerRegistryMu.Lock()
		mqServerRegistry = original
		mqServerRegistryMu.Unlock()
	})
}
