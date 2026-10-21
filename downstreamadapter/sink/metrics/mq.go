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

	"github.com/prometheus/client_golang/prometheus"
)

var (
	mqServerRegistryMu sync.RWMutex
	// mqServerRegistry is shared by all MQ sinks on the node. Bootstrap can now
	// create multiple changefeeds concurrently, so both reads and the fallback
	// initialization must be synchronized to avoid racing on the global pointer.
	mqServerRegistry *prometheus.Registry
)

// GetMQMetricRegistry for add pulsar default metrics
func GetMQMetricRegistry() *prometheus.Registry {
	mqServerRegistryMu.RLock()
	registry := mqServerRegistry
	mqServerRegistryMu.RUnlock()
	if registry != nil {
		return registry
	}

	mqServerRegistryMu.Lock()
	defer mqServerRegistryMu.Unlock()
	// Make sure registry is not nil when MQ sink metrics are first requested
	// before the server metrics bootstrap wires in the shared registry.
	if mqServerRegistry == nil {
		mqServerRegistry = prometheus.DefaultRegisterer.(*prometheus.Registry)
	}
	return mqServerRegistry
}
