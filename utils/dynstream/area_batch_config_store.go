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

package dynstream

// areaBatchConfigStore stores per-area batching config overrides.

import (
	"sync"
	"sync/atomic"
)

type areaBatchConfigStore[A Area] struct {
	defaultConfig batchConfig

	// The configs are stored as an immutable map in atomic.Value, so reads in the hot path
	// don't need locks. Updates are copy-on-write and guarded by mu.
	configs atomic.Value // map[A]batchConfig

	mu           sync.Mutex
	areaRefCount map[A]int
}

func newAreaBatchConfigStore[A Area](defaultConfig batchConfig) *areaBatchConfigStore[A] {
	s := &areaBatchConfigStore[A]{
		defaultConfig: defaultConfig,
		areaRefCount:  make(map[A]int),
	}
	s.configs.Store(make(map[A]batchConfig))
	return s
}

func (s *areaBatchConfigStore[A]) getBatchConfig(area A) batchConfig {
	configs := s.configs.Load().(map[A]batchConfig)
	if config, ok := configs[area]; ok {
		return config
	}
	return s.defaultConfig
}

func (s *areaBatchConfigStore[A]) onAddPath(area A) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.areaRefCount[area]++
}

func (s *areaBatchConfigStore[A]) onRemovePath(area A) {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldCount := s.areaRefCount[area]
	if oldCount <= 1 {
		delete(s.areaRefCount, area)
		s.removeOverrideLocked(area)
		return
	}
	s.areaRefCount[area] = oldCount - 1
}

func (s *areaBatchConfigStore[A]) setAreaBatchConfig(area A, batchCount uint64, batchBytes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.areaRefCount[area] == 0 {
		// Keep the same semantics as SetAreaSettings: avoid leaking per-area overrides for areas
		// without any existing paths.
		return
	}

	newConfig := newBatchConfig(batchCount, batchBytes)

	if newConfig == s.defaultConfig {
		s.removeOverrideLocked(area)
		return
	}

	configs := s.configs.Load().(map[A]batchConfig)
	if currentConfig, ok := configs[area]; ok && currentConfig == newConfig {
		return
	}
	s.setOverrideLocked(area, newConfig)
}

func (s *areaBatchConfigStore[A]) setOverrideLocked(area A, config batchConfig) {
	oldConfigs := s.configs.Load().(map[A]batchConfig)
	newConfigs := make(map[A]batchConfig, len(oldConfigs)+1)
	for k, v := range oldConfigs {
		newConfigs[k] = v
	}
	newConfigs[area] = config
	s.configs.Store(newConfigs)
}

func (s *areaBatchConfigStore[A]) removeOverrideLocked(area A) {
	oldConfigs := s.configs.Load().(map[A]batchConfig)
	if _, ok := oldConfigs[area]; !ok {
		return
	}
	newConfigs := make(map[A]batchConfig, len(oldConfigs)-1)
	for k, v := range oldConfigs {
		if k == area {
			continue
		}
		newConfigs[k] = v
	}
	s.configs.Store(newConfigs)
}
