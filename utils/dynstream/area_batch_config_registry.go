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

import (
	"sync"
	"sync/atomic"
)

type areaBatchConfigRegistry[A Area] struct {
	defaultConfig batchConfig

	// The configs are stored as an immutable map in atomic.Value, so reads in the hot path
	// don't need locks. Updates are copy-on-write and guarded by mu.
	configs atomic.Value // map[A]batchConfig

	mu           sync.Mutex
	areaRefCount map[A]int
}

func newAreaBatchConfigRegistry[A Area](defaultConfig batchConfig) *areaBatchConfigRegistry[A] {
	s := &areaBatchConfigRegistry[A]{
		defaultConfig: defaultConfig,
		areaRefCount:  make(map[A]int),
	}
	s.configs.Store(make(map[A]batchConfig))
	return s
}

func (s *areaBatchConfigRegistry[A]) getBatchConfig(area A) batchConfig {
	configs := s.configs.Load().(map[A]batchConfig)
	if config, ok := configs[area]; ok {
		return config
	}
	return s.defaultConfig
}

func (s *areaBatchConfigRegistry[A]) onAddPath(area A, cfg batchConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldCount := s.areaRefCount[area]
	s.areaRefCount[area] = oldCount + 1
	if oldCount > 0 {
		// First-add semantics: once an area already has paths, later AddPath calls
		// should not overwrite its batch config.
		return
	}

	// Zero config means no explicit area batch config was provided.
	if cfg.count <= 0 && cfg.bytes <= 0 {
		return
	}

	newConfig := NewBatchConfig(cfg.count, cfg.bytes)
	if newConfig == s.defaultConfig {
		return
	}

	s.setOverrideLocked(area, newConfig)
}

func (s *areaBatchConfigRegistry[A]) onRemovePath(area A) {
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

func (s *areaBatchConfigRegistry[A]) setOverrideLocked(area A, config batchConfig) {
	oldConfigs := s.configs.Load().(map[A]batchConfig)
	newConfigs := make(map[A]batchConfig, len(oldConfigs)+1)
	for k, v := range oldConfigs {
		newConfigs[k] = v
	}
	newConfigs[area] = config
	s.configs.Store(newConfigs)
}

func (s *areaBatchConfigRegistry[A]) removeOverrideLocked(area A) {
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
