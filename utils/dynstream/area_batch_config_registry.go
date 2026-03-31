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

import "sync/atomic"

type areaBatchConfigRegistry[A Area] struct {
	defaultConfig batchConfig

	// The configs are stored as an immutable map in atomic.Value
	// getBatchConfig can stay lock-free on the hot path (popEvents).
	// The tradeoff is copy-on-write on updates.
	configs atomic.Value // map[A]batchConfig

	// areaRefCount is not thread safe by itself.
	// Callers must serialize updates externally
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

// onAddPath is not thread safe.
func (s *areaBatchConfigRegistry[A]) onAddPath(area A, cfg batchConfig) {
	oldCount := s.areaRefCount[area]
	s.areaRefCount[area] = oldCount + 1
	if oldCount > 0 {
		// First-add semantics: once an area already has paths, later AddPath calls
		// should not overwrite its batch config.
		return
	}

	// Zero config means no explicit area batch config was provided.
	if cfg.softCount <= 0 && cfg.hardBytes <= 0 {
		return
	}

	if cfg == s.defaultConfig {
		return
	}

	s.setOverrideLocked(area, cfg)
}

// onRemovePath is not thread safe.
func (s *areaBatchConfigRegistry[A]) onRemovePath(area A) {
	oldCount := s.areaRefCount[area]
	if oldCount <= 1 {
		delete(s.areaRefCount, area)
		s.removeOverrideLocked(area)
		return
	}
	s.areaRefCount[area] = oldCount - 1
}

func (s *areaBatchConfigRegistry[A]) setOverrideLocked(area A, config batchConfig) {
	// Copy-on-write update:
	// - Read path is lock-free and wait-free (single atomic load + map lookup).
	// - Write path is O(n) in override-count, but writes are expected to be infrequent
	//   compared to reads, and n is usually small because only overridden areas are stored.
	oldConfigs := s.configs.Load().(map[A]batchConfig)
	newConfigs := make(map[A]batchConfig, len(oldConfigs)+1)
	for k, v := range oldConfigs {
		newConfigs[k] = v
	}
	newConfigs[area] = config
	s.configs.Store(newConfigs)
}

func (s *areaBatchConfigRegistry[A]) removeOverrideLocked(area A) {
	// Same copy-on-write rationale as setOverrideLocked.
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
