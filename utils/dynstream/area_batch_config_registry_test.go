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

// Tests for per-area batch config registry.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAreaConfigNoOverrideOnZeroConfig(t *testing.T) {
	defaultConfig := NewBatchConfig(4, 0)
	registry := newAreaBatchConfigRegistry[int](defaultConfig)

	registry.onAddPath(1, batchConfig{})
	require.Equal(t, defaultConfig, registry.getBatchConfig(1))
}

func TestAreaConfigApplyOnFirstAdd(t *testing.T) {
	defaultConfig := NewBatchConfig(4, 0)
	registry := newAreaBatchConfigRegistry[int](defaultConfig)

	registry.onAddPath(1, batchConfig{count: 2})
	require.Equal(t, NewBatchConfig(2, 0), registry.getBatchConfig(1))
}

func TestAreaConfigFirstAddWins(t *testing.T) {
	defaultConfig := NewBatchConfig(4, 0)
	registry := newAreaBatchConfigRegistry[int](defaultConfig)

	registry.onAddPath(1, batchConfig{count: 2})
	registry.onAddPath(1, batchConfig{count: 3})
	require.Equal(t, NewBatchConfig(2, 0), registry.getBatchConfig(1))
}

func TestAreaConfigReapplyAfterCleanup(t *testing.T) {
	defaultConfig := NewBatchConfig(4, 0)
	registry := newAreaBatchConfigRegistry[int](defaultConfig)

	registry.onAddPath(1, batchConfig{count: 2})
	require.Equal(t, NewBatchConfig(2, 0), registry.getBatchConfig(1))

	registry.onRemovePath(1)
	require.Equal(t, defaultConfig, registry.getBatchConfig(1))

	registry.onAddPath(1, batchConfig{count: 3})
	require.Equal(t, NewBatchConfig(3, 0), registry.getBatchConfig(1))
}

func TestAreaSettingsBatchConfigNormalized(t *testing.T) {
	settings := NewAreaSettingsWithMaxPendingSizeAndBatchConfig(
		64*1024*1024, 0, "test", 0, -1,
	)
	require.Equal(t, NewBatchConfig(0, -1), settings.batchConfig)
}
