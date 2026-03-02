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

// Tests for per-area batch config store.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAreaBatchConfigStoreSetAreaBatchConfigNoopWithoutPaths(t *testing.T) {
	defaultConfig := newBatchConfig(4, 0)
	store := newAreaBatchConfigStore[int](defaultConfig)

	store.setAreaBatchConfig(1, 2, 0)
	require.Equal(t, defaultConfig, store.getBatchConfig(1))
}

func TestAreaBatchConfigStoreSetAreaBatchConfigCanClearOverride(t *testing.T) {
	defaultConfig := newBatchConfig(4, 0)
	store := newAreaBatchConfigStore[int](defaultConfig)

	store.onAddPath(1)
	store.setAreaBatchConfig(1, 2, 0)
	require.Equal(t, newBatchConfig(2, 0), store.getBatchConfig(1))

	// Reset to default by setting policy values back to the default.
	store.setAreaBatchConfig(1, defaultConfig.batchCount, defaultConfig.batchBytes)
	require.Equal(t, defaultConfig, store.getBatchConfig(1))
}

func TestAreaBatchConfigStoreOnRemovePathCleansConfig(t *testing.T) {
	defaultConfig := newBatchConfig(4, 0)
	store := newAreaBatchConfigStore[int](defaultConfig)

	store.onAddPath(1)
	store.setAreaBatchConfig(1, 2, 0)
	require.Equal(t, newBatchConfig(2, 0), store.getBatchConfig(1))

	store.onRemovePath(1)
	require.Equal(t, defaultConfig, store.getBatchConfig(1))
}
