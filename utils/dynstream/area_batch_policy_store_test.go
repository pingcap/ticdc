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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAreaBatchPolicyStoreSetAreaSettingsNoopWithoutPaths(t *testing.T) {
	defaultPolicy := newBatchPolicy(4, 0)
	store := newAreaBatchPolicyStore[int](defaultPolicy)

	store.setAreaSettings(1, AreaSettings{}.WithBatchPolicy(2, 0))
	require.Equal(t, defaultPolicy, store.getPolicy(1))
}

func TestAreaBatchPolicyStoreSetAreaSettingsDoesNotClearWithoutExplicitOverride(t *testing.T) {
	defaultPolicy := newBatchPolicy(4, 0)
	store := newAreaBatchPolicyStore[int](defaultPolicy)

	store.onAddPath(1)
	store.setAreaSettings(1, AreaSettings{}.WithBatchPolicy(2, 0))
	require.Equal(t, newBatchPolicy(2, 0), store.getPolicy(1))

	// Caller may only want to update memory control settings. Batching override should remain unchanged.
	store.setAreaSettings(1, NewAreaSettingsWithMaxPendingSize(1024, MemoryControlForEventCollector, "test"))
	require.Equal(t, newBatchPolicy(2, 0), store.getPolicy(1))
}

func TestAreaBatchPolicyStoreSetAreaSettingsCanClearOverride(t *testing.T) {
	defaultPolicy := newBatchPolicy(4, 0)
	store := newAreaBatchPolicyStore[int](defaultPolicy)

	store.onAddPath(1)
	store.setAreaSettings(1, AreaSettings{}.WithBatchPolicy(2, 0))
	require.Equal(t, newBatchPolicy(2, 0), store.getPolicy(1))

	// Reset to default by setting policy values back to the default.
	store.setAreaSettings(1, AreaSettings{}.WithBatchPolicy(defaultPolicy.batchCount, defaultPolicy.batchBytes))
	require.Equal(t, defaultPolicy, store.getPolicy(1))
}

func TestAreaBatchPolicyStoreOnRemovePathCleansPolicy(t *testing.T) {
	defaultPolicy := newBatchPolicy(4, 0)
	store := newAreaBatchPolicyStore[int](defaultPolicy)

	store.onAddPath(1)
	store.setAreaSettings(1, AreaSettings{}.WithBatchPolicy(2, 0))
	require.Equal(t, newBatchPolicy(2, 0), store.getPolicy(1))

	store.onRemovePath(1)
	require.Equal(t, defaultPolicy, store.getPolicy(1))
}
