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
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestDrainStateRejectSameEpochReactivation(t *testing.T) {
	state := NewDrainState()

	state.SetDispatcherDrainTarget(node.ID("n1"), 1)
	target, epoch := state.DispatcherDrainTarget()
	require.Equal(t, node.ID("n1"), target)
	require.Equal(t, uint64(1), epoch)

	state.SetDispatcherDrainTarget("", 1)
	target, epoch = state.DispatcherDrainTarget()
	require.Equal(t, node.ID(""), target)
	require.Equal(t, uint64(1), epoch)

	// A stale add path snapshot must not reactivate the target after the same
	// epoch has already been cleared.
	state.SetDispatcherDrainTarget(node.ID("n1"), 1)
	target, epoch = state.DispatcherDrainTarget()
	require.Equal(t, node.ID(""), target)
	require.Equal(t, uint64(1), epoch)

	// A newer epoch still wins.
	state.SetDispatcherDrainTarget(node.ID("n2"), 2)
	target, epoch = state.DispatcherDrainTarget()
	require.Equal(t, node.ID("n2"), target)
	require.Equal(t, uint64(2), epoch)
}
