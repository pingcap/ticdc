// Copyright 2024 PingCAP, Inc.
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

package logpuller

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestRegionPriorityTask(t *testing.T) {
	// Create a mock regionInfo for testing
	mockRegionInfo := regionInfo{
		verID: tikv.NewRegionVerID(1, 1, 1),
		span: heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		},
	}

	t.Run("test task type priority", func(t *testing.T) {
		// Test region change task has higher priority (lower value)
		regionChangeTask := NewRegionPriorityTask(TaskHighPrior, mockRegionInfo)
		newSubTask := NewRegionPriorityTask(TaskLowPrior, mockRegionInfo)

		regionChangePriority := regionChangeTask.Priority()
		newSubPriority := newSubTask.Priority()

		// Region change should have higher priority (lower value)
		require.Less(t, regionChangePriority, newSubPriority)
	})

	t.Run("test time-based priority", func(t *testing.T) {
		// Create two tasks of the same type
		task1 := NewRegionPriorityTask(TaskLowPrior, mockRegionInfo)

		// Wait a bit
		time.Sleep(10 * time.Millisecond)

		task2 := NewRegionPriorityTask(TaskLowPrior, mockRegionInfo)

		priority1 := task1.Priority()
		priority2 := task2.Priority()

		// task1 should have higher priority (lower value) because it waited longer
		require.Less(t, priority1, priority2)
	})

	t.Run("test priority calculation", func(t *testing.T) {
		task := NewRegionPriorityTask(TaskHighPrior, mockRegionInfo)

		// For region change task, base priority should be 0
		priority := task.Priority()

		// Priority should be negative (due to time bonus) and close to 0
		require.LessOrEqual(t, priority, 0)
		require.Greater(t, priority, -100) // Should not be too negative for a fresh task
	})
}
