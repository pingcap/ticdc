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

package logpuller

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/utils/priorityqueue"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

// TestPriorityCalculationLogic tests the priority calculation logic in isolation
func TestPriorityCalculationLogic(t *testing.T) {
	currentTime := time.Now()
	currentTs := oracle.GoTimeToTS(currentTime)

	// Test cases for priority calculation
	tests := []struct {
		name                    string
		taskType                TaskType
		resolvedTsOffsetSeconds int64 // Offset relative to currentTs (negative means resolvedTs is older)
		description             string
	}{
		{
			name:                    "high_priority_new_resolvedTs",
			taskType:                TaskHighPrior,
			resolvedTsOffsetSeconds: -5, // resolvedTs is 5 seconds earlier than currentTs
			description:             "High priority task with newer resolvedTs",
		},
		{
			name:                    "high_priority_old_resolvedTs",
			taskType:                TaskHighPrior,
			resolvedTsOffsetSeconds: -30, // resolvedTs is 30 seconds earlier than currentTs
			description:             "High priority task with older resolvedTs",
		},
		{
			name:                    "low_priority_new_resolvedTs",
			taskType:                TaskLowPrior,
			resolvedTsOffsetSeconds: -5, // resolvedTs is 5 seconds earlier than currentTs
			description:             "Low priority task with newer resolvedTs",
		},
		{
			name:                    "low_priority_old_resolvedTs",
			taskType:                TaskLowPrior,
			resolvedTsOffsetSeconds: -30, // resolvedTs is 30 seconds earlier than currentTs
			description:             "Low priority task with older resolvedTs",
		},
	}

	var priorities []int
	var taskDescriptions []string

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate resolvedTs: currentTs + offset
			resolvedTime := oracle.GetTimeFromTS(currentTs).Add(time.Duration(tt.resolvedTsOffsetSeconds) * time.Second)
			resolvedTs := oracle.GoTimeToTS(resolvedTime)

			priority := calculatePriorityDirectly(tt.taskType, currentTs, resolvedTs)

			t.Logf("%s: Priority = %d", tt.description, priority)
			priorities = append(priorities, priority)
			taskDescriptions = append(taskDescriptions, tt.description)
		})
	}

	// Verify priority order
	t.Run("verify_priority_order", func(t *testing.T) {
		require.Equal(t, 4, len(priorities))

		highPriorNewResolvedTs := priorities[0] // High priority with new resolvedTs
		highPriorOldResolvedTs := priorities[1] // High priority with old resolvedTs
		lowPriorNewResolvedTs := priorities[2]  // Low priority with new resolvedTs
		lowPriorOldResolvedTs := priorities[3]  // Low priority with old resolvedTs

		t.Logf("Priority comparison:")
		for i, desc := range taskDescriptions {
			t.Logf("  %s: %d", desc, priorities[i])
		}

		// Core verification: For the same task type, newer resolvedTs (smaller lag) should have higher priority (smaller value)
		require.Less(t, highPriorNewResolvedTs, highPriorOldResolvedTs,
			"For the same task type, tasks with newer resolvedTs should have higher priority")
		require.Less(t, lowPriorNewResolvedTs, lowPriorOldResolvedTs,
			"For the same task type, tasks with newer resolvedTs should have higher priority")

		// Verify: High priority tasks always have higher priority than low priority tasks
		require.Less(t, highPriorNewResolvedTs, lowPriorNewResolvedTs,
			"High priority tasks should have higher priority than low priority tasks")
		require.Less(t, highPriorOldResolvedTs, lowPriorOldResolvedTs,
			"Even with older resolvedTs, high priority tasks should still have higher priority than low priority tasks")
	})
}

// calculatePriorityDirectly directly calculates priority for testing.
func calculatePriorityDirectly(taskType TaskType, currentTs, resolvedTs uint64) int {
	basePriority := 0
	switch taskType {
	case TaskHighPrior:
		basePriority = highPriorityBase
	case TaskLowPrior:
		basePriority = lowPriorityBase
	}

	resolvedTsLag := oracle.GetTimeFromTS(currentTs).Sub(oracle.GetTimeFromTS(resolvedTs))
	resolvedTsLagBonus := int(resolvedTsLag.Seconds())

	return max(basePriority+resolvedTsLagBonus, 0)
}

func TestResolvedTsLagLogic(t *testing.T) {
	currentTime := time.Now()
	currentTs := oracle.GoTimeToTS(currentTime)

	t.Run("test_resolvedTs_lag_calculation_logic", func(t *testing.T) {
		// Scenario 1: resolvedTs is 10 seconds earlier than currentTs (resolvedTs is older)
		resolvedTs1 := oracle.GoTimeToTS(currentTime.Add(-10 * time.Second))
		lag1 := oracle.GetTimeFromTS(currentTs).Sub(oracle.GetTimeFromTS(resolvedTs1))
		t.Logf("resolvedTs is 10 seconds earlier, lag = %v (%.0f seconds)", lag1, lag1.Seconds())

		// Scenario 2: resolvedTs is 1 second earlier than currentTs (resolvedTs is newer)
		resolvedTs2 := oracle.GoTimeToTS(currentTime.Add(-1 * time.Second))
		lag2 := oracle.GetTimeFromTS(currentTs).Sub(oracle.GetTimeFromTS(resolvedTs2))
		t.Logf("resolvedTs is 1 second earlier, lag = %v (%.0f seconds)", lag2, lag2.Seconds())

		// Verify: newer resolvedTs should have smaller lag
		require.Less(t, lag2, lag1, "newer resolvedTs should have smaller lag")

		// Calculate the impact on priority
		priority1 := calculatePriorityDirectly(TaskHighPrior, currentTs, resolvedTs1)
		priority2 := calculatePriorityDirectly(TaskHighPrior, currentTs, resolvedTs2)

		t.Logf("Priority with resolvedTs 10 seconds old: %d", priority1)
		t.Logf("Priority with resolvedTs 1 second old: %d", priority2)

		// Verify: newer resolvedTs should have higher priority (smaller value)
		require.Less(t, priority2, priority1,
			"tasks with newer resolvedTs should have higher priority")
	})
}

func TestEdgeCases(t *testing.T) {
	currentTime := time.Now()
	currentTs := oracle.GoTimeToTS(currentTime)

	t.Run("resolvedTs in the future", func(t *testing.T) {
		resolvedTs := oracle.GoTimeToTS(currentTime.Add(5 * time.Second))

		lag := oracle.GetTimeFromTS(currentTs).Sub(oracle.GetTimeFromTS(resolvedTs))
		t.Logf("resolvedTs in the future 5 seconds, lag = %v (%.0f seconds)", lag, lag.Seconds())

		priority := calculatePriorityDirectly(TaskHighPrior, currentTs, resolvedTs)
		t.Logf("resolvedTs in the future priority: %d", priority)

		require.GreaterOrEqual(t, priority, 0, "priority should not be less than 0")
	})

	t.Run("priority is stable after task creation", func(t *testing.T) {
		resolvedTs := oracle.GoTimeToTS(currentTime.Add(-10 * time.Second))
		subscribedSpan := &subscribedSpan{}
		subscribedSpan.resolvedTs.Store(resolvedTs)
		regionInfo := regionInfo{subscribedSpan: subscribedSpan}

		task := newRegionPriorityTask(TaskHighPrior, regionInfo, currentTs, 1)
		priority := task.Priority()
		subscribedSpan.resolvedTs.Store(currentTs)

		require.Equal(t, priority, task.Priority())
	})
}

func TestRegionPriorityTaskQueueOrder(t *testing.T) {
	queue := priorityqueue.New[*regionPriorityTask]()
	ctx := t.Context()

	currentTs := oracle.GoTimeToTS(time.Now())
	verID := tikv.NewRegionVerID(1, 1, 1)
	span := heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("z")}

	subscribedSpan := &subscribedSpan{
		resolvedTs: atomic.Uint64{},
	}
	subscribedSpan.resolvedTs.Store(oracle.GoTimeToTS(time.Now().Add(-time.Second)))

	regionInfo := regionInfo{
		verID:          verID,
		span:           span,
		subscribedSpan: subscribedSpan,
	}

	firstHighTask := newRegionPriorityTask(TaskHighPrior, regionInfo, currentTs, 1)
	secondHighTask := newRegionPriorityTask(TaskHighPrior, regionInfo, currentTs, 2)
	lowTask := newRegionPriorityTask(TaskLowPrior, regionInfo, currentTs, 3)

	require.True(t, queue.Push(lowTask))
	require.True(t, queue.Push(secondHighTask))
	require.True(t, queue.Push(firstHighTask))

	first, err := queue.Pop(ctx)
	require.NoError(t, err)
	require.Same(t, firstHighTask, first)

	second, err := queue.Pop(ctx)
	require.NoError(t, err)
	require.Same(t, secondHighTask, second)

	third, err := queue.Pop(ctx)
	require.NoError(t, err)
	require.Same(t, lowTask, third)

	require.Equal(t, 0, queue.Len())
}
