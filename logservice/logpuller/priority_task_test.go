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

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

// TestPriorityCalculationLogic tests the priority calculation logic in isolation
func TestPriorityCalculationLogic(t *testing.T) {
	currentTime := time.Now()
	currentTs := oracle.GoTimeToTS(currentTime)

	// Test cases for priority calculation
	tests := []struct {
		name                    string
		taskType                TaskType
		resolvedTsOffsetSeconds int64 // 相对于currentTs的偏移（负数表示resolvedTs更旧）
		waitTimeSeconds         int   // 任务等待时间
		description             string
	}{
		{
			name:                    "高优先级_新resolvedTs",
			taskType:                TaskHighPrior,
			resolvedTsOffsetSeconds: -5, // resolvedTs 比 currentTs 早5秒
			waitTimeSeconds:         10, // 等待了10秒
			description:             "高优先级任务，resolvedTs较新",
		},
		{
			name:                    "高优先级_旧resolvedTs",
			taskType:                TaskHighPrior,
			resolvedTsOffsetSeconds: -30, // resolvedTs 比 currentTs 早30秒
			waitTimeSeconds:         10,  // 等待了10秒
			description:             "高优先级任务，resolvedTs较旧",
		},
		{
			name:                    "低优先级_新resolvedTs",
			taskType:                TaskLowPrior,
			resolvedTsOffsetSeconds: -5, // resolvedTs 比 currentTs 早5秒
			waitTimeSeconds:         10, // 等待了10秒
			description:             "低优先级任务，resolvedTs较新",
		},
		{
			name:                    "低优先级_旧resolvedTs",
			taskType:                TaskLowPrior,
			resolvedTsOffsetSeconds: -30, // resolvedTs 比 currentTs 早30秒
			waitTimeSeconds:         10,  // 等待了10秒
			description:             "低优先级任务，resolvedTs较旧",
		},
	}

	var priorities []int
	var taskDescriptions []string

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 计算 resolvedTs：currentTs + offset
			resolvedTime := oracle.GetTimeFromTS(currentTs).Add(time.Duration(tt.resolvedTsOffsetSeconds) * time.Second)
			resolvedTs := oracle.GoTimeToTS(resolvedTime)

			// 模拟优先级计算逻辑
			priority := calculatePriorityDirectly(tt.taskType, currentTs, resolvedTs, tt.waitTimeSeconds)

			t.Logf("%s: Priority = %d", tt.description, priority)
			priorities = append(priorities, priority)
			taskDescriptions = append(taskDescriptions, tt.description)
		})
	}

	// 验证优先级顺序
	t.Run("验证优先级顺序", func(t *testing.T) {
		require.Equal(t, 4, len(priorities))

		highPriorNewResolvedTs := priorities[0] // 高优先级_新resolvedTs
		highPriorOldResolvedTs := priorities[1] // 高优先级_旧resolvedTs
		lowPriorNewResolvedTs := priorities[2]  // 低优先级_新resolvedTs
		lowPriorOldResolvedTs := priorities[3]  // 低优先级_旧resolvedTs

		t.Logf("优先级对比:")
		for i, desc := range taskDescriptions {
			t.Logf("  %s: %d", desc, priorities[i])
		}

		// 核心验证：相同任务类型下，resolvedTs越新（lag越小），优先级越高（数值越小）
		require.Less(t, highPriorNewResolvedTs, highPriorOldResolvedTs,
			"相同任务类型下，resolvedTs越新的任务优先级应该越高")
		require.Less(t, lowPriorNewResolvedTs, lowPriorOldResolvedTs,
			"相同任务类型下，resolvedTs越新的任务优先级应该越高")

		// 验证：高优先级任务总是比低优先级任务优先级高
		require.Less(t, highPriorNewResolvedTs, lowPriorNewResolvedTs,
			"高优先级任务应该比低优先级任务优先级高")
		require.Less(t, highPriorOldResolvedTs, lowPriorOldResolvedTs,
			"即使resolvedTs较旧，高优先级任务仍应比低优先级任务优先级高")
	})
}

// calculatePriorityDirectly 直接计算优先级，用于测试
// 复制了 regionPriorityTask.Priority() 的逻辑
func calculatePriorityDirectly(taskType TaskType, currentTs, resolvedTs uint64, waitTimeSeconds int) int {
	// Base priority based on task type
	basePriority := 0
	switch taskType {
	case TaskHighPrior:
		basePriority = highPriorityBase // 1200
	case TaskLowPrior:
		basePriority = lowPriorityBase // 3600
	}

	// Add time-based priority bonus
	// Wait time in seconds, longer wait time means higher priority (lower value)
	timeBonus := waitTimeSeconds

	// Calculate resolvedTs lag
	resolvedTsLag := oracle.GetTimeFromTS(currentTs).Sub(oracle.GetTimeFromTS(resolvedTs))
	resolvedTsLagBonus := int(resolvedTsLag.Seconds())

	priority := basePriority - timeBonus + resolvedTsLagBonus

	if priority < 0 {
		priority = 0
	}
	return priority
}

func TestResolvedTsLagLogic(t *testing.T) {
	currentTime := time.Now()
	currentTs := oracle.GoTimeToTS(currentTime)

	t.Run("测试resolvedTs lag计算逻辑", func(t *testing.T) {
		// 场景1：resolvedTs比currentTs早10秒（resolvedTs更旧）
		resolvedTs1 := oracle.GoTimeToTS(currentTime.Add(-10 * time.Second))
		lag1 := oracle.GetTimeFromTS(currentTs).Sub(oracle.GetTimeFromTS(resolvedTs1))
		t.Logf("resolvedTs早10秒，lag = %v (%.0f 秒)", lag1, lag1.Seconds())

		// 场景2：resolvedTs比currentTs早1秒（resolvedTs较新）
		resolvedTs2 := oracle.GoTimeToTS(currentTime.Add(-1 * time.Second))
		lag2 := oracle.GetTimeFromTS(currentTs).Sub(oracle.GetTimeFromTS(resolvedTs2))
		t.Logf("resolvedTs早1秒，lag = %v (%.0f 秒)", lag2, lag2.Seconds())

		// 验证：resolvedTs越新，lag越小
		require.Less(t, lag2, lag1, "resolvedTs越新，lag应该越小")

		// 计算对优先级的影响
		priority1 := calculatePriorityDirectly(TaskHighPrior, currentTs, resolvedTs1, 5)
		priority2 := calculatePriorityDirectly(TaskHighPrior, currentTs, resolvedTs2, 5)

		t.Logf("resolvedTs旧10秒的优先级: %d", priority1)
		t.Logf("resolvedTs新1秒的优先级: %d", priority2)

		// 验证：resolvedTs越新，优先级越高（数值越小）
		require.Less(t, priority2, priority1,
			"resolvedTs越新的任务优先级应该越高")
	})
}

func TestEdgeCases(t *testing.T) {
	currentTime := time.Now()
	currentTs := oracle.GoTimeToTS(currentTime)

	t.Run("resolvedTs在未来的情况", func(t *testing.T) {
		// resolvedTs比currentTs晚5秒（在未来）
		resolvedTs := oracle.GoTimeToTS(currentTime.Add(5 * time.Second))

		lag := oracle.GetTimeFromTS(currentTs).Sub(oracle.GetTimeFromTS(resolvedTs))
		t.Logf("resolvedTs在未来5秒，lag = %v (%.0f 秒)", lag, lag.Seconds())

		priority := calculatePriorityDirectly(TaskHighPrior, currentTs, resolvedTs, 5)
		t.Logf("resolvedTs在未来的优先级: %d", priority)

		// 当resolvedTs在未来时，lag为负数，会降低优先级数值，提高实际优先级
		require.GreaterOrEqual(t, priority, 0, "优先级不应该小于0")
	})

	t.Run("不同等待时间的影响", func(t *testing.T) {
		resolvedTs := oracle.GoTimeToTS(currentTime.Add(-10 * time.Second))

		priority1 := calculatePriorityDirectly(TaskHighPrior, currentTs, resolvedTs, 2)  // 等待2秒
		priority2 := calculatePriorityDirectly(TaskHighPrior, currentTs, resolvedTs, 10) // 等待10秒

		t.Logf("等待2秒的优先级: %d", priority1)
		t.Logf("等待10秒的优先级: %d", priority2)

		// 等待时间越长，优先级越高（数值越小）
		require.Less(t, priority2, priority1, "等待时间越长的任务优先级应该越高")
	})
}
