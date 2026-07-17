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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logpuller

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/utils/priorityqueue"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
)

func TestScanPriorityPolicyResolve(t *testing.T) {
	currentTime := time.Date(2026, time.June, 27, 12, 0, 0, 0, time.UTC)
	pdClock := pdutil.NewClock4Test()
	pdClock.(*pdutil.Clock4Test).SetTS(oracle.GoTimeToTS(currentTime))
	policy := newScanPriorityPolicy(pdClock, 30*time.Minute)

	for _, tc := range []struct {
		name             string
		inherited        TaskType
		regionResolvedTs uint64
		expected         TaskType
	}{
		{
			name:             "zero resolved ts",
			inherited:        TaskLowPrior,
			regionResolvedTs: 0,
			expected:         TaskLowPrior,
		},
		{
			name:             "recent region",
			inherited:        TaskLowPrior,
			regionResolvedTs: oracle.GoTimeToTS(currentTime.Add(-29 * time.Minute)),
			expected:         TaskHighPrior,
		},
		{
			name:             "threshold boundary",
			inherited:        TaskLowPrior,
			regionResolvedTs: oracle.GoTimeToTS(currentTime.Add(-30 * time.Minute)),
			expected:         TaskHighPrior,
		},
		{
			name:             "old region",
			inherited:        TaskLowPrior,
			regionResolvedTs: oracle.GoTimeToTS(currentTime.Add(-31 * time.Minute)),
			expected:         TaskLowPrior,
		},
		{
			name:             "future region",
			inherited:        TaskLowPrior,
			regionResolvedTs: oracle.GoTimeToTS(currentTime.Add(time.Minute)),
			expected:         TaskHighPrior,
		},
		{
			name:             "inherited high",
			inherited:        TaskHighPrior,
			regionResolvedTs: oracle.GoTimeToTS(currentTime.Add(-time.Hour)),
			expected:         TaskHighPrior,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, policy.resolve(tc.inherited, tc.regionResolvedTs, currentTime))
		})
	}
}

func TestScanPriorityPolicyRemainsHighAfterCatchUp(t *testing.T) {
	currentTime := time.Date(2026, time.June, 27, 12, 0, 0, 0, time.UTC)
	pdClock := pdutil.NewClock4Test()
	pdClock.(*pdutil.Clock4Test).SetTS(oracle.GoTimeToTS(currentTime))
	policy := newScanPriorityPolicy(pdClock, 30*time.Minute)

	require.False(t, policy.observeSpanResolved(oracle.GoTimeToTS(currentTime.Add(-31*time.Minute))))
	require.True(t, policy.observeSpanResolved(oracle.GoTimeToTS(currentTime.Add(-time.Minute))))
	require.False(t, policy.observeSpanResolved(oracle.GoTimeToTS(currentTime)))
	require.Equal(t, TaskHighPrior, policy.resolve(
		TaskLowPrior,
		oracle.GoTimeToTS(currentTime.Add(-time.Hour)),
		currentTime,
	))
}

func TestScanPriorityUsesRestoredRegionProgress(t *testing.T) {
	currentTime := time.Date(2026, time.June, 27, 12, 0, 0, 0, time.UTC)
	currentTs := oracle.GoTimeToTS(currentTime)
	pdClock := pdutil.NewClock4Test()
	pdClock.(*pdutil.Clock4Test).SetTS(currentTs)
	client := &subscriptionClient{
		pdClock:         pdClock,
		regionTaskQueue: priorityqueue.New[PriorityTask](),
	}

	startTs := oracle.GoTimeToTS(currentTime.Add(-time.Hour))
	rawSpan := heartbeatpb.TableSpan{
		TableID:  1,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
	}
	span := &subscribedSpan{
		subID:          SubscriptionID(1),
		span:           rawSpan,
		startTs:        startTs,
		rangeLock:      regionlock.NewRangeLock(1, rawSpan.StartKey, rawSpan.EndKey, startTs),
		priorityPolicy: newScanPriorityPolicy(pdClock, 30*time.Minute),
	}
	region := newRegionInfo(tikv.NewRegionVerID(1, 1, 1), rawSpan, nil, span, false)

	client.scheduleRegionRequest(context.Background(), region, TaskLowPrior)
	firstTask := popRegionPriorityTask(t, client.regionTaskQueue)
	require.Equal(t, TaskLowPrior, firstTask.taskType)

	firstRegion := firstTask.GetRegionInfo()
	firstRegion.lockedRangeState.ResolvedTs.Store(oracle.GoTimeToTS(currentTime.Add(-time.Minute)))
	span.rangeLock.UnlockRange(
		firstRegion.span.StartKey,
		firstRegion.span.EndKey,
		firstRegion.verID.GetID(),
		firstRegion.verID.GetVer(),
	)

	retryRegion := newRegionInfo(tikv.NewRegionVerID(1, 1, 2), rawSpan, nil, span, false)
	client.scheduleRegionRequest(context.Background(), retryRegion, TaskLowPrior)
	retryTask := popRegionPriorityTask(t, client.regionTaskQueue)
	require.Equal(t, TaskHighPrior, retryTask.taskType)
	require.Equal(t, cdcpb.ScanPriority_SCAN_PRIORITY_HIGH, retryTask.GetRegionInfo().scanPriority)
	require.False(t, span.priorityPolicy.everCaughtUp.Load())
}

func popRegionPriorityTask(
	t *testing.T,
	queue *priorityqueue.PriorityQueue[PriorityTask],
) *regionPriorityTask {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	task, err := queue.Pop(ctx)
	require.NoError(t, err)
	return task.(*regionPriorityTask)
}
