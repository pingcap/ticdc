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

package logpuller

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func newTestQuotaSpan(subID SubscriptionID, changefeedID common.ChangeFeedID) *subscribedSpan {
	span := &subscribedSpan{
		subID: subID,
		meta:  NewChangefeedSubscriptionMeta(changefeedID),
	}
	span.resolvedTs.Store(oracle.GoTimeToTS(time.Now()))
	return span
}

func TestMemoryQuotaAdmissionLevels(t *testing.T) {
	controller := newMemoryQuotaController(100)

	warmingSpan := newTestQuotaSpan(1, common.NewChangeFeedIDWithName("warming", common.DefaultKeyspaceName))
	normalSpan := newTestQuotaSpan(2, common.NewChangeFeedIDWithName("normal", common.DefaultKeyspaceName))
	normalSpan.initialized.Store(true)

	controller.addSubscription(warmingSpan)
	controller.addSubscription(normalSpan)
	controller.markSubscriptionInitialized(normalSpan)

	ok, reason := controller.allowNewScan(warmingSpan)
	require.True(t, ok)
	require.Empty(t, reason)

	softLease := controller.acquireEvent(context.Background(), normalSpan, 20)
	t.Cleanup(softLease.Release)
	ok, reason = controller.allowNewScan(warmingSpan)
	require.False(t, ok)
	require.Equal(t, deferReasonMemoryWarming, reason)
	ok, reason = controller.allowNewScan(normalSpan)
	require.True(t, ok)
	require.Empty(t, reason)

	hardLease := controller.acquireEvent(context.Background(), normalSpan, 70)
	t.Cleanup(hardLease.Release)
	ok, reason = controller.allowNewScan(normalSpan)
	require.False(t, ok)
	require.Equal(t, deferReasonMemoryFreeze, reason)

	hardLease.Release()
	_, _, level := controller.Snapshot()
	require.Equal(t, admissionPauseWarming, level)

	softLease.Release()
	_, _, level = controller.Snapshot()
	require.Equal(t, admissionNormal, level)
}

func TestWarmingHighPriorityTaskBlockedByMemoryGate(t *testing.T) {
	controller := newMemoryQuotaController(100)
	warmingSpan := newTestQuotaSpan(1, common.NewChangeFeedIDWithName("warming", common.DefaultKeyspaceName))
	controller.addSubscription(warmingSpan)
	lease := controller.acquireEvent(context.Background(), warmingSpan, 20)
	t.Cleanup(lease.Release)

	scheduler := &regionRequestScheduler{memoryQuota: controller}
	region := regionInfo{subscribedSpan: warmingSpan}
	task := newRegionPriorityTask(TaskHighPrior, region, oracle.GoTimeToTS(time.Now()), 1)

	ok, reason, err := scheduler.tryAdmitTask(context.Background(), &requestedStore{}, task, region)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, deferReasonMemoryWarming, reason)
}

func TestRemoveSubscriptionReleasesOnlyItsOutstandingMemory(t *testing.T) {
	controller := newMemoryQuotaController(100)
	changefeedID := common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName)
	span1 := newTestQuotaSpan(1, changefeedID)
	span2 := newTestQuotaSpan(2, changefeedID)
	controller.addSubscription(span1)
	controller.addSubscription(span2)

	lease1 := controller.acquireEvent(context.Background(), span1, 30)
	lease2 := controller.acquireEvent(context.Background(), span2, 40)
	t.Cleanup(lease2.Release)

	used, _, _ := controller.Snapshot()
	require.Equal(t, uint64(70), used)

	controller.removeSubscription(span1)
	used, _, _ = controller.Snapshot()
	require.Equal(t, uint64(40), used)

	lease1.Release()
	used, _, _ = controller.Snapshot()
	require.Equal(t, uint64(40), used)

	lease2.Release()
	used, _, _ = controller.Snapshot()
	require.Equal(t, uint64(0), used)
}
