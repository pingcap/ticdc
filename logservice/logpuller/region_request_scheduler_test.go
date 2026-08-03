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
	"testing"

	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/stretchr/testify/require"
)

func TestRegionRequestSchedulerBroadcastDeregisterUsesWorkerControlQueue(t *testing.T) {
	scheduler := &regionRequestScheduler{}

	worker1 := &regionRequestWorker{
		storeAddr:    "store-1",
		admission:    newRegionAdmissionController(1, 1),
		controlQueue: newControlQueue(),
	}
	worker2 := &regionRequestWorker{
		storeAddr:    "store-2",
		admission:    newRegionAdmissionController(1, 1),
		controlQueue: newControlQueue(),
	}
	store1 := &regionRequestStore{workers: []*regionRequestWorker{worker1}}
	store2 := &regionRequestStore{workers: []*regionRequestWorker{worker2}}
	scheduler.stores.Store("store-1", store1)
	scheduler.stores.Store("store-2", store2)

	dummyRegion := regionInfo{
		subscribedSpan:   &subscribedSpan{subID: SubscriptionID(2)},
		lockedRangeState: &regionlock.LockedRangeState{},
	}
	require.True(t, worker1.admission.submit(newRegionPriorityTask(dummyRegion, 1)))

	scheduler.BroadcastDeregister(SubscriptionID(1), true)

	req1, ok := worker1.controlQueue.tryPop()
	require.True(t, ok)
	require.Equal(t, SubscriptionID(1), req1.subID)
	require.True(t, req1.filterLoop)

	req2, ok := worker2.controlQueue.tryPop()
	require.True(t, ok)
	require.Equal(t, SubscriptionID(1), req2.subID)
	require.True(t, req2.filterLoop)

	require.Equal(t, 1, worker1.admission.stats().pending)
}

func TestRegionRequestSchedulerInflightCountAggregatesStores(t *testing.T) {
	scheduler := &regionRequestScheduler{}

	worker1 := &regionRequestWorker{admission: newRegionAdmissionController(2, 1)}
	worker2 := &regionRequestWorker{admission: newRegionAdmissionController(2, 1)}
	scheduler.stores.Store("store-1", &regionRequestStore{workers: []*regionRequestWorker{worker1}})
	scheduler.stores.Store("store-2", &regionRequestStore{workers: []*regionRequestWorker{worker2}})

	req1 := admitRegionRequest(t, worker1.admission, prepareRegionForAdmission(createTestRegionInfo(1, 1), 100))
	req2 := admitRegionRequest(t, worker2.admission, prepareRegionForAdmission(createTestRegionInfo(1, 2), 100))

	require.Equal(t, 2, scheduler.inflightCount())

	require.True(t, req1.abort())
	require.True(t, req2.abort())
}
