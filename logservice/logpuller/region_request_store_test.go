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

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestRegionRequestStoreDistributesRegionsAcrossWorkers(t *testing.T) {
	worker1 := &regionRequestWorker{admission: newRegionAdmissionController(1, 1)}
	worker2 := &regionRequestWorker{admission: newRegionAdmissionController(1, 1)}
	store := &regionRequestStore{
		workers: []*regionRequestWorker{worker1, worker2},
	}

	for i := uint64(1); i <= 4; i++ {
		region := prepareRegionForAdmission(createTestRegionInfo(1, i), 100)
		region.verID = tikv.NewRegionVerID(i, 1, 1)
		require.True(t, store.submit(newRegionPriorityTask(region, i)))
	}

	require.Equal(t, 2, worker1.admission.stats().pending)
	require.Equal(t, 2, worker2.admission.stats().pending)
}

func TestRegionRequestStoreInflightCountAggregatesWorkers(t *testing.T) {
	worker1 := &regionRequestWorker{admission: newRegionAdmissionController(2, 1)}
	worker2 := &regionRequestWorker{admission: newRegionAdmissionController(2, 1)}
	store := &regionRequestStore{
		workers: []*regionRequestWorker{worker1, worker2},
	}

	req1 := admitRegionRequest(t, worker1.admission, prepareRegionForAdmission(createTestRegionInfo(1, 1), 100))
	req2 := admitRegionRequest(t, worker2.admission, prepareRegionForAdmission(createTestRegionInfo(1, 2), 100))

	require.Equal(t, 2, store.inflightCount())

	require.True(t, req1.abort())
	require.True(t, req2.abort())
}

func TestRegionRequestStoreCloseClosesWorkerAdmissions(t *testing.T) {
	worker1 := &regionRequestWorker{admission: newRegionAdmissionController(1, 1)}
	worker2 := &regionRequestWorker{admission: newRegionAdmissionController(1, 1)}
	store := &regionRequestStore{
		workers: []*regionRequestWorker{worker1, worker2},
	}

	store.close()

	region1 := prepareRegionForAdmission(createTestRegionInfo(1, 1), 100)
	region2 := prepareRegionForAdmission(createTestRegionInfo(1, 2), 100)
	require.False(t, worker1.admission.submit(newRegionPriorityTask(region1, 1)))
	require.False(t, worker2.admission.submit(newRegionPriorityTask(region2, 2)))
}
