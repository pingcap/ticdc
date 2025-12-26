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
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestAppendKVEntriesFromRegionEntriesOutOfOrder(t *testing.T) {
	span := heartbeatpb.TableSpan{
		TableID:  100,
		StartKey: common.ToComparableKey([]byte{}),
		EndKey:   common.ToComparableKey(common.UpperBoundKey),
	}
	subSpan := &subscribedSpan{
		subID:   SubscriptionID(999),
		span:    span,
		startTs: 1000,
	}

	worker := &regionRequestWorker{requestCache: &requestCache{}}
	region := newRegionInfo(tikv.RegionVerID{}, span, &tikv.RPCContext{}, subSpan, false)
	region.lockedRangeState = &regionlock.LockedRangeState{}
	state := newRegionFeedState(region, 1, worker)
	state.start()

	var out []common.RawKVEntry

	out = appendKVEntriesFromRegionEntries(out[:0], subSpan, state, &cdcpb.Event_Entries_{
		Entries: &cdcpb.Event_Entries{
			Entries: []*cdcpb.Event_Row{{
				StartTs:  1,
				Type:     cdcpb.Event_PREWRITE,
				OpType:   cdcpb.Event_Row_PUT,
				Key:      []byte("key"),
				Value:    nil,
				OldValue: []byte("oldvalue"),
			}},
		},
	})
	require.Len(t, out, 0)

	out = appendKVEntriesFromRegionEntries(out[:0], subSpan, state, &cdcpb.Event_Entries_{
		Entries: &cdcpb.Event_Entries{
			Entries: []*cdcpb.Event_Row{{
				StartTs:  1,
				CommitTs: 2,
				Type:     cdcpb.Event_COMMIT,
				OpType:   cdcpb.Event_Row_PUT,
				Key:      []byte("key"),
			}},
		},
	})
	require.Len(t, out, 0)

	out = appendKVEntriesFromRegionEntries(out[:0], subSpan, state, &cdcpb.Event_Entries_{
		Entries: &cdcpb.Event_Entries{
			Entries: []*cdcpb.Event_Row{{
				StartTs:  1,
				Type:     cdcpb.Event_PREWRITE,
				OpType:   cdcpb.Event_Row_PUT,
				Key:      []byte("key"),
				Value:    []byte("value"),
				OldValue: []byte("oldvalue"),
			}},
		},
	})
	require.Len(t, out, 0)

	out = appendKVEntriesFromRegionEntries(out[:0], subSpan, state, &cdcpb.Event_Entries_{
		Entries: &cdcpb.Event_Entries{
			Entries: []*cdcpb.Event_Row{{Type: cdcpb.Event_INITIALIZED}},
		},
	})

	require.Len(t, out, 1)
	require.Equal(t, uint64(2), out[0].CRTs)
	require.Equal(t, uint64(1), out[0].StartTs)
	require.Equal(t, "value", string(out[0].Value))
	require.Equal(t, "oldvalue", string(out[0].OldValue))
}

func TestMaybeGenerateSpanResolvedTsMinAcrossRegions(t *testing.T) {
	totalSpan := heartbeatpb.TableSpan{
		TableID:  100,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
	}
	subSpan := &subscribedSpan{
		subID:           SubscriptionID(1),
		span:            totalSpan,
		startTs:         1,
		rangeLock:       regionlock.NewRangeLock(1, totalSpan.StartKey, totalSpan.EndKey, 1),
		advanceInterval: 0,
	}
	subSpan.resolvedTs.Store(5)

	worker := &regionRequestWorker{requestCache: &requestCache{}}

	res1 := subSpan.rangeLock.LockRange(context.Background(), []byte("a"), []byte("m"), 1, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res1.Status)
	region1 := newRegionInfo(tikv.NewRegionVerID(1, 1, 1), totalSpan, &tikv.RPCContext{}, subSpan, false)
	region1.lockedRangeState = res1.LockedRangeState
	state1 := newRegionFeedState(region1, uint64(subSpan.subID), worker)
	state1.start()
	state1.setInitialized()

	res2 := subSpan.rangeLock.LockRange(context.Background(), []byte("m"), []byte("z"), 2, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res2.Status)
	region2 := newRegionInfo(tikv.NewRegionVerID(2, 2, 2), totalSpan, &tikv.RPCContext{}, subSpan, false)
	region2.lockedRangeState = res2.LockedRangeState
	state2 := newRegionFeedState(region2, uint64(subSpan.subID), worker)
	state2.start()
	state2.setInitialized()

	updateRegionResolvedTs(subSpan, state1, 10)
	require.Equal(t, uint64(0), maybeAdvanceSpanResolvedTs(subSpan, state1.getRegionID()))
	updateRegionResolvedTs(subSpan, state2, 10)
	require.Equal(t, uint64(10), maybeAdvanceSpanResolvedTs(subSpan, state2.getRegionID()))
	require.Equal(t, uint64(10), subSpan.resolvedTs.Load())
}
