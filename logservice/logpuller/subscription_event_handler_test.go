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
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

// For UPDATE SQL, its prewrite event has both value and old value.
// It is possible that TiDB prewrites multiple times for the same row when
// there are other transactions it conflicts with. For this case,
// if the value is not "short", only the first prewrite contains the value.
//
// TiKV may output events for the UPDATE SQL as following:
//
// TiDB: [Prwrite1]    [Prewrite2]      [Commit]
//
//	v             v                v                                   Time
//
// ---------------------------------------------------------------------------->
//
//	^            ^    ^           ^     ^       ^     ^          ^     ^
//
// TiKV:   [Scan Start] [Send Prewrite2] [Send Commit] [Send Prewrite1] [Send Init]
// TiCDC:                    [Recv Prewrite2]  [Recv Commit] [Recv Prewrite1] [Recv Init]
func TestHandleEventEntryEventOutOfOrder(t *testing.T) {
	// initialize
	option := dynstream.NewOption()
	ds := dynstream.NewParallelDynamicStream(&subscriptionEventHandler{}, option)
	ds.Start()

	span := heartbeatpb.TableSpan{
		TableID:  100,
		StartKey: common.ToComparableKey([]byte{}), // TODO: remove spanz dependency
		EndKey:   common.ToComparableKey(common.UpperBoundKey),
	}
	subID := SubscriptionID(999)
	eventCh := make(chan common.RawKVEntry, 1000)
	consumeKVEvents := func(events []common.RawKVEntry, _ func()) bool {
		for _, e := range events {
			eventCh <- e
		}
		return false
	}
	advanceResolvedTs := func(ts uint64) {
		// not used
	}
	subSpan := &subscribedSpan{
		subID:             subID,
		span:              span,
		startTs:           1000, // not used
		consumeKVEvents:   consumeKVEvents,
		advanceResolvedTs: advanceResolvedTs,
		advanceInterval:   0,
	}
	ds.AddPath(subID, subSpan, dynstream.AreaSettings{})

	worker := &regionRequestWorker{
		requestCache: &requestCache{},
	}
	region := newRegionInfo(
		tikv.RegionVerID{},
		span,
		&tikv.RPCContext{},
		subSpan,
		false,
	)
	region.lockedRangeState = &regionlock.LockedRangeState{}
	state := newRegionFeedState(region, 1, worker)
	state.start()

	// Receive prewrite2 with empty value.
	{
		events := &cdcpb.Event_Entries_{
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
		}
		regionEvent := regionEvent{
			state:   state,
			entries: events,
		}
		ds.Push(subID, regionEvent)
	}

	// Receive commit.
	{
		events := &cdcpb.Event_Entries_{
			Entries: &cdcpb.Event_Entries{
				Entries: []*cdcpb.Event_Row{{
					StartTs:  1,
					CommitTs: 2,
					Type:     cdcpb.Event_COMMIT,
					OpType:   cdcpb.Event_Row_PUT,
					Key:      []byte("key"),
				}},
			},
		}
		regionEvent := regionEvent{
			state:   state,
			entries: events,
		}
		ds.Push(subID, regionEvent)
	}

	// Must not output event.
	{
		select {
		case <-eventCh:
			require.True(t, false, "shouldn't get an event")
		case <-time.NewTimer(100 * time.Millisecond).C:
		}
	}

	// Receive prewrite1 with actual value.
	{
		events := &cdcpb.Event_Entries_{
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
		}
		regionEvent := regionEvent{
			state:   state,
			entries: events,
		}
		ds.Push(subID, regionEvent)
	}

	// Must not output event.
	{
		select {
		case <-eventCh:
			require.True(t, false, "shouldn't get an event")
		case <-time.NewTimer(100 * time.Millisecond).C:
		}
	}

	// Receive initialized.
	{
		events := &cdcpb.Event_Entries_{
			Entries: &cdcpb.Event_Entries{
				Entries: []*cdcpb.Event_Row{
					{
						Type: cdcpb.Event_INITIALIZED,
					},
				},
			},
		}
		regionEvent := regionEvent{
			state:   state,
			entries: events,
		}
		ds.Push(subID, regionEvent)
	}

	// Must output event.
	{
		select {
		case event := <-eventCh:
			require.Equal(t, uint64(2), event.CRTs)
			require.Equal(t, uint64(1), event.StartTs)
			require.Equal(t, "value", string(event.Value))
			require.Equal(t, "oldvalue", string(event.OldValue))
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
	}
}

func TestHandleResolvedStateUpdateSpan(t *testing.T) {
	ctx := context.Background()
	worker := &regionRequestWorker{requestCache: &requestCache{}}
	startKey := common.ToComparableKey([]byte{0})
	midKey := common.ToComparableKey([]byte{1})
	endKey := common.ToComparableKey(common.UpperBoundKey)
	span := heartbeatpb.TableSpan{
		TableID:  101,
		StartKey: startKey,
		EndKey:   endKey,
	}
	subSpan := &subscribedSpan{
		subID:           SubscriptionID(42),
		span:            span,
		startTs:         1,
		rangeLock:       regionlock.NewRangeLock(uint64(42), span.StartKey, span.EndKey, 1),
		advanceInterval: 0,
	}
	subSpan.advanceResolvedTs = func(uint64) {}

	lockRes := subSpan.rangeLock.LockRange(ctx, span.StartKey, midKey, 1, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, lockRes.Status)
	lockRes.LockedRangeState.Initialized.Store(true)
	region := newRegionInfo(
		tikv.NewRegionVerID(1, 1, 1),
		heartbeatpb.TableSpan{StartKey: startKey, EndKey: midKey},
		&tikv.RPCContext{},
		subSpan,
		false,
	)
	region.lockedRangeState = lockRes.LockedRangeState
	state := newRegionFeedState(region, uint64(subSpan.subID), worker)
	state.start()
	state.region.lockedRangeState.Initialized.Store(true)

	require.True(t, handleResolvedState(subSpan, state, 10))
	require.Equal(t, uint64(10), state.getLastResolvedTs())

	res := updateSpanResolvedTs(subSpan, []*regionFeedState{state})
	require.Equal(t, uint64(1), res)

	res = updateSpanResolvedTs(subSpan, []*regionFeedState{state})
	require.Equal(t, uint64(1), res)

	res = updateSpanResolvedTs(subSpan, nil)
	require.Equal(t, uint64(0), res)

	lockRes2 := subSpan.rangeLock.LockRange(ctx, midKey, span.EndKey, 2, 1)
	require.Equal(t, regionlock.LockRangeStatusSuccess, lockRes2.Status)
	lockRes2.LockedRangeState.Initialized.Store(true)
	state2 := newRegionFeedState(regionInfo{
		verID:            tikv.NewRegionVerID(2, 2, 2),
		subscribedSpan:   subSpan,
		span:             heartbeatpb.TableSpan{StartKey: midKey, EndKey: span.EndKey},
		lockedRangeState: lockRes2.LockedRangeState,
	}, uint64(subSpan.subID), worker)
	state2.start()

	require.True(t, handleResolvedState(subSpan, state2, 12))
	res = updateSpanResolvedTs(subSpan, []*regionFeedState{state2})
	require.Equal(t, uint64(10), res)
}
