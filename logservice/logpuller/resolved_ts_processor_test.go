package logpuller

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestHandleResolvedTs(t *testing.T) {
	consumeKVEvents := func(events []common.RawKVEntry, _ func()) bool { return false }
	tsCh := make(chan uint64, 100)
	advanceResolvedTs := func(ts uint64) {
		tsCh <- ts
	}

	worker := &regionRequestWorker{requestCache: &requestCache{}}

	newSpanAndState := func(subID SubscriptionID, initialResolved uint64, initialized bool) (*subscribedSpan, *regionFeedState) {
		span := heartbeatpb.TableSpan{
			TableID:  100,
			StartKey: common.ToComparableKey([]byte{}),
			EndKey:   common.ToComparableKey(common.UpperBoundKey),
		}
		subSpan := &subscribedSpan{
			subID:             subID,
			span:              span,
			rangeLock:         regionlock.NewRangeLock(uint64(subID), span.StartKey, span.EndKey, 1),
			consumeKVEvents:   consumeKVEvents,
			advanceResolvedTs: advanceResolvedTs,
			advanceInterval:   0,
		}
		state := newRegionFeedState(regionInfo{verID: tikv.NewRegionVerID(uint64(subID), 1, 1)}, uint64(subID), worker)
		state.start()
		state.region.subscribedSpan = subSpan
		state.region.lockedRangeState = &regionlock.LockedRangeState{}
		if initialized {
			state.setInitialized()
		}
		if initialResolved > 0 {
			state.updateResolvedTs(initialResolved)
		}
		return subSpan, state
	}

	span1, state1 := newSpanAndState(SubscriptionID(1), 9, true)
	span2, state2 := newSpanAndState(SubscriptionID(2), 11, true)
	span3, state3 := newSpanAndState(SubscriptionID(3), 8, false)

	if ts := handleResolvedTs(span1, state1, 10); ts > 0 {
		span1.advanceResolvedTs(ts)
	}
	if ts := handleResolvedTs(span2, state2, 10); ts > 0 {
		span2.advanceResolvedTs(ts)
	}
	if ts := handleResolvedTs(span3, state3, 10); ts > 0 {
		span3.advanceResolvedTs(ts)
	}

	select {
	case <-tsCh:
	default:
		t.Fatal("must get an event")
	}

	select {
	case <-tsCh:
		t.Fatal("should get only one event")
	default:
	}

	require.Equal(t, uint64(10), state1.getLastResolvedTs())
	require.Equal(t, uint64(11), state2.getLastResolvedTs())
	require.Equal(t, uint64(8), state3.getLastResolvedTs())
}
