package dispatcher

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func newSharedInfoForInflightBudgetTest() *SharedInfo {
	defaultAtomicity := config.DefaultAtomicityLevel()
	return NewSharedInfo(
		common.NewChangefeedID(common.DefaultKeyspaceName),
		"system",
		false,
		false,
		nil,
		nil,
		&syncpoint.SyncPointConfig{
			SyncPointInterval:  5 * time.Second,
			SyncPointRetention: 10 * time.Minute,
		},
		&defaultAtomicity,
		false,
		make(chan TableSpanStatusWithSeq, 128),
		make(chan *heartbeatpb.TableSpanBlockStatus, 128),
		make(chan error, 1),
	)
}

func TestChangefeedInflightBudgetBlocksAndWakes(t *testing.T) {
	changefeedID := common.NewChangefeedID(common.DefaultKeyspaceName)
	budget := newGlobalInflightBudget(common.CloudStorageSinkType, changefeedID, 100, 50)

	var wakeCount atomic.Int32
	dispatcherID := common.NewDispatcherID()

	budget.acquire(120)
	require.True(t, budget.TryBlock(dispatcherID, func() { wakeCount.Add(1) }))
	require.Equal(t, int32(0), wakeCount.Load())

	// in-flight falls to low watermark, should wake once.
	budget.release(70)
	require.Equal(t, int32(1), wakeCount.Load())
}

func TestChangefeedInflightBudgetDedupAndCleanup(t *testing.T) {
	changefeedID := common.NewChangefeedID(common.DefaultKeyspaceName)
	dispatcherID := common.NewDispatcherID()

	t.Run("dedup", func(t *testing.T) {
		budget := newGlobalInflightBudget(common.CloudStorageSinkType, changefeedID, 100, 50)
		var wakeCount atomic.Int32

		budget.acquire(120)
		require.True(t, budget.TryBlock(dispatcherID, func() { wakeCount.Add(1) }))
		require.True(t, budget.TryBlock(dispatcherID, func() { wakeCount.Add(1) }))

		budget.release(70)
		require.Equal(t, int32(1), wakeCount.Load())
	})

	t.Run("cleanup", func(t *testing.T) {
		budget := newGlobalInflightBudget(common.CloudStorageSinkType, changefeedID, 100, 50)
		var wakeCount atomic.Int32

		budget.acquire(120)
		require.True(t, budget.TryBlock(dispatcherID, func() { wakeCount.Add(1) }))
		budget.cleanupDispatcher(dispatcherID)

		budget.release(70)
		require.Equal(t, int32(0), wakeCount.Load())
	})
}

func TestInflightBudgetGlobalWakeWhenOtherDispatcherFlushes(t *testing.T) {
	sharedInfo := newSharedInfoForInflightBudgetTest()
	t.Cleanup(sharedInfo.Close)

	// Use a tiny area quota so the derived global watermarks are small and deterministic:
	// globalHigh = quota/2 = 100, globalLow = 50.
	sharedInfo.InitChangefeedInflightBudget(common.CloudStorageSinkType, 200)

	changefeedID := sharedInfo.changefeedID

	b1 := newInflightBudget(common.CloudStorageSinkType, changefeedID, common.NewDispatcherID(), sharedInfo)
	b2 := newInflightBudget(common.CloudStorageSinkType, changefeedID, common.NewDispatcherID(), sharedInfo)

	var wake1 atomic.Int32
	var wake2 atomic.Int32
	wakeCallback1 := func() { wake1.Add(1) }
	wakeCallback2 := func() { wake2.Add(1) }

	dml1 := &commonEvent.DMLEvent{CommitTs: 1, Length: 1, ApproximateSize: 60}
	b1.trackDML(dml1, wakeCallback1)
	require.True(t, b1.tryBlock(wakeCallback1))

	dml2 := &commonEvent.DMLEvent{CommitTs: 2, Length: 1, ApproximateSize: 60}
	b2.trackDML(dml2, wakeCallback2)
	require.True(t, b2.tryBlock(wakeCallback2))

	// Dispatcher 1 flush completes but global inflight is still high (dispatcher 2 still in-flight),
	// so dispatcher 1 must not be woken yet.
	dml1.PostFlush()
	require.Equal(t, int32(0), wake1.Load())

	// Dispatcher 2 flush drops the global inflight to the low watermark and should wake blocked dispatchers.
	dml2.PostFlush()
	require.Equal(t, int32(1), wake1.Load())
	require.Equal(t, int32(1), wake2.Load())
}
