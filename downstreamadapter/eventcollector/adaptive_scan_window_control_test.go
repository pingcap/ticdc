package eventcollector

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestUpdateScanMaxTsIgnoresDispatchersWithoutResolved(t *testing.T) {
	t.Parallel()

	c := &EventCollector{}

	cfStat := newChangefeedStat(mockChangefeedID)
	now := time.Unix(0, 0)
	cfStat.scanWindow.now = func() time.Time { return now }
	cfStat.scanWindow.lastObserveTime = now

	addDispatcher := func(id common.DispatcherID, checkpoint time.Time, eligible bool) {
		target := newMockDispatcher(id, oracle.GoTimeToTS(now))
		target.checkPointTs = oracle.GoTimeToTS(checkpoint)
		stat := &dispatcherStat{target: target}
		stat.hasReceivedResolvedTs.Store(eligible)
		c.dispatcherMap.Store(id, stat)
		cfStat.dispatcherIDs.Store(id, struct{}{})
	}

	d1 := common.NewDispatcherID()
	d2 := common.NewDispatcherID()
	d3 := common.NewDispatcherID()
	addDispatcher(d1, now.Add(10*time.Second), false) // ignored
	addDispatcher(d2, now.Add(20*time.Second), true)  // eligible min
	addDispatcher(d3, now.Add(30*time.Second), true)

	scanMaxTs := c.updateScanMaxTsForChangefeed(cfStat, 0.60)
	require.Equal(t, oracle.GoTimeToTS(now.Add(25*time.Second)), scanMaxTs)
}

func TestUpdateScanMaxTsReturnsZeroWhenEligibleEmpty(t *testing.T) {
	t.Parallel()

	c := &EventCollector{}

	cfStat := newChangefeedStat(mockChangefeedID)
	now := time.Unix(0, 0)
	cfStat.scanWindow.now = func() time.Time { return now }
	cfStat.scanWindow.lastObserveTime = now

	d1 := common.NewDispatcherID()
	d2 := common.NewDispatcherID()

	target1 := newMockDispatcher(d1, oracle.GoTimeToTS(now))
	target1.checkPointTs = oracle.GoTimeToTS(now.Add(10 * time.Second))
	stat1 := &dispatcherStat{target: target1}
	stat1.hasReceivedResolvedTs.Store(false)
	c.dispatcherMap.Store(d1, stat1)
	cfStat.dispatcherIDs.Store(d1, struct{}{})

	target2 := newMockDispatcher(d2, oracle.GoTimeToTS(now))
	target2.checkPointTs = oracle.GoTimeToTS(now.Add(5 * time.Second))
	stat2 := &dispatcherStat{target: target2}
	stat2.hasReceivedResolvedTs.Store(false)
	c.dispatcherMap.Store(d2, stat2)
	cfStat.dispatcherIDs.Store(d2, struct{}{})

	scanMaxTs := c.updateScanMaxTsForChangefeed(cfStat, 0.60)
	require.Equal(t, uint64(0), scanMaxTs)
}
