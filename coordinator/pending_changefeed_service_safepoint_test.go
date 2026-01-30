package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

type errorPdClient struct {
	pd.Client
	err error
}

func (c *errorPdClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	return 0, c.err
}

func (c *errorPdClient) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	return &errorGCStatesClient{err: c.err}
}

type errorGCStatesClient struct {
	err error
}

func (c *errorGCStatesClient) SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GCBarrierInfo, error) {
	return pdgc.NewGCBarrierInfo(barrierID, barrierTS, ttl, time.Now()), c.err
}

func (c *errorGCStatesClient) DeleteGCBarrier(ctx context.Context, barrierID string) (*pdgc.GCBarrierInfo, error) {
	return nil, c.err
}

func (c *errorGCStatesClient) GetGCState(ctx context.Context) (pdgc.GCState, error) {
	return pdgc.GCState{}, c.err
}

func TestPendingChangefeedServiceSafepointGating(t *testing.T) {
	p := newPendingChangefeedServiceSafepoint()
	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)

	p.addCreating(cfID, 1)
	require.Empty(t, p.takeReadyForUndo())

	p.beginGCTick()
	require.Empty(t, p.takeReadyForUndo())

	p.markUpdateSucceeded()
	tasks := p.takeReadyForUndo()
	require.Len(t, tasks, 1)
	require.Equal(t, cfID, tasks[0].changefeedID)

	p.addCreating(cfID, 1)
	require.Empty(t, p.takeReadyForUndo())

	p.beginGCTick()
	p.markUpdateSucceeded()
	tasks = p.takeReadyForUndo()
	require.Len(t, tasks, 1)
	require.Equal(t, cfID, tasks[0].changefeedID)
}

func TestTryClearEnsureGCSafepointRequeuesRemainingTasksOnError(t *testing.T) {
	pending := newPendingChangefeedServiceSafepoint()
	cleaner := newChangefeedServiceSafepointCleaner(
		&errorPdClient{err: context.Canceled},
		"test-gc-service",
		&pending,
	)

	cfID1 := common.NewChangeFeedIDWithName("test1", common.DefaultKeyspaceName)
	cfID2 := common.NewChangeFeedIDWithName("test2", common.DefaultKeyspaceName)
	pending.addCreating(cfID1, 1)
	pending.addCreating(cfID2, 1)
	pending.beginGCTick()
	pending.markUpdateSucceeded()

	err := cleaner.tryClearEnsureGCSafepoint(context.Background())
	require.Error(t, err)

	entry1 := pending.pending[cfID1]
	require.NotNil(t, entry1)
	require.True(t, entry1.needCreating)

	entry2 := pending.pending[cfID2]
	require.NotNil(t, entry2)
	require.True(t, entry2.needCreating)
}
