package coordinator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

type noopGCManager struct{}

var _ gc.Manager = noopGCManager{}

func (noopGCManager) TryUpdateServiceGCSafepoint(ctx context.Context, checkpointTs common.Ts) error {
	return nil
}

func (noopGCManager) CheckStaleCheckpointTs(keyspaceID uint32, changefeedID common.ChangeFeedID, checkpointTs common.Ts) error {
	return nil
}

func (noopGCManager) TryUpdateKeyspaceGCBarrier(ctx context.Context, keyspaceID uint32, keyspaceName string, checkpointTs common.Ts) error {
	return nil
}

type blockingPdClient struct {
	pd.Client

	startedOnce sync.Once
	undoStarted chan struct{}
	releaseUndo chan struct{}
}

func (c *blockingPdClient) markStarted() {
	c.startedOnce.Do(func() { close(c.undoStarted) })
}

func (c *blockingPdClient) UpdateServiceGCSafePoint(
	ctx context.Context, serviceID string, ttl int64, safePoint uint64,
) (uint64, error) {
	c.markStarted()
	select {
	case <-c.releaseUndo:
		return safePoint, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (c *blockingPdClient) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	return &blockingGCStatesClient{parent: c}
}

type blockingGCStatesClient struct {
	parent *blockingPdClient
}

func (c *blockingGCStatesClient) SetGCBarrier(
	ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration,
) (*pdgc.GCBarrierInfo, error) {
	return pdgc.NewGCBarrierInfo(barrierID, barrierTS, ttl, time.Now()), nil
}

func (c *blockingGCStatesClient) DeleteGCBarrier(ctx context.Context, barrierID string) (*pdgc.GCBarrierInfo, error) {
	c.parent.markStarted()
	select {
	case <-c.parent.releaseUndo:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *blockingGCStatesClient) GetGCState(ctx context.Context) (pdgc.GCState, error) {
	return pdgc.GCState{}, nil
}

func TestTryClearEnsureGCSafepointDoesNotBlockChangefeedChanges(t *testing.T) {
	undoStarted := make(chan struct{})
	releaseUndo := make(chan struct{})
	pdClient := &blockingPdClient{
		undoStarted: undoStarted,
		releaseUndo: releaseUndo,
	}

	co := &coordinator{
		controller: &Controller{
			changefeedDB: changefeed.NewChangefeedDB(1),
		},
		gcServiceID:                       "test-gc-service",
		gcManager:                         noopGCManager{},
		pdClient:                          pdClient,
		pdClock:                           pdutil.NewClock4Test(),
		changefeedChangeCh:                make(chan []*changefeedChange),
		pendingChangefeedServiceSafepoint: newPendingChangefeedServiceSafepoint(),
		lastTickTime:                      time.Now(),
	}
	co.changefeedServiceSafepointCleaner = newChangefeedServiceSafepointCleaner(
		pdClient,
		co.gcServiceID,
		&co.pendingChangefeedServiceSafepoint,
	)

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	co.pendingChangefeedServiceSafepoint.addCreating(cfID, 1)

	gcTickCh := make(chan time.Time, 1)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 2)
	go func() { errCh <- co.changefeedServiceSafepointCleaner.Run(ctx) }()
	go func() { errCh <- co.runWithGCTicker(ctx, gcTickCh) }()
	t.Cleanup(func() {
		cancel()
		close(releaseUndo)
		<-errCh
		<-errCh
	})

	gcTickCh <- time.Now()

	select {
	case <-undoStarted:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timeout waiting for undo ensure gc safepoint")
	}

	select {
	case co.changefeedChangeCh <- nil:
	case <-time.After(200 * time.Millisecond):
		require.FailNow(t, "changefeedChangeCh send blocked by tryClearEnsureGCSafepoint")
	}
}
