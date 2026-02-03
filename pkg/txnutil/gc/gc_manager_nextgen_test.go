//go:build nextgen
// +build nextgen

package gc

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

type mockGCStatesClient struct {
	txnSafePoint uint64
}

func (c *mockGCStatesClient) SetGCBarrier(
	ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration,
) (*pdgc.GCBarrierInfo, error) {
	if barrierTS < c.txnSafePoint {
		// Mark this error as non-retryable for SetGCBarrier's internal retry loop.
		return nil, errors.Annotate(context.Canceled, "ErrGCBarrierTSBehindTxnSafePoint")
	}
	return pdgc.NewGCBarrierInfo(barrierID, barrierTS, ttl, time.Now()), nil
}

func (c *mockGCStatesClient) DeleteGCBarrier(ctx context.Context, barrierID string) (*pdgc.GCBarrierInfo, error) {
	return nil, nil
}

func (c *mockGCStatesClient) GetGCState(ctx context.Context) (pdgc.GCState, error) {
	return pdgc.GCState{
		TxnSafePoint: c.txnSafePoint,
	}, nil
}

func TestTryUpdateKeyspaceGCBarrierDoesNotReturnSnapshotLost(t *testing.T) {
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())

	keyspaceID := uint32(1)
	keyspaceName := "test"
	checkpointTs := common.Ts(100)
	txnSafePoint := uint64(200)

	gcStatesClient := &mockGCStatesClient{txnSafePoint: txnSafePoint}
	pdClient := &MockPDClient{
		GetGCStatesClientFunc: func(id uint32) pdgc.GCStatesClient {
			require.Equal(t, keyspaceID, id)
			return gcStatesClient
		},
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			return 0, stderrors.New("not used")
		},
	}

	m := NewManager("test-service", pdClient)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, m.TryUpdateKeyspaceGCBarrier(ctx, keyspaceID, keyspaceName, checkpointTs))

	cfID := common.NewChangeFeedIDWithName("test-changefeed", keyspaceName)
	err := m.CheckStaleCheckpointTs(keyspaceID, cfID, checkpointTs)
	require.Error(t, err)
	errCode, ok := cerrors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerrors.ErrSnapshotLostByGC.RFCCode(), errCode)
}
