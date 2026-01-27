package gc

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
)

func TestTryUpdateServiceGCSafepointForceUpdateReturnsErrorOnUpdateFailure(t *testing.T) {
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())

	pdClient := &MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			return 0, stderrors.New("pd is unstable")
		},
	}

	m := NewManager("test-service", pdClient)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := m.TryUpdateServiceGCSafepoint(ctx, common.Ts(100), true)
	require.Error(t, err)
	errCode, ok := cerrors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerrors.ErrUpdateServiceSafepointFailed.RFCCode(), errCode)
}
