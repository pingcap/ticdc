package dispatchermanager

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/stretchr/testify/require"
)

func TestDispatcherSetChecksumWatermarkSuppression(t *testing.T) {
	oldID := appcontext.GetID()
	appcontext.SetID("test-capture")
	t.Cleanup(func() { appcontext.SetID(oldID) })

	manager := createTestManager(t)
	d1 := createTestDispatcher(t, manager, common.NewDispatcherID(), 1, []byte("a"), []byte("b"))
	manager.dispatcherMap.Set(d1.GetId(), d1)

	t.Run("uninitialized", func(t *testing.T) {
		manager.ResetDispatcherSetChecksum()

		req := manager.aggregateDispatcherHeartbeats(false)
		require.Equal(t, heartbeatpb.ChecksumState_UNINITIALIZED, req.ChecksumState)
		require.Nil(t, req.Watermark)
	})

	t.Run("mismatch", func(t *testing.T) {
		manager.ResetDispatcherSetChecksum()

		actual := manager.computeDispatcherSetChecksum(common.DefaultMode)
		checksum := actual.ToPB()
		checksum.Count++
		manager.ApplyDispatcherSetChecksumUpdate(&heartbeatpb.DispatcherSetChecksumUpdate{
			Epoch:    1,
			Mode:     common.DefaultMode,
			Seq:      1,
			Checksum: checksum,
		})

		req := manager.aggregateDispatcherHeartbeats(false)
		require.Equal(t, heartbeatpb.ChecksumState_MISMATCH, req.ChecksumState)
		require.Nil(t, req.Watermark)
	})

	t.Run("ok", func(t *testing.T) {
		manager.ResetDispatcherSetChecksum()

		actual := manager.computeDispatcherSetChecksum(common.DefaultMode)
		manager.ApplyDispatcherSetChecksumUpdate(&heartbeatpb.DispatcherSetChecksumUpdate{
			Epoch:    1,
			Mode:     common.DefaultMode,
			Seq:      1,
			Checksum: actual.ToPB(),
		})

		req := manager.aggregateDispatcherHeartbeats(false)
		require.Equal(t, heartbeatpb.ChecksumState_OK, req.ChecksumState)
		require.NotNil(t, req.Watermark)
	})
}
