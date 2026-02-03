//go:build nextgen

package coordinator

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/stretchr/testify/require"
)

func TestUpdateGCSafepointUsesKeyspaceGCBarrierInNextGen(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	checkpointTs1 := uint64(100)
	checkpointTs2 := uint64(200)

	cf1 := newChangefeedForGCTest(t, "cf1", "ks1", 101, checkpointTs1)
	cf2 := newChangefeedForGCTest(t, "cf2", "ks2", 202, checkpointTs2)

	changefeedDB := changefeed.NewChangefeedDB(1)
	changefeedDB.AddAbsentChangefeed(cf1, cf2)

	controller := &Controller{
		changefeedDB: changefeedDB,
	}

	mockGCManager := gc.NewMockManager(ctrl)
	mockGCManager.EXPECT().
		TryUpdateKeyspaceGCBarrier(gomock.Any(), uint32(101), "ks1", common.Ts(checkpointTs1-1)).
		Return(nil)
	mockGCManager.EXPECT().
		TryUpdateKeyspaceGCBarrier(gomock.Any(), uint32(202), "ks2", common.Ts(checkpointTs2-1)).
		Return(nil)

	c := &coordinator{
		controller: controller,
		gcManager:  mockGCManager,
	}

	require.NoError(t, c.updateGCSafepoint(context.Background()))
}
