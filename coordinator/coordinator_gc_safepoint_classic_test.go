//go:build !nextgen

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

func TestUpdateGCSafepointUsesServiceGCSafepointInClassic(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	checkpointTs := uint64(100)

	cf := newChangefeedForGCTest(t, "cf", "ks", 0, checkpointTs)

	changefeedDB := changefeed.NewChangefeedDB(1)
	changefeedDB.AddAbsentChangefeed(cf)

	controller := &Controller{
		changefeedDB: changefeedDB,
	}

	mockGCManager := gc.NewMockManager(ctrl)
	mockGCManager.EXPECT().
		TryUpdateServiceGCSafepoint(gomock.Any(), common.Ts(checkpointTs-1)).
		Return(nil)

	c := &coordinator{
		controller: controller,
		gcManager:  mockGCManager,
	}

	require.NoError(t, c.updateGCSafepoint(context.Background()))
}
