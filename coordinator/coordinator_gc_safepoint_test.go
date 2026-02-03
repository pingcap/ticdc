package coordinator

import (
	"testing"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
)

func newChangefeedForGCTest(
	t *testing.T, changefeedName string, keyspace string, keyspaceID uint32, checkpointTs uint64,
) *changefeed.Changefeed {
	t.Helper()

	cfID := common.NewChangeFeedIDWithName(changefeedName, keyspace)
	info := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		SinkURI:      "blackhole://",
		Config:       config.GetDefaultReplicaConfig(),
		State:        config.StateNormal,
		KeyspaceID:   keyspaceID,
	}

	return changefeed.NewChangefeed(cfID, info, checkpointTs, false)
}
