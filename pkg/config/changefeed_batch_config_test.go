package config

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestChangeFeedInfoToChangefeedConfig_EventCollectorBatchConfig(t *testing.T) {
	t.Parallel()

	info := &ChangeFeedInfo{
		ChangefeedID: common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName),
		StartTs:      1,
		TargetTs:     2,
		SinkURI:      "blackhole://",
		Epoch:        3,
		Config: &ReplicaConfig{
			Sink:                     &SinkConfig{},
			Scheduler:                &ChangefeedSchedulerConfig{},
			EventCollectorBatchCount: util.AddressOf(uint64(4096)),
			EventCollectorBatchBytes: util.AddressOf(uint64(64 * 1024 * 1024)),
		},
	}

	cfg := info.ToChangefeedConfig()
	require.Equal(t, uint64(4096), cfg.EventCollectorBatchCount)
	require.Equal(t, uint64(64*1024*1024), cfg.EventCollectorBatchBytes)
}

// todo: why this test, what's the purpose?
func TestChangeFeedInfoToChangefeedConfig_EventCollectorBatchConfigDefaultZero(t *testing.T) {
	t.Parallel()

	info := &ChangeFeedInfo{
		ChangefeedID: common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName),
		StartTs:      1,
		TargetTs:     2,
		SinkURI:      "blackhole://",
		Epoch:        3,
		Config: &ReplicaConfig{
			Sink:      &SinkConfig{},
			Scheduler: &ChangefeedSchedulerConfig{},
		},
	}

	cfg := info.ToChangefeedConfig()
	require.Equal(t, uint64(0), cfg.EventCollectorBatchCount)
	require.Equal(t, uint64(0), cfg.EventCollectorBatchBytes)
}
