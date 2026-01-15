// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package v2

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestReplicaConfigConversion(t *testing.T) {
	t.Parallel()

	// Test case 1: All fields are set
	apiCfg := &ReplicaConfig{
		MemoryQuota:           util.AddressOf(uint64(1024)),
		CaseSensitive:         util.AddressOf(true),
		ForceReplicate:        util.AddressOf(true),
		IgnoreIneligibleTable: util.AddressOf(true),
		CheckGCSafePoint:      util.AddressOf(true),
		EnableSyncPoint:       util.AddressOf(true),
		EnableTableMonitor:    util.AddressOf(true),
		BDRMode:               util.AddressOf(true),
		Mounter: &MounterConfig{
			WorkerNum: util.AddressOf(16),
		},
		Scheduler: &ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: util.AddressOf(true),
			RegionThreshold:        util.AddressOf(1000),
		},
		Integrity: &IntegrityConfig{
			IntegrityCheckLevel:   util.AddressOf("correctness"),
			CorruptionHandleLevel: util.AddressOf("warn"),
		},
		Consistent: &ConsistentConfig{
			Level:             util.AddressOf("eventual"),
			MaxLogSize:        util.AddressOf(int64(128)),
			FlushIntervalInMs: util.AddressOf(int64(2000)),
			Storage:           util.AddressOf("s3://test"),
		},
	}

	internalCfg := apiCfg.ToInternalReplicaConfig()
	require.Equal(t, uint64(1024), util.GetOrZero(internalCfg.MemoryQuota))
	require.True(t, util.GetOrZero(internalCfg.CaseSensitive))
	require.True(t, util.GetOrZero(internalCfg.ForceReplicate))
	require.True(t, util.GetOrZero(internalCfg.IgnoreIneligibleTable))
	require.True(t, util.GetOrZero(internalCfg.CheckGCSafePoint))
	require.True(t, util.GetOrZero(internalCfg.EnableSyncPoint))
	require.True(t, util.GetOrZero(internalCfg.EnableTableMonitor))
	require.True(t, util.GetOrZero(internalCfg.BDRMode))
	require.Equal(t, internalCfg.Mounter.WorkerNum, *apiCfg.Mounter.WorkerNum)
	require.True(t, util.GetOrZero(internalCfg.Scheduler.EnableTableAcrossNodes))
	require.Equal(t, 1000, util.GetOrZero(internalCfg.Scheduler.RegionThreshold))
	require.Equal(t, "correctness", util.GetOrZero(internalCfg.Integrity.IntegrityCheckLevel))
	require.Equal(t, "warn", util.GetOrZero(internalCfg.Integrity.CorruptionHandleLevel))
	require.Equal(t, "eventual", util.GetOrZero(internalCfg.Consistent.Level))
	require.Equal(t, int64(128), util.GetOrZero(internalCfg.Consistent.MaxLogSize))
	require.Equal(t, int64(2000), util.GetOrZero(internalCfg.Consistent.FlushIntervalInMs))
	require.Equal(t, "s3://test", util.GetOrZero(internalCfg.Consistent.Storage))

	// Test case 2: Nil fields (should use defaults or be nil)
	apiCfgNil := &ReplicaConfig{}
	internalCfgNil := apiCfgNil.ToInternalReplicaConfig()
	// Check defaults from GetDefaultReplicaConfig which ToInternalReplicaConfig uses as base
	defaultCfg := config.GetDefaultReplicaConfig()
	require.Equal(t, util.GetOrZero(defaultCfg.MemoryQuota), util.GetOrZero(internalCfgNil.MemoryQuota))
	require.Equal(t, util.GetOrZero(defaultCfg.CaseSensitive), util.GetOrZero(internalCfgNil.CaseSensitive))

	// Test case 3: Conversion back to API config
	apiCfgBack := ToAPIReplicaConfig(internalCfg)
	require.Equal(t, uint64(1024), *apiCfgBack.MemoryQuota)
	require.True(t, *apiCfgBack.CaseSensitive)
	require.True(t, *apiCfgBack.ForceReplicate)
	require.True(t, *apiCfgBack.IgnoreIneligibleTable)
	require.Equal(t, 16, *apiCfgBack.Mounter.WorkerNum)
	require.True(t, *apiCfgBack.Scheduler.EnableTableAcrossNodes)
	require.Equal(t, "correctness", *apiCfgBack.Integrity.IntegrityCheckLevel)
	require.Equal(t, "eventual", *apiCfgBack.Consistent.Level)
}
