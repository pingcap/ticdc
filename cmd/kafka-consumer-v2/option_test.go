// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestApplySyncpointConfigUsesCommandLineOverrides(t *testing.T) {
	cfg := config.GetDefaultReplicaConfig()
	configEnableSyncpoint := false
	configInterval := 30 * time.Minute
	configRetention := 48 * time.Hour
	cfg.EnableSyncPoint = &configEnableSyncpoint
	cfg.SyncPointInterval = &configInterval
	cfg.SyncPointRetention = &configRetention

	o := newOption()
	o.enableSyncpoint = true
	o.syncpointInterval = time.Minute
	o.syncpointRetention = 2 * time.Hour
	o.enableSyncpointSet = true
	o.syncpointIntervalSet = true
	o.syncpointRetentionSet = true

	o.applySyncpointConfig(cfg)
	require.True(t, o.enableSyncpoint)
	require.Equal(t, time.Minute, o.syncpointInterval)
	require.Equal(t, 2*time.Hour, o.syncpointRetention)
}

func TestApplySyncpointConfigKeepsReplicaConfigWhenNoOverride(t *testing.T) {
	cfg := config.GetDefaultReplicaConfig()
	configEnableSyncpoint := true
	configInterval := 30 * time.Minute
	configRetention := 48 * time.Hour
	cfg.EnableSyncPoint = &configEnableSyncpoint
	cfg.SyncPointInterval = &configInterval
	cfg.SyncPointRetention = &configRetention

	o := newOption()
	o.applySyncpointConfig(cfg)
	require.True(t, o.enableSyncpoint)
	require.Equal(t, 30*time.Minute, o.syncpointInterval)
	require.Equal(t, 48*time.Hour, o.syncpointRetention)
}
