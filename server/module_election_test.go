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

package server

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCoordinatorSchedulerSettingsUsesCapturedConfig(t *testing.T) {
	// Scenario: the coordinator should honor the scheduler config captured during server startup.
	// Steps: install a different global config, read settings from an explicit scheduler config,
	// and verify both the concurrency limit and balance interval come from the captured config.
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() {
		config.StoreGlobalServerConfig(original)
	})

	globalCfg := config.GetDefaultServerConfig()
	globalCfg.Debug.Scheduler.MaxTaskConcurrency = 9
	globalCfg.Debug.Scheduler.CheckBalanceInterval = config.TomlDuration(44 * time.Second)
	config.StoreGlobalServerConfig(globalCfg)

	cfg := config.GetDefaultServerConfig()
	cfg.Debug.Scheduler.MaxTaskConcurrency = 3
	cfg.Debug.Scheduler.CheckBalanceInterval = config.TomlDuration(22 * time.Second)

	maxTaskConcurrency, checkBalanceInterval := coordinatorSchedulerSettings(cfg.Debug.Scheduler)
	require.Equal(t, 3, maxTaskConcurrency)
	require.Equal(t, 22*time.Second, checkBalanceInterval)
}
