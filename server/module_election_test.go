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
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/liveness"
	"github.com/pingcap/ticdc/pkg/node"
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

func TestRunLogCoordinatorStopsWhenNodeStartsDraining(t *testing.T) {
	started := make(chan struct{})
	e := &elector{svr: &server{
		info:     &node.Info{ID: "node-a"},
		liveness: liveness.CaptureAlive,
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- e.runLogCoordinator(ctx, func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("log coordinator did not start")
	}
	require.True(t, e.svr.liveness.Store(liveness.CaptureDraining))

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("log coordinator did not stop after the node started draining")
	}
}
