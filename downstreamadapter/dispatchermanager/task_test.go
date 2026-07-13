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

package dispatchermanager

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestHeartbeatIntervalsByPerformanceMode(t *testing.T) {
	original := config.GetGlobalServerConfig()
	t.Cleanup(func() {
		config.StoreGlobalServerConfig(original)
	})

	cfg := original.Clone()
	config.StoreGlobalServerConfig(cfg)
	require.Equal(t, defaultHeartbeatInterval, heartbeatInterval())
	require.Equal(t, defaultHeartbeatInitialDelay, heartbeatInitialDelay())

	cfg.PerformanceMode = config.PerformanceModeLowLatency
	config.StoreGlobalServerConfig(cfg)
	require.Equal(t, lowLatencyHeartbeatInterval, heartbeatInterval())
	require.Zero(t, heartbeatInitialDelay())
}
