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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPullerConfigValidateAndAdjustRegionRequestWindow(t *testing.T) {
	defaultCfg := NewDefaultPullerConfig()
	require.Equal(t, 32, defaultCfg.PendingRegionRequestQueueSize)
	require.Equal(t, 4, defaultCfg.RegionRequestMaxWindowMultiplier)
	require.Equal(t, uint64(1024*1024*1024), defaultCfg.MemoryQuota)
	require.Equal(t, uint64(8*1024*1024), defaultCfg.ScanBaseSize)

	cfg := &PullerConfig{
		PendingRegionRequestQueueSize:    -1,
		RegionRequestMaxWindowMultiplier: 0,
	}
	cfg.ValidateAndAdjust()
	require.Equal(t, defaultCfg.PendingRegionRequestQueueSize, cfg.PendingRegionRequestQueueSize)
	require.Equal(t, defaultCfg.RegionRequestMaxWindowMultiplier, cfg.RegionRequestMaxWindowMultiplier)
	require.Equal(t, defaultCfg.MemoryQuota, cfg.MemoryQuota)
	require.Equal(t, defaultCfg.ScanBaseSize, cfg.ScanBaseSize)
}
