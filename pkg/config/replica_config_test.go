// Copyright 2024 PingCAP, Inc.
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

package config

import (
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestReplicaConfig_EnableSplittableCheck_AutoAdjust(t *testing.T) {
	tests := []struct {
		name          string
		sinkURI       string
		userConfig    *ChangefeedSchedulerConfig
		expectedValue bool
	}{
		{
			name:    "MySQL downstream - auto set to true",
			sinkURI: "mysql://localhost:3306/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     util.AddressOf(true),
				RegionThreshold:            util.AddressOf(100000),
				RegionCountPerSpan:         util.AddressOf(100),
				RegionCountRefreshInterval: util.AddressOf(5 * time.Minute),
				WriteKeyThreshold:          util.AddressOf(1000),
				SchedulingTaskCountPerNode: util.AddressOf(20),
				BalanceScoreThreshold:      util.AddressOf(20),
				MinTrafficPercentage:       util.AddressOf(0.8),
				MaxTrafficPercentage:       util.AddressOf(1.25),
				EnableSplittableCheck:      util.AddressOf(false), // User sets to false
			},
			expectedValue: true, // Should be auto-adjusted to true
		},
		{
			name:    "TiDB downstream - auto set to true",
			sinkURI: "tidb://localhost:4000/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     util.AddressOf(true),
				RegionThreshold:            util.AddressOf(100000),
				RegionCountPerSpan:         util.AddressOf(100),
				RegionCountRefreshInterval: util.AddressOf(5 * time.Minute),
				WriteKeyThreshold:          util.AddressOf(1000),
				SchedulingTaskCountPerNode: util.AddressOf(20),
				BalanceScoreThreshold:      util.AddressOf(20),
				MinTrafficPercentage:       util.AddressOf(0.8),
				MaxTrafficPercentage:       util.AddressOf(1.25),
				EnableSplittableCheck:      util.AddressOf(false), // User sets to false
			},
			expectedValue: true, // Should be auto-adjusted to true
		},
		{
			name:    "MySQL SSL downstream - auto set to true",
			sinkURI: "mysql+ssl://localhost:3306/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     util.AddressOf(true),
				RegionThreshold:            util.AddressOf(100000),
				RegionCountPerSpan:         util.AddressOf(100),
				RegionCountRefreshInterval: util.AddressOf(5 * time.Minute),
				WriteKeyThreshold:          util.AddressOf(1000),
				SchedulingTaskCountPerNode: util.AddressOf(20),
				BalanceScoreThreshold:      util.AddressOf(20),
				MinTrafficPercentage:       util.AddressOf(0.8),
				MaxTrafficPercentage:       util.AddressOf(1.25),
				EnableSplittableCheck:      util.AddressOf(false), // User sets to false
			},
			expectedValue: true, // Should be auto-adjusted to true
		},
		{
			name:    "Kafka downstream - respect user config true",
			sinkURI: "kafka://localhost:9092/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     util.AddressOf(true),
				RegionThreshold:            util.AddressOf(100000),
				RegionCountPerSpan:         util.AddressOf(100),
				RegionCountRefreshInterval: util.AddressOf(5 * time.Minute),
				WriteKeyThreshold:          util.AddressOf(1000),
				SchedulingTaskCountPerNode: util.AddressOf(20),
				BalanceScoreThreshold:      util.AddressOf(20),
				MinTrafficPercentage:       util.AddressOf(0.8),
				MaxTrafficPercentage:       util.AddressOf(1.25),
				EnableSplittableCheck:      util.AddressOf(true), // User sets to true
			},
			expectedValue: true, // Should respect user config
		},
		{
			name:    "Kafka downstream - respect user config false",
			sinkURI: "kafka://localhost:9092/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     util.AddressOf(true),
				RegionThreshold:            util.AddressOf(100000),
				RegionCountPerSpan:         util.AddressOf(100),
				RegionCountRefreshInterval: util.AddressOf(5 * time.Minute),
				WriteKeyThreshold:          util.AddressOf(1000),
				SchedulingTaskCountPerNode: util.AddressOf(20),
				BalanceScoreThreshold:      util.AddressOf(20),
				MinTrafficPercentage:       util.AddressOf(0.8),
				MaxTrafficPercentage:       util.AddressOf(1.25),
				EnableSplittableCheck:      util.AddressOf(false), // User sets to false
			},
			expectedValue: false, // Should respect user config
		},
		{
			name:    "Kafka downstream - use default value",
			sinkURI: "kafka://localhost:9092/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     util.AddressOf(true),
				RegionThreshold:            util.AddressOf(100000),
				RegionCountPerSpan:         util.AddressOf(100),
				RegionCountRefreshInterval: util.AddressOf(5 * time.Minute),
				WriteKeyThreshold:          util.AddressOf(1000),
				SchedulingTaskCountPerNode: util.AddressOf(20),
				BalanceScoreThreshold:      util.AddressOf(20),
				MinTrafficPercentage:       util.AddressOf(0.8),
				MaxTrafficPercentage:       util.AddressOf(1.25),
				// EnableSplittableCheck not set, should use default
			},
			expectedValue: false, // Should use default value
		},
		{
			name:    "Pulsar downstream - respect user config",
			sinkURI: "pulsar://localhost:6650/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     util.AddressOf(true),
				RegionThreshold:            util.AddressOf(100000),
				RegionCountPerSpan:         util.AddressOf(100),
				RegionCountRefreshInterval: util.AddressOf(5 * time.Minute),
				WriteKeyThreshold:          util.AddressOf(1000),
				SchedulingTaskCountPerNode: util.AddressOf(20),
				EnableSplittableCheck:      util.AddressOf(true), // User sets to true
				BalanceScoreThreshold:      util.AddressOf(20),
				MinTrafficPercentage:       util.AddressOf(0.8),
				MaxTrafficPercentage:       util.AddressOf(1.25),
			},
			expectedValue: true, // Should respect user config
		},
		{
			name:    "File storage downstream - respect user config",
			sinkURI: "file:///tmp/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     util.AddressOf(true),
				RegionThreshold:            util.AddressOf(100000),
				RegionCountPerSpan:         util.AddressOf(100),
				RegionCountRefreshInterval: util.AddressOf(5 * time.Minute),
				WriteKeyThreshold:          util.AddressOf(1000),
				SchedulingTaskCountPerNode: util.AddressOf(20),
				EnableSplittableCheck:      util.AddressOf(false), // User sets to false
				BalanceScoreThreshold:      util.AddressOf(20),
				MinTrafficPercentage:       util.AddressOf(0.8),
				MaxTrafficPercentage:       util.AddressOf(1.25),
			},
			expectedValue: false, // Should respect user config
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create config with user settings
			config := &ReplicaConfig{
				Scheduler: tt.userConfig,
			}

			// Parse sink URI
			sinkURI, err := url.Parse(tt.sinkURI)
			require.NoError(t, err)

			// Call ValidateAndAdjust
			err = config.ValidateAndAdjust(sinkURI)
			require.NoError(t, err)

			// Verify the final value
			require.Equal(t, tt.expectedValue, util.GetOrZero(config.Scheduler.EnableSplittableCheck))
		})
	}
}

func TestReplicaConfig_EnableSplittableCheck_DefaultValue(t *testing.T) {
	config := GetDefaultReplicaConfig()
	require.NotNil(t, config.Scheduler)
	require.False(t, util.GetOrZero(config.Scheduler.EnableSplittableCheck))
}

func TestReplicaConfig_OnlyOutputPKColumnsValidation(t *testing.T) {
	t.Parallel()

	t.Run("requires active active", func(t *testing.T) {
		cfg := GetDefaultReplicaConfig()
		cfg.EnableActiveActive = util.AddressOf(false)
		cfg.Sink.OnlyOutputPKColumns = util.AddressOf(true)

		sinkURI, err := url.Parse("file:///tmp/test?protocol=canal-json")
		require.NoError(t, err)
		err = cfg.ValidateAndAdjust(sinkURI)
		require.ErrorContains(t, err, "only-output-pk-columns requires enable-active-active to be true")
	})

	t.Run("requires canal json protocol", func(t *testing.T) {
		cfg := GetDefaultReplicaConfig()
		cfg.EnableActiveActive = util.AddressOf(true)
		cfg.Sink.OnlyOutputPKColumns = util.AddressOf(true)

		sinkURI, err := url.Parse("file:///tmp/test?protocol=open-protocol")
		require.NoError(t, err)
		err = cfg.ValidateAndAdjust(sinkURI)
		require.ErrorContains(t, err, "only-output-pk-columns only supports canal-json protocol")
	})

	t.Run("requires enable tidb extension", func(t *testing.T) {
		cfg := GetDefaultReplicaConfig()
		cfg.EnableActiveActive = util.AddressOf(true)
		cfg.Sink.OnlyOutputPKColumns = util.AddressOf(true)

		sinkURI, err := url.Parse("file:///tmp/test?protocol=canal-json")
		require.NoError(t, err)
		err = cfg.ValidateAndAdjust(sinkURI)
		require.ErrorContains(t, err, "only-output-pk-columns requires enable-tidb-extension to be true")
	})

	t.Run("valid with active active canal json and tidb extension", func(t *testing.T) {
		cfg := GetDefaultReplicaConfig()
		cfg.EnableActiveActive = util.AddressOf(true)
		cfg.Sink.OnlyOutputPKColumns = util.AddressOf(true)

		sinkURI, err := url.Parse("file:///tmp/test?protocol=canal-json&enable-tidb-extension=true")
		require.NoError(t, err)
		require.NoError(t, cfg.ValidateAndAdjust(sinkURI))
	})
}
