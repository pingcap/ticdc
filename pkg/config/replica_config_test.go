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
				EnableTableAcrossNodes:     true,
				RegionThreshold:            100000,
				RegionCountPerSpan:         100,
				WriteKeyThreshold:          1000,
				SchedulingTaskCountPerNode: 20,
				BalanceScoreThreshold:      20,
				MinTrafficPercentage:       0.8,
				MaxTrafficPercentage:       1.25,
				EnableSplittableCheck:      false, // User sets to false
			},
			expectedValue: true, // Should be auto-adjusted to true
		},
		{
			name:    "TiDB downstream - auto set to true",
			sinkURI: "tidb://localhost:4000/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     true,
				RegionThreshold:            100000,
				RegionCountPerSpan:         100,
				WriteKeyThreshold:          1000,
				SchedulingTaskCountPerNode: 20,
				BalanceScoreThreshold:      20,
				MinTrafficPercentage:       0.8,
				MaxTrafficPercentage:       1.25,
				EnableSplittableCheck:      false, // User sets to false
			},
			expectedValue: true, // Should be auto-adjusted to true
		},
		{
			name:    "MySQL SSL downstream - auto set to true",
			sinkURI: "mysql+ssl://localhost:3306/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     true,
				RegionThreshold:            100000,
				RegionCountPerSpan:         100,
				WriteKeyThreshold:          1000,
				SchedulingTaskCountPerNode: 20,
				BalanceScoreThreshold:      20,
				MinTrafficPercentage:       0.8,
				MaxTrafficPercentage:       1.25,
				EnableSplittableCheck:      false, // User sets to false
			},
			expectedValue: true, // Should be auto-adjusted to true
		},
		{
			name:    "Kafka downstream - respect user config true",
			sinkURI: "kafka://localhost:9092/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     true,
				RegionThreshold:            100000,
				RegionCountPerSpan:         100,
				WriteKeyThreshold:          1000,
				SchedulingTaskCountPerNode: 20,
				BalanceScoreThreshold:      20,
				MinTrafficPercentage:       0.8,
				MaxTrafficPercentage:       1.25,
				EnableSplittableCheck:      true, // User sets to true
			},
			expectedValue: true, // Should respect user config
		},
		{
			name:    "Kafka downstream - respect user config false",
			sinkURI: "kafka://localhost:9092/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     true,
				RegionThreshold:            100000,
				RegionCountPerSpan:         100,
				WriteKeyThreshold:          1000,
				SchedulingTaskCountPerNode: 20,
				BalanceScoreThreshold:      20,
				MinTrafficPercentage:       0.8,
				MaxTrafficPercentage:       1.25,
				EnableSplittableCheck:      false, // User sets to false
			},
			expectedValue: false, // Should respect user config
		},
		{
			name:    "Kafka downstream - use default value",
			sinkURI: "kafka://localhost:9092/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     true,
				RegionThreshold:            100000,
				RegionCountPerSpan:         100,
				WriteKeyThreshold:          1000,
				SchedulingTaskCountPerNode: 20,
				BalanceScoreThreshold:      20,
				MinTrafficPercentage:       0.8,
				MaxTrafficPercentage:       1.25,
				// EnableSplittableCheck not set, should use default
			},
			expectedValue: false, // Should use default value
		},
		{
			name:    "Pulsar downstream - respect user config",
			sinkURI: "pulsar://localhost:6650/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     true,
				RegionThreshold:            100000,
				RegionCountPerSpan:         100,
				WriteKeyThreshold:          1000,
				SchedulingTaskCountPerNode: 20,
				EnableSplittableCheck:      true, // User sets to true
				BalanceScoreThreshold:      20,
				MinTrafficPercentage:       0.8,
				MaxTrafficPercentage:       1.25,
			},
			expectedValue: true, // Should respect user config
		},
		{
			name:    "File storage downstream - respect user config",
			sinkURI: "file:///tmp/test",
			userConfig: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     true,
				RegionThreshold:            100000,
				RegionCountPerSpan:         100,
				WriteKeyThreshold:          1000,
				SchedulingTaskCountPerNode: 20,
				EnableSplittableCheck:      false, // User sets to false
				BalanceScoreThreshold:      20,
				MinTrafficPercentage:       0.8,
				MaxTrafficPercentage:       1.25,
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
			require.Equal(t, tt.expectedValue, config.Scheduler.EnableSplittableCheck)
		})
	}
}

func TestReplicaConfig_EnableSplittableCheck_DefaultValue(t *testing.T) {
	config := GetDefaultReplicaConfig()
	require.NotNil(t, config.Scheduler)
	require.False(t, config.Scheduler.EnableSplittableCheck)
}
