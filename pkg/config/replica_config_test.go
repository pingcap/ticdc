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

func TestReplicaConfigPerformanceMode(t *testing.T) {
	sinkURI, err := url.Parse("mysql://localhost:3306/test")
	require.NoError(t, err)

	cfg := GetDefaultReplicaConfig()
	require.Equal(t, PerformanceModeThroughput, util.GetOrZero(cfg.PerformanceMode))
	require.False(t, cfg.IsLowLatencyMode())

	cfg.PerformanceMode = util.AddressOf(PerformanceModeLowLatency)
	require.NoError(t, cfg.ValidateAndAdjust(sinkURI))
	require.True(t, cfg.IsLowLatencyMode())

	cfg.PerformanceMode = util.AddressOf("invalid")
	require.ErrorContains(t, cfg.ValidateAndAdjust(sinkURI), "unknown performance mode")

	cfg.PerformanceMode = nil
	require.NoError(t, cfg.ValidateAndAdjust(sinkURI))
	require.Equal(t, PerformanceModeThroughput, util.GetOrZero(cfg.PerformanceMode))
}

// TestReplicaConfigValidateBatchConfig verifies validation accepts zero as an
// explicit override and rejects values outside the supported range.
func TestReplicaConfigValidateBatchConfig(t *testing.T) {
	sinkURI, err := url.Parse("mysql://localhost:3306/test")
	require.NoError(t, err)

	assertBatchConfig := func(batchCount *int, batchBytes *int, wantErr string) {
		cfg := GetDefaultReplicaConfig()
		cfg.EventCollectorBatchCount = batchCount
		cfg.EventCollectorBatchBytes = batchBytes

		err := cfg.ValidateAndAdjust(sinkURI)
		if wantErr != "" {
			require.ErrorContains(t, err, wantErr)
			return
		}
		require.NoError(t, err)
	}

	assertBatchConfig(util.AddressOf(0), nil, "")
	assertBatchConfig(nil, util.AddressOf(0), "")
	assertBatchConfig(util.AddressOf(1), util.AddressOf(1), "")
	assertBatchConfig(util.AddressOf(MaxEventCollectorBatchCount), nil, "")
	assertBatchConfig(util.AddressOf(MaxEventCollectorBatchCount+1), nil, "event-collector-batch-count")
	assertBatchConfig(util.AddressOf(-1), nil, "event-collector-batch-count")
	assertBatchConfig(nil, util.AddressOf(-1), "event-collector-batch-bytes")
}

// TestConsistentFlushBatchSizeValidation verifies that adjustment supplies the
// disabled-by-default value, preserves non-negative overrides, and rejects a
// negative row-count threshold before the redo writer is created.
func TestConsistentFlushBatchSizeValidation(t *testing.T) {
	require.Equal(t, 0, util.GetOrZero(GetDefaultReplicaConfig().Consistent.FlushBatchSize))

	tests := []struct {
		name      string
		value     *int
		wantValue int
		wantErr   bool
	}{
		{name: "unset uses disabled default", wantValue: 0},
		{name: "zero disables count based flush", value: util.AddressOf(0), wantValue: 0},
		{name: "positive value enables count based flush", value: util.AddressOf(2048), wantValue: 2048},
		{name: "negative value is rejected", value: util.AddressOf(-1), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &ConsistentConfig{
				Level:          util.AddressOf("eventual"),
				FlushBatchSize: tt.value,
				Storage:        util.AddressOf("blackhole://"),
			}

			err := cfg.validateAndAdjust(false)
			if tt.wantErr {
				require.ErrorContains(t, err, "consistent.flush-batch-size")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, cfg.FlushBatchSize)
			require.Equal(t, tt.wantValue, *cfg.FlushBatchSize)
		})
	}
}

func TestReplicaConfig_EnableRedoIOCheck_DefaultValue(t *testing.T) {
	config := GetDefaultReplicaConfig()
	require.True(t, util.GetOrZero(config.EnableRedoIOCheck))
}

func TestReplicaConfig_EnableRedoIOCheck_DefaultEnabled(t *testing.T) {
	config := GetDefaultReplicaConfig()
	config.Consistent.Level = util.AddressOf("eventual")
	config.Consistent.Storage = util.AddressOf("s3:///redo-test-no-bucket")

	sinkURI, err := url.Parse("blackhole://")
	require.NoError(t, err)
	require.Error(t, config.ValidateAndAdjust(sinkURI))
}

func TestReplicaConfig_EnableRedoIOCheck_CanDisableForCLI(t *testing.T) {
	config := GetDefaultReplicaConfig()
	config.EnableRedoIOCheck = util.AddressOf(false)
	config.Consistent.Level = util.AddressOf("eventual")
	config.Consistent.Storage = util.AddressOf("s3:///redo-test-no-bucket")

	sinkURI, err := url.Parse("blackhole://")
	require.NoError(t, err)
	require.NoError(t, config.ValidateAndAdjust(sinkURI))
}
