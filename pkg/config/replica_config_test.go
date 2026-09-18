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

	"github.com/BurntSushi/toml"
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

func TestReplicaConfig_AllowSameCluster(t *testing.T) {
	t.Parallel()

	cfg := GetDefaultReplicaConfig()
	require.Nil(t, cfg.AllowSameCluster)

	// `allow-same-cluster` is a top-level option of the changefeed config file.
	metaData, err := toml.Decode("allow-same-cluster = true\n", cfg)
	require.NoError(t, err)
	require.Empty(t, metaData.Undecoded())
	require.True(t, util.GetOrZero(cfg.AllowSameCluster))

	// Clone must keep the option, the config is cloned when it is converted to the API model.
	require.True(t, util.GetOrZero(cfg.Clone().AllowSameCluster))

	// The same upstream/downstream check runs on the ChangefeedConfig derived from the persisted
	// replica config, so the option must survive the conversion to skip the check.
	info := &ChangeFeedInfo{Config: cfg}
	require.True(t, info.ToChangefeedConfig().AllowSameCluster)
}

func TestReplicaConfigTableRouteSupport(t *testing.T) {
	cases := []struct {
		name         string
		uri          string
		routed       bool
		redoEnabled  bool
		activeActive bool
		// allowSameCluster mirrors the changefeed config `allow-same-cluster`.
		allowSameCluster bool
		// filterRules defaults to `*.*` when it is nil.
		filterRules []string
		wantError   string
	}{
		{name: "mysql routing", uri: "mysql://localhost:3306", routed: true},
		{name: "tidb routing", uri: "tidb://localhost:4000", routed: true},
		{name: "kafka routing", uri: "kafka://localhost:9092/topic?protocol=open-protocol", routed: true, wantError: "table routing only supports MySQL-compatible sinks"},
		{name: "storage routing", uri: "file:///tmp/table-route", routed: true, wantError: "table routing only supports MySQL-compatible sinks"},
		{name: "blackhole routing", uri: "blackhole://", routed: true, wantError: "table routing only supports MySQL-compatible sinks"},
		{name: "redo with routing", uri: "mysql://localhost:3306", routed: true, redoEnabled: true, wantError: "table routing is incompatible with redo"},
		{name: "redo without routing", uri: "mysql://localhost:3306", redoEnabled: true},
		{name: "active active with routing", uri: "tidb://localhost:4000", routed: true, activeActive: true},
		{name: "active active without routing", uri: "tidb://localhost:4000", activeActive: true},
		{name: "kafka dispatch without routing", uri: "kafka://localhost:9092/topic?protocol=open-protocol"},
		{name: "allow-same-cluster without routing", uri: "mysql://localhost:3306", allowSameCluster: true, wantError: "allow-same-cluster requires table routing to be enabled"},
		{name: "allow-same-cluster with routing", uri: "mysql://localhost:3306", routed: true, allowSameCluster: true, filterRules: []string{"db.*"}},
		{name: "allow-same-cluster with kafka sink", uri: "kafka://localhost:9092/topic?protocol=open-protocol", allowSameCluster: true, wantError: "allow-same-cluster requires table routing to be enabled"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := GetDefaultReplicaConfig()
			cfg.EnableRedoIOCheck = util.AddressOf(false)
			cfg.EnableActiveActive = util.AddressOf(tc.activeActive)
			cfg.BDRMode = util.AddressOf(tc.activeActive)
			cfg.AllowSameCluster = util.AddressOf(tc.allowSameCluster)
			if tc.filterRules != nil {
				cfg.Filter.Rules = tc.filterRules
			}
			if tc.redoEnabled {
				cfg.Consistent.Level = util.AddressOf("eventual")
				cfg.Consistent.Storage = util.AddressOf("file:///tmp/table-route-redo")
			}
			rule := &DispatchRule{Matcher: []string{"db.*"}, PartitionRule: "table", TopicRule: "topic"}
			if tc.routed {
				rule.TargetSchema = "archive"
			}
			cfg.Sink.DispatchRules = []*DispatchRule{rule}
			sinkURI, err := url.Parse(tc.uri)
			require.NoError(t, err)
			err = cfg.ValidateAndAdjust(sinkURI)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
				require.Equal(t, "table", rule.PartitionRule)
				require.Equal(t, "topic", rule.TopicRule)
			}
		})
	}
}
