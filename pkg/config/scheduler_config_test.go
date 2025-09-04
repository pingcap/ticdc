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

func TestChangefeedSchedulerConfig_Integration(t *testing.T) {
	t.Run("config should work with ReplicaConfig validation", func(t *testing.T) {
		replicaConfig := &ReplicaConfig{
			Scheduler: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     true,
				RegionThreshold:            100000,
				RegionCountPerSpan:         100,
				WriteKeyThreshold:          1000,
				SchedulingTaskCountPerNode: 20,
				EnableSplittableCheck:      false,
			},
		}

		sinkURI, err := url.Parse("kafka://localhost:9092/test")
		require.NoError(t, err)

		err = replicaConfig.ValidateAndAdjust(sinkURI)
		require.NoError(t, err)

		// Should respect user config for non-MySQL downstream
		require.False(t, replicaConfig.Scheduler.EnableSplittableCheck)
	})

	t.Run("MySQL downstream should override user config", func(t *testing.T) {
		replicaConfig := &ReplicaConfig{
			Scheduler: &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     true,
				RegionThreshold:            100000,
				RegionCountPerSpan:         100,
				WriteKeyThreshold:          1000,
				SchedulingTaskCountPerNode: 20,
				EnableSplittableCheck:      false, // User sets to false
			},
		}

		sinkURI, err := url.Parse("mysql://localhost:3306/test")
		require.NoError(t, err)

		err = replicaConfig.ValidateAndAdjust(sinkURI)
		require.NoError(t, err)

		// Should be auto-adjusted to true for MySQL downstream
		require.True(t, replicaConfig.Scheduler.EnableSplittableCheck)
	})
}
