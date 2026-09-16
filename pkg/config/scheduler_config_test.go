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

package config

import (
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestChangefeedSchedulerConfigValidateAndAdjustRegionCountPerSpan(t *testing.T) {
	sinkURI, err := url.Parse("kafka://localhost:9092/test")
	require.NoError(t, err)

	tests := []struct {
		name                       string
		regionThreshold            int
		regionCountPerSpan         int
		expectedRegionCountPerSpan int
	}{
		{
			name:                       "adjust_when_region_count_per_span_exceeds_threshold",
			regionThreshold:            100,
			regionCountPerSpan:         200,
			expectedRegionCountPerSpan: 100,
		},
		{
			name:                       "keep_when_region_count_per_span_not_exceed_threshold",
			regionThreshold:            100,
			regionCountPerSpan:         80,
			expectedRegionCountPerSpan: 80,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &ChangefeedSchedulerConfig{
				EnableTableAcrossNodes:     util.AddressOf(true),
				RegionThreshold:            util.AddressOf(tt.regionThreshold),
				RegionCountPerSpan:         util.AddressOf(tt.regionCountPerSpan),
				RegionCountRefreshInterval: util.AddressOf(5 * time.Minute),
				WriteKeyThreshold:          util.AddressOf(MinWriteKeyThreshold),
				SchedulingTaskCountPerNode: util.AddressOf(20),
				BalanceScoreThreshold:      util.AddressOf(20),
				MinTrafficPercentage:       util.AddressOf(0.8),
				MaxTrafficPercentage:       util.AddressOf(1.25),
				EnableSplittableCheck:      util.AddressOf(false),
			}

			err := cfg.ValidateAndAdjust(sinkURI)
			require.NoError(t, err)
			require.Equal(t, tt.expectedRegionCountPerSpan, util.GetOrZero(cfg.RegionCountPerSpan))
		})
	}
}
