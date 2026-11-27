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

	"github.com/BurntSushi/toml"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestServerPullerSplitConfig(t *testing.T) {
	t.Parallel()
	cfg := GetDefaultServerConfig()
	require.Equal(t, &PullerSplitConfig{RegionThreshold: 500000, RegionCountPerSpan: 100000}, cfg.Puller)
	meta, err := toml.Decode(`[puller]
enable-span-split = true
region-threshold = 600000
region-count-per-span = 200000
`, cfg)
	require.NoError(t, err)
	require.Empty(t, meta.Undecoded())
	require.NoError(t, cfg.ValidateAndAdjust())
	require.Equal(t, &PullerSplitConfig{EnableSpanSplit: true, RegionThreshold: 600000, RegionCountPerSpan: 200000}, cfg.Puller)
	clone := cfg.Clone()
	require.Equal(t, cfg.Puller, clone.Puller)
	clone.Puller.EnableSpanSplit = false
	require.True(t, cfg.Puller.EnableSpanSplit)

	cfg = GetDefaultServerConfig()
	cfg.Puller = nil
	require.NoError(t, cfg.ValidateAndAdjust())
	require.Equal(t, NewDefaultPullerSplitConfig(), cfg.Puller)

	for _, test := range []struct {
		threshold int
		perSpan   int
		valid     bool
	}{
		{500000, 100000, true},
		{100000, 100000, true},
		{0, 100000, false},
		{-1, 100000, false},
		{500000, 0, false},
		{500000, -1, false},
		{500000, 500001, false},
	} {
		cfg := GetDefaultServerConfig()
		cfg.Puller.RegionThreshold = test.threshold
		cfg.Puller.RegionCountPerSpan = test.perSpan
		err := cfg.ValidateAndAdjust()
		if test.valid {
			require.NoError(t, err)
		} else {
			require.True(t, errors.ErrInvalidServerOption.Equal(err))
		}
	}
}
