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

import "github.com/pingcap/ticdc/pkg/errors"

// PullerSplitConfig controls server-local subdivision of upstream subscriptions.
// Internal spans share the original subscription's consumer and storage.
type PullerSplitConfig struct {
	EnableSpanSplit bool `toml:"enable-span-split" json:"enable-span-split"`
	// RegionThreshold must be exceeded before a subscription is subdivided.
	RegionThreshold int `toml:"region-threshold" json:"region-threshold"`
	// RegionCountPerSpan is the target size at subscription creation. Later TiKV
	// region splits and merges do not change the internal span boundaries.
	RegionCountPerSpan int `toml:"region-count-per-span" json:"region-count-per-span"`
}

// NewDefaultPullerSplitConfig returns the public server puller defaults.
func NewDefaultPullerSplitConfig() *PullerSplitConfig {
	return &PullerSplitConfig{
		RegionThreshold:    500000,
		RegionCountPerSpan: 100000,
	}
}

// Validate checks the split threshold and target span size.
func (c *PullerSplitConfig) Validate() error {
	if c.RegionThreshold <= 0 {
		return errors.ErrInvalidServerOption.GenWithStackByArgs("puller.region-threshold must be greater than 0")
	}
	if c.RegionCountPerSpan <= 0 || c.RegionCountPerSpan > c.RegionThreshold {
		return errors.ErrInvalidServerOption.GenWithStackByArgs(
			"puller.region-count-per-span must be greater than 0 and no greater than puller.region-threshold")
	}
	return nil
}
