// Copyright 2022 PingCAP, Inc.
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
	"errors"
	"net/url"
	"time"

	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

const (
	// MinWriteKeyThreshold is the minimum allowed value for WriteKeyThreshold
	MinWriteKeyThreshold = 10485760 // 10MB
)

// ChangefeedSchedulerConfig is per changefeed scheduler settings.
type ChangefeedSchedulerConfig struct {
	// EnableTableAcrossNodes set true to split one table to multiple spans and
	// distribute to multiple TiCDC nodes.
	EnableTableAcrossNodes *bool `toml:"enable-table-across-nodes" json:"enable-table-across-nodes,omitempty"`
	// RegionThreshold is the region count threshold of splitting a table.
	RegionThreshold *int `toml:"region-threshold" json:"region-threshold,omitempty"`
	// RegionCountPerSpan is the maximax region count for each span when first splitted by RegionCountSpliiter
	RegionCountPerSpan *int `toml:"region-count-per-span" json:"region-count-per-span,omitempty"`
	// RegionCountRefreshInterval controls how often we refresh span region count with PD.
	RegionCountRefreshInterval *time.Duration `toml:"region-count-refresh-interval" json:"region-count-refresh-interval,omitempty"`
	// WriteKeyThreshold is the written keys threshold of splitting a table.
	WriteKeyThreshold *int `toml:"write-key-threshold" json:"write-key-threshold,omitempty"`
	// SchedulingTaskCountPerNode is the upper limit for scheduling tasks each node.
	SchedulingTaskCountPerNode *int `toml:"scheduling-task-count-per-node" json:"scheduling-task-count-per-node,omitempty"`
	// EnableSplittableCheck controls whether to check if a table is splittable before splitting.
	// If true, only tables with primary key and no unique key can be split.
	// If false, all tables can be split without checking.
	// For MySQL downstream, this is always set to true for data consistency.
	EnableSplittableCheck *bool `toml:"enable-splittable-check" json:"enable-splittable-check"`
	// ForceSplit controls whether to skip the splittable table check for MySQL downstream.
	// If true, the splittable table check will be skipped even if the downstream is MySQL.
	// This is useful for advanced users who are aware of the risks of splitting unsplittable tables.
	// Default value is false.
	ForceSplit *bool `toml:"force-split" json:"force-split"`
	// These config is used for adjust the frequency of balancing traffic.
	// BalanceScoreThreshold is the score threshold for balancing traffic. Larger value means less frequent balancing.
	// Default value is 20
	BalanceScoreThreshold *int `toml:"balance-score-threshold" json:"balance-score-threshold,omitempty"`
	// MinTrafficPercentage is the minimum traffic percentage for balancing traffic. Larger value means less frequent balancing.
	// MinTrafficPercentage must be less then 1. Default value is 0.8
	MinTrafficPercentage *float64 `toml:"min-traffic-percentage" json:"min-traffic-percentage,omitempty"`
	// MaxTrafficPercentage is the maximum traffic percentage for balancing traffic. Less value means less frequent balancing.
	// MaxTrafficPercentage must be greater then 1. Default value is 1.25
	MaxTrafficPercentage *float64 `toml:"max-traffic-percentage" json:"max-traffic-percentage,omitempty"`
}

// FillMissingWithDefaults copies default values into invalid or zero fields.
func (c *ChangefeedSchedulerConfig) FillMissingWithDefaults(defaultCfg *ChangefeedSchedulerConfig) {
	if c == nil || defaultCfg == nil {
		return
	}
	if util.GetOrZero(c.RegionThreshold) <= 0 {
		c.RegionThreshold = defaultCfg.RegionThreshold
	}
	if util.GetOrZero(c.RegionCountPerSpan) <= 0 {
		c.RegionCountPerSpan = defaultCfg.RegionCountPerSpan
	}
	if c.RegionCountRefreshInterval == nil || *c.RegionCountRefreshInterval <= 0 {
		c.RegionCountRefreshInterval = defaultCfg.RegionCountRefreshInterval
	}
	if util.GetOrZero(c.WriteKeyThreshold) < 0 {
		c.WriteKeyThreshold = defaultCfg.WriteKeyThreshold
	}
	if util.GetOrZero(c.SchedulingTaskCountPerNode) <= 0 {
		c.SchedulingTaskCountPerNode = defaultCfg.SchedulingTaskCountPerNode
	}
	if util.GetOrZero(c.BalanceScoreThreshold) <= 0 {
		c.BalanceScoreThreshold = defaultCfg.BalanceScoreThreshold
	}
	if util.GetOrZero(c.MinTrafficPercentage) <= 0 || util.GetOrZero(c.MinTrafficPercentage) >= 1 {
		c.MinTrafficPercentage = defaultCfg.MinTrafficPercentage
	}
	if util.GetOrZero(c.MaxTrafficPercentage) <= 1 {
		c.MaxTrafficPercentage = defaultCfg.MaxTrafficPercentage
	}
}

// Validate validates the config.
func (c *ChangefeedSchedulerConfig) ValidateAndAdjust(sinkURI *url.URL) error {
	if !util.GetOrZero(c.EnableTableAcrossNodes) {
		return nil
	}
	if util.GetOrZero(c.RegionThreshold) < 0 {
		return errors.New("region-threshold must be larger than 0")
	}
	if util.GetOrZero(c.WriteKeyThreshold) < 0 {
		return errors.New("write-key-threshold must be larger than 0")
	}

	// Validate and adjust WriteKeyThreshold if it's too small
	if util.GetOrZero(c.WriteKeyThreshold) > 0 && util.GetOrZero(c.WriteKeyThreshold) < MinWriteKeyThreshold {
		log.Warn("WriteKeyThreshold is set too small, adjusting to minimum recommended value",
			zap.Int("configuredValue", util.GetOrZero(c.WriteKeyThreshold)),
			zap.Int("adjustedValue", MinWriteKeyThreshold),
			zap.String("reason", "small values may cause performance issues and frequent table splitting"))
		c.WriteKeyThreshold = util.AddressOf(MinWriteKeyThreshold)
	}
	if util.GetOrZero(c.SchedulingTaskCountPerNode) < 0 {
		return errors.New("scheduling-task-count-per-node must be larger than 0")
	}
	if util.GetOrZero(c.RegionCountPerSpan) <= 0 {
		return errors.New("region-count-per-span must be larger than 0")
	}
	if c.RegionCountRefreshInterval == nil || *c.RegionCountRefreshInterval <= 0 {
		return errors.New("region-count-refresh-interval must be larger than 0")
	}

	if util.GetOrZero(c.BalanceScoreThreshold) <= 0 {
		return errors.New("balance-score-threshold must be larger than 0")
	}

	if util.GetOrZero(c.MinTrafficPercentage) <= 0 || util.GetOrZero(c.MinTrafficPercentage) >= 1 {
		return errors.New("min-traffic-percentage must be between 0 and 1")
	}

	if util.GetOrZero(c.MaxTrafficPercentage) <= 1 {
		return errors.New("max-traffic-percentage must be greater than 1")
	}

	forceSplit := util.GetOrZero(c.ForceSplit)
	if IsMySQLCompatibleScheme(sinkURI.Scheme) && !forceSplit {
		c.EnableSplittableCheck = util.AddressOf(true)
	} else if forceSplit {
		c.EnableSplittableCheck = util.AddressOf(false)
	}
	return nil
}

// SchedulerConfig configs TiCDC scheduler.
type SchedulerConfig struct {
	// HeartbeatTick is the number of owner tick to initial a heartbeat to captures.
	HeartbeatTick int `toml:"heartbeat-tick" json:"heartbeat-tick"`
	// CollectStatsTick is the number of owner tick to collect stats.
	CollectStatsTick int `toml:"collect-stats-tick" json:"collect-stats-tick"`
	// MaxTaskConcurrency the maximum of concurrent running schedule tasks.
	MaxTaskConcurrency int `toml:"max-task-concurrency" json:"max-task-concurrency"`
	// CheckBalanceInterval the interval of balance tables between each capture.
	CheckBalanceInterval TomlDuration `toml:"check-balance-interval" json:"check-balance-interval"`
	// AddTableBatchSize is the batch size of adding tables on each tick,
	// used by the `BasicScheduler`.
	// When the new owner in power, other captures may not online yet, there might have hundreds of
	// tables need to be dispatched, add tables in a batch way to prevent suddenly resource usage
	// spikes, also wait for other captures join the cluster
	// When there are only 2 captures, and a large number of tables, this can be helpful to prevent
	// oom caused by all tables dispatched to only one capture.
	AddTableBatchSize int `toml:"add-table-batch-size" json:"add-table-batch-size"`

	// ChangefeedSettings is setting by changefeed.
	ChangefeedSettings *ChangefeedSchedulerConfig `toml:"-" json:"-"`
}

// NewDefaultSchedulerConfig return the default scheduler configuration.
func NewDefaultSchedulerConfig() *SchedulerConfig {
	return &SchedulerConfig{
		HeartbeatTick: 2,
		// By default, owner ticks every 50ms, we want to low the frequency of
		// collecting stats to reduce memory allocation and CPU usage.
		CollectStatsTick:     200, // 200 * 50ms = 10s.
		MaxTaskConcurrency:   10,
		CheckBalanceInterval: TomlDuration(15 * time.Second),
		AddTableBatchSize:    10000,
	}
}

// ValidateAndAdjust verifies that each parameter is valid.
func (c *SchedulerConfig) ValidateAndAdjust() error {
	if c.HeartbeatTick <= 0 {
		return cerror.ErrInvalidServerOption.GenWithStackByArgs(
			"heartbeat-tick must be larger than 0")
	}
	if c.CollectStatsTick <= 0 {
		return cerror.ErrInvalidServerOption.GenWithStackByArgs(
			"collect-stats-tick must be larger than 0")
	}
	if c.MaxTaskConcurrency <= 0 {
		return cerror.ErrInvalidServerOption.GenWithStackByArgs(
			"max-task-concurrency must be larger than 0")
	}
	if time.Duration(c.CheckBalanceInterval) <= time.Second {
		return cerror.ErrInvalidServerOption.GenWithStackByArgs(
			"check-balance-interval must be larger than 1s")
	}
	if c.AddTableBatchSize <= 0 {
		return cerror.ErrInvalidServerOption.GenWithStackByArgs(
			"add-table-batch-size must be large than 0")
	}
	return nil
}
