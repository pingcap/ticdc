// Copyright 2021 PingCAP, Inc.
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

package integrity

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

// Config represents integrity check config for a changefeed.
type Config struct {
	IntegrityCheckLevel   *string `toml:"integrity-check-level" json:"integrity-check-level,omitempty"`
	CorruptionHandleLevel *string `toml:"corruption-handle-level" json:"corruption-handle-level,omitempty"`
}

const (
	// CheckLevelNone means no integrity check, the default value.
	CheckLevelNone string = "none"
	// CheckLevelCorrectness means check each row data correctness.
	CheckLevelCorrectness string = "correctness"
)

const (
	// CorruptionHandleLevelWarn is the default value,
	// log the corrupted event, and mark it as corrupted and send it to the downstream.
	CorruptionHandleLevelWarn string = "warn"
	// CorruptionHandleLevelError means log the corrupted event, and then stopped the changefeed.
	CorruptionHandleLevelError string = "error"
)

// Validate the integrity config.
func (c *Config) Validate() error {
	if util.GetOrZero(c.IntegrityCheckLevel) != CheckLevelNone &&
		util.GetOrZero(c.IntegrityCheckLevel) != CheckLevelCorrectness {
		return errors.ErrInvalidReplicaConfig.GenWithStackByArgs()
	}
	if util.GetOrZero(c.CorruptionHandleLevel) != CorruptionHandleLevelWarn &&
		util.GetOrZero(c.CorruptionHandleLevel) != CorruptionHandleLevelError {
		return errors.ErrInvalidReplicaConfig.GenWithStackByArgs()
	}

	if c.Enabled() {
		log.Info("integrity check is enabled",
			zap.Any("integrityCheckLevel", c.IntegrityCheckLevel),
			zap.Any("corruptionHandleLevel", c.CorruptionHandleLevel))
	}

	return nil
}

// Enabled returns true if the integrity check is enabled.
func (c *Config) Enabled() bool {
	return util.GetOrZero(c.IntegrityCheckLevel) == CheckLevelCorrectness
}

// ErrorHandle returns true if the corruption handle level is error.
func (c *Config) ErrorHandle() bool {
	return util.GetOrZero(c.CorruptionHandleLevel) == CorruptionHandleLevelError
}
