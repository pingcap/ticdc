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

package metering

import (
	"strings"

	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/ticdc/pkg/errors"
)

// ValidateConfig validates the shared pool identifier used in report paths.
// Storage configuration and credentials are interpreted by the SDK.
func ValidateConfig(c *config.MeteringConfig) error {
	if c == nil || c.Type == "" {
		return nil
	}
	if c.SharedPoolID != "" && !validPathComponent(c.SharedPoolID) {
		return errors.ErrInvalidServerOption.GenWithStack("metering: shared-pool-id must be a single path component")
	}
	return nil
}

func validPathComponent(s string) bool {
	return s != "" && s != "." && s != ".." && !strings.ContainsAny(s, "/\\\x00\r\n")
}
