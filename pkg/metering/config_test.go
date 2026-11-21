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
	"testing"

	"github.com/pingcap/metering_sdk/config"
	"github.com/stretchr/testify/require"
)

func TestValidateConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *config.MeteringConfig
		ok   bool
	}{
		{"disabled nil", nil, true},
		{"disabled empty", &config.MeteringConfig{}, true},
		{"SDK owns provider configuration", &config.MeteringConfig{Type: "s3"}, true},
		{"pool", &config.MeteringConfig{Type: "s3", SharedPoolID: "pool1"}, true},
		{"pool traversal", &config.MeteringConfig{Type: "s3", SharedPoolID: "../pool"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateConfig(tc.cfg)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
