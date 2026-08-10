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
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultOldStartTsScanLowPriorityThreshold(t *testing.T) {
	t.Parallel()

	expected := TomlDuration(10 * time.Minute)
	pullerConfig := NewDefaultPullerConfig()
	require.Equal(t, expected, pullerConfig.OldStartTsScanLowPriorityThreshold)

	pullerConfig.OldStartTsScanLowPriorityThreshold = 0
	pullerConfig.ValidateAndAdjust()
	require.Equal(t, expected, pullerConfig.OldStartTsScanLowPriorityThreshold)
}
