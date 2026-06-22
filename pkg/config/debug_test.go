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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPullerMemoryQuotaConfig(t *testing.T) {
	config := GetDefaultServerConfig()
	require.Equal(t, uint64(1024*1024*1024), config.Debug.Puller.MemoryQuota)
	require.NoError(t, config.Debug.ValidateAndAdjust())

	config.Debug.Puller.MemoryQuota = 0
	require.Error(t, config.Debug.ValidateAndAdjust())
}
