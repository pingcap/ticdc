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

package eventstore

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestDiskSpaceUsageClampsNegativeInProgressBytes(t *testing.T) {
	m := &pebble.Metrics{}
	m.WAL.PhysicalSize = 100
	m.Table.ObsoleteSize = 50
	m.Table.ZombieSize = 200
	m.Compact.InProgressBytes = -1024

	require.Equal(t, uint64(350), diskSpaceUsage(m))
}
