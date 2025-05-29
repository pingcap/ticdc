// Copyright 2025 PingCAP, Inc.
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

package mysql

import (
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

// TestShouldGenBatchSQL tests the shouldGenBatchSQL function
func TestShouldGenBatchSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		hasPK          bool
		hasVirtualCols bool
		events         []*commonEvent.DMLEvent
		config         *Config
		safemode       bool
		want           bool
	}{
		{
			name:           "table without primary key should not use batch SQL",
			hasPK:          false,
			hasVirtualCols: false,
			events:         []*commonEvent.DMLEvent{newDMLEvent(t, 1, 1, 1)},
			config:         &Config{SafeMode: false, BatchDMLEnable: true},
			want:           false,
		},
		{
			name:           "table with virtual columns should not use batch SQL",
			hasPK:          true,
			hasVirtualCols: true,
			events:         []*commonEvent.DMLEvent{newDMLEvent(t, 1, 1, 1)},
			config:         &Config{SafeMode: false, BatchDMLEnable: true},
			want:           false,
		},
		{
			name:           "single row event should not use batch SQL",
			hasPK:          true,
			hasVirtualCols: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 1, 1, 1),
			},
			config: &Config{SafeMode: false, BatchDMLEnable: true},
			want:   false,
		},
		{
			name:           "all rows in same safe mode should use batch SQL",
			hasPK:          true,
			hasVirtualCols: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 1, 2, 2),
				newDMLEvent(t, 2, 3, 2),
			},
			config: &Config{SafeMode: false, BatchDMLEnable: true},
			want:   true,
		},
		{
			name:           "multiple rows with primary key in unsafe mode should use batch SQL",
			hasPK:          true,
			hasVirtualCols: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 2),
				newDMLEvent(t, 3, 1, 2),
			},
			config: &Config{SafeMode: false, BatchDMLEnable: true},
			want:   true,
		},
		{
			name:           "global safe mode should use batch SQL",
			hasPK:          true,
			hasVirtualCols: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 2),
			},
			config: &Config{SafeMode: true, BatchDMLEnable: true},
			want:   true,
		},
		{
			name:           "multiple rows with primary key in different safe mode should not use batch SQL",
			hasPK:          true,
			hasVirtualCols: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 2),
				newDMLEvent(t, 1, 2, 2),
			},
			config: &Config{SafeMode: false, BatchDMLEnable: true},
			want:   false,
		},
		{
			name:           "batch dml is disabled",
			hasPK:          true,
			hasVirtualCols: false,
			events:         []*commonEvent.DMLEvent{newDMLEvent(t, 1, 1, 1)},
			config:         &Config{SafeMode: false, BatchDMLEnable: false},
			want:           false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := shouldGenBatchSQL(tt.hasPK, tt.hasVirtualCols, tt.events, tt.config)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestAllRowInSameSafeMode tests the allRowInSameSafeMode function which determines
// if all rows in a batch of DML events have the same safe mode status
func TestAllRowInSameSafeMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		safemode bool
		events   []*commonEvent.DMLEvent
		want     bool
	}{
		{
			name:     "global safe mode enabled",
			safemode: true,
			events:   []*commonEvent.DMLEvent{newDMLEvent(t, 2, 1, 1)},
			want:     true,
		},
		{
			name:     "all events have same safe mode status (all CommitTs > ReplicatingTs)",
			safemode: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 1),
				newDMLEvent(t, 3, 2, 1),
			},
			want: true,
		},
		{
			name:     "all events have same safe mode status (all CommitTs <= ReplicatingTs)",
			safemode: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 1, 1, 1),
				newDMLEvent(t, 1, 2, 1),
			},
			want: true,
		},
		{
			name:     "events have mixed safe mode status",
			safemode: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 1), // CommitTs > ReplicatingTs
				newDMLEvent(t, 1, 2, 1), // CommitTs < ReplicatingTs
			},
			want: false,
		},
		{
			name:     "events have mixed safe mode status (equal case)",
			safemode: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 1), // CommitTs > ReplicatingTs
				newDMLEvent(t, 2, 2, 1), // CommitTs = ReplicatingTs
			},
			want: false,
		},
		{
			name:     "empty events array",
			safemode: false,
			events:   []*commonEvent.DMLEvent{},
			want:     false,
		},
		{
			name:     "single event",
			safemode: false,
			events:   []*commonEvent.DMLEvent{newDMLEvent(t, 1, 1, 1)},
			want:     true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := allRowInSameSafeMode(tt.safemode, tt.events)
			require.Equal(t, tt.want, got)
		})
	}
}

// newDMLEvent creates a mock DMLEvent for testing
func newDMLEvent(_ *testing.T, commitTs, replicatingTs, rowCount uint64) *commonEvent.DMLEvent {
	return &commonEvent.DMLEvent{
		CommitTs:      commitTs,
		ReplicatingTs: replicatingTs,
		Length:        int32(rowCount),
	}
}
