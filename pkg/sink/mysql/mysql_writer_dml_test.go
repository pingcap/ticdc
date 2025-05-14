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
		safemode       bool
		want           bool
	}{
		{
			name:           "table without primary key should not use batch SQL",
			hasPK:          false,
			hasVirtualCols: false,
			events:         []*commonEvent.DMLEvent{newDMLEvent(t, 1, 1, 1)},
			safemode:       false,
			want:           false,
		},
		{
			name:           "table with virtual columns should not use batch SQL",
			hasPK:          true,
			hasVirtualCols: true,
			events:         []*commonEvent.DMLEvent{newDMLEvent(t, 1, 1, 1)},
			safemode:       false,
			want:           false,
		},
		{
			name:           "single row event should not use batch SQL",
			hasPK:          true,
			hasVirtualCols: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 1, 1, 1),
			},
			safemode: false,
			want:     false,
		},
		{
			name:           "safe mode should not use batch SQL",
			hasPK:          true,
			hasVirtualCols: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 1, 2, 2),
				newDMLEvent(t, 2, 3, 2),
			},
			safemode: false,
			want:     false,
		},
		{
			name:           "multiple rows with primary key in unsafe mode should use batch SQL",
			hasPK:          true,
			hasVirtualCols: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 2),
				newDMLEvent(t, 3, 1, 2),
			},
			safemode: false,
			want:     true,
		},
		{
			name:           "global safe mode should not use batch SQL",
			hasPK:          true,
			hasVirtualCols: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 2),
			},
			safemode: true,
			want:     false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := shouldGenBatchSQL(tt.hasPK, tt.hasVirtualCols, tt.events, tt.safemode)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestShouldSafeMode tests the shouldSafeMode function
func TestShouldSafeMode(t *testing.T) {
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
			name:     "all events have CommitTs > ReplicatingTs",
			safemode: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 1),
				newDMLEvent(t, 3, 2, 1),
			},
			want: false,
		},
		{
			name:     "at least one event has CommitTs < ReplicatingTs",
			safemode: false,
			events: []*commonEvent.DMLEvent{
				newDMLEvent(t, 2, 1, 1),
				newDMLEvent(t, 1, 2, 1), // This event triggers safe mode
				newDMLEvent(t, 3, 2, 1),
			},
			want: true,
		},
		{
			name:     "empty events should not trigger safe mode",
			safemode: false,
			events:   []*commonEvent.DMLEvent{},
			want:     false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := shouldSafeMode(tt.safemode, tt.events)
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
