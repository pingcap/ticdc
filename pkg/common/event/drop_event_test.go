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

package event

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestDropEvent(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewDropEvent(did, 123, 100, 456)
	data, err := e.Marshal()
	require.NoError(t, err)
	require.Len(t, data, int(e.GetSize())+int(GetEventHeaderSize()))

	var e2 DropEvent
	err = e2.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, e.Version, e2.Version)
	require.Equal(t, e.DispatcherID, e2.DispatcherID)
	require.Equal(t, e.DroppedSeq, e2.DroppedSeq)
	require.Equal(t, e.DroppedCommitTs, e2.DroppedCommitTs)
}

func TestDropEventMethods(t *testing.T) {
	did := common.NewDispatcherID()
	e := NewDropEvent(did, 123, 100, 456)

	// Test GetType
	require.Equal(t, TypeDropEvent, e.GetType())

	// Test GetSeq
	require.Equal(t, uint64(123), e.GetSeq())

	// Test GetDispatcherID
	require.Equal(t, did, e.GetDispatcherID())

	// Test GetCommitTs
	require.Equal(t, common.Ts(456), e.GetCommitTs())

	// Test GetStartTs
	require.Equal(t, common.Ts(0), e.GetStartTs())

	// Test IsPaused
	require.False(t, e.IsPaused())

	// Test Len
	require.Equal(t, int32(0), e.Len())
}

func TestDropEventMarshalUnmarshal(t *testing.T) {
	testCases := []struct {
		name      string
		event     *DropEvent
		wantError bool
	}{
		{
			name: "normal case",
			event: NewDropEvent(
				common.NewDispatcherID(),
				123,
				100,
				456,
			),
			wantError: false,
		},
		{
			name: "zero values",
			event: &DropEvent{
				Version:         DropEventVersion1,
				DispatcherID:    common.DispatcherID{},
				DroppedSeq:      0,
				DroppedCommitTs: 0,
			},
			wantError: false,
		},
		{
			name: "invalid version",
			event: &DropEvent{
				Version:         99,
				DispatcherID:    common.NewDispatcherID(),
				DroppedSeq:      0,
				DroppedCommitTs: 0,
				DroppedEpoch:    0,
			},
			wantError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test Marshal
			data, err := tc.event.Marshal()
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, data, int(tc.event.GetSize())+int(GetEventHeaderSize()))

			// Test Unmarshal
			var e2 DropEvent
			err = e2.Unmarshal(data)
			require.NoError(t, err)
			require.Equal(t, tc.event.Version, e2.Version)
			require.Equal(t, tc.event.DispatcherID, e2.DispatcherID)
			require.Equal(t, tc.event.DroppedSeq, e2.DroppedSeq)
			require.Equal(t, tc.event.DroppedCommitTs, e2.DroppedCommitTs)
		})
	}
}

func TestDropEventInvalidData(t *testing.T) {
	testCases := []struct {
		name      string
		data      []byte
		wantError bool
	}{
		{
			name:      "empty data",
			data:      []byte{},
			wantError: true,
		},
		{
			name:      "nil data",
			data:      nil,
			wantError: true,
		},
		{
			name:      "incomplete data",
			data:      []byte{0}, // Only version byte
			wantError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var e DropEvent
			err := e.Unmarshal(tc.data)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
