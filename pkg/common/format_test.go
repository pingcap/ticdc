// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package common

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
)

// All test cases in this file are to make sure the format functions will not panic.

func TestFormatBlockStatusRequest(t *testing.T) {
	t.Parallel()

	cfID := NewChangefeedID4Test("test-keyspace", "test-changefeed")
	dispatcherID := NewDispatcherID()

	testCases := []struct {
		name          string
		input         *heartbeatpb.BlockStatusRequest
		isEmptyString bool
	}{
		{
			name:          "nil request",
			input:         nil,
			isEmptyString: true,
		},
		{
			name: "normal request",
			input: &heartbeatpb.BlockStatusRequest{
				ChangefeedID: cfID.ToPB(),
				BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
					{
						ID: dispatcherID.ToPB(),
						State: &heartbeatpb.State{
							Stage: heartbeatpb.BlockStage_WAITING,
						},
					},
				},
			},
			isEmptyString: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := FormatBlockStatusRequest(tc.input)
			require.Equal(t, tc.isEmptyString, result == "")
		})
	}
}

func TestFormatTableSpanBlockStatus(t *testing.T) {
	t.Parallel()

	dispatcherID := NewDispatcherID()

	testCases := []struct {
		name          string
		input         *heartbeatpb.TableSpanBlockStatus
		isEmptyString bool
	}{
		{
			name:          "nil status",
			input:         nil,
			isEmptyString: true,
		},
		{
			name: "normal status",
			input: &heartbeatpb.TableSpanBlockStatus{
				ID: dispatcherID.ToPB(),
			},
			isEmptyString: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := FormatTableSpanBlockStatus(tc.input)
			require.Equal(t, tc.isEmptyString, result == "")
		})
	}
}

func TestFormatDispatcherStatus(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		input         *heartbeatpb.DispatcherStatus
		isEmptyString bool
	}{
		{
			name:          "nil status",
			input:         nil,
			isEmptyString: true,
		},
		{
			name: "normal status",
			input: &heartbeatpb.DispatcherStatus{
				Action: &heartbeatpb.DispatcherAction{
					Action: heartbeatpb.Action_Write,
				},
			},
			isEmptyString: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := FormatDispatcherStatus(tc.input)
			require.Equal(t, tc.isEmptyString, result == "")
		})
	}
}

func TestFormatInfluencedDispatchers(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		input         *heartbeatpb.InfluencedDispatchers
		isEmptyString bool
	}{
		{
			name:          "nil dispatchers",
			input:         nil,
			isEmptyString: true,
		},
		{
			name: "normal dispatchers",
			input: &heartbeatpb.InfluencedDispatchers{
				SchemaID:      1,
				InfluenceType: heartbeatpb.InfluenceType_All,
			},
			isEmptyString: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := FormatInfluencedDispatchers(tc.input)
			require.Equal(t, tc.isEmptyString, result == "")
		})
	}
}

func TestFormatTableSpan(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		input         *heartbeatpb.TableSpan
		isEmptyString bool
	}{
		{
			name:          "nil span",
			input:         nil,
			isEmptyString: true,
		},
		{
			name: "normal span",
			input: &heartbeatpb.TableSpan{
				TableID:  1,
				StartKey: []byte("start"),
				EndKey:   []byte("end"),
			},
			isEmptyString: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := FormatTableSpan(tc.input)
			require.Equal(t, tc.isEmptyString, result == "")
		})
	}
}

func TestFormatMaintainerStatus(t *testing.T) {
	t.Parallel()

	cfID := NewChangefeedID4Test("test-keyspace", "test-changefeed")
	testCases := []struct {
		name          string
		input         *heartbeatpb.MaintainerStatus
		isEmptyString bool
	}{
		{
			name:          "nil status",
			input:         nil,
			isEmptyString: true,
		},
		{
			name: "normal status",
			input: &heartbeatpb.MaintainerStatus{
				ChangefeedID: cfID.ToPB(),
				FeedState:    "Normal",
				State:        heartbeatpb.ComponentState_Working,
				CheckpointTs: 100,
			},
			isEmptyString: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := FormatMaintainerStatus(tc.input)
			require.Equal(t, tc.isEmptyString, result == "")
		})
	}
}
