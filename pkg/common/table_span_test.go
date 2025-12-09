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

package common

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
)

func TestMergeDataRange(t *testing.T) {
	span1 := heartbeatpb.NewTableSpan(1, []byte("a"), []byte("z"), 0)
	dataRange1 := NewDataRange(1, span1, 10, 20)
	dataRange2 := NewDataRange(1, span1, 15, 25)

	expectedDataRange := NewDataRange(1, span1, 10, 25)
	// Case 1: Merge two data ranges with the same span , and intersecting timestamps.
	mergedDataRange := dataRange1.Merge(dataRange2)
	require.NotNil(t, mergedDataRange)
	require.Equal(t, expectedDataRange, mergedDataRange)

	// Case 2: Merge two data ranges with the same span, but non-intersecting timestamps.
	dataRange3 := NewDataRange(1, span1, 25, 30)
	expectedDataRange = NewDataRange(1, span1, 10, 30)
	mergedDataRange = dataRange1.Merge(dataRange3)
	require.NotNil(t, mergedDataRange)
	require.Equal(t, expectedDataRange, mergedDataRange)

	// Case 3: Merge two data ranges with different spans.
	span2 := heartbeatpb.NewTableSpan(1, []byte("b"), []byte("y"), 0)
	dataRange4 := NewDataRange(1, span2, 10, 20)
	mergedDataRange = dataRange1.Merge(dataRange4)
	require.Nil(t, mergedDataRange)
}

func TestDataRangeEqual(t *testing.T) {
	span1 := heartbeatpb.NewTableSpan(1, []byte("a"), []byte("z"), 0)
	span2 := heartbeatpb.NewTableSpan(1, []byte("a"), []byte("z"), 0)
	span3 := heartbeatpb.NewTableSpan(2, []byte("b"), []byte("y"), 0)

	dataRange1 := NewDataRange(1, span1, 10, 20)
	dataRange2 := NewDataRange(1, span2, 10, 20)
	dataRange3 := NewDataRange(1, span1, 15, 25)
	dataRange4 := NewDataRange(2, span3, 10, 20)

	require.True(t, dataRange1.Equal(dataRange2))
	require.False(t, dataRange1.Equal(dataRange3))
	require.False(t, dataRange1.Equal(dataRange4))
}

func TestTableSpanLess(t *testing.T) {
	span1 := heartbeatpb.NewTableSpan(1, []byte("a"), []byte("z"), 0)
	span2 := heartbeatpb.NewTableSpan(2, []byte("b"), []byte("y"), 0)
	span3 := heartbeatpb.NewTableSpan(2, []byte("c"), []byte("x"), 0)

	require.True(t, span1.Less(span2))
	require.False(t, span2.Less(span1))
	require.True(t, span2.Less(span3))
}

func TestTableSpanEqual(t *testing.T) {
	span1 := heartbeatpb.NewTableSpan(1, []byte("a"), []byte("z"), 0)
	span2 := heartbeatpb.NewTableSpan(1, []byte("a"), []byte("z"), 0)
	span3 := heartbeatpb.NewTableSpan(2, []byte("b"), []byte("y"), 0)

	require.True(t, span1.Equal(span2))
	require.False(t, span1.Equal(span3))
}
