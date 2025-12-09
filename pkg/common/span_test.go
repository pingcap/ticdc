// Copyright 2020 PingCAP, Inc.
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
	"bytes"
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
)

func TestStartCompare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lhs []byte
		rhs []byte
		res int
	}{
		{nil, nil, 0},
		{nil, []byte{}, 0},
		{[]byte{}, nil, 0},
		{[]byte{}, []byte{}, 0},
		{[]byte{1}, []byte{2}, -1},
		{[]byte{2}, []byte{1}, 1},
		{[]byte{3}, []byte{3}, 0},
	}

	for _, test := range tests {
		require.Equal(t, test.res, StartCompare(test.lhs, test.rhs))
	}
}

func TestEndCompare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lhs []byte
		rhs []byte
		res int
	}{
		{nil, nil, 0},
		{nil, []byte{}, 0},
		{[]byte{}, nil, 0},
		{[]byte{}, []byte{}, 0},
		{[]byte{1}, []byte{2}, -1},
		{[]byte{2}, []byte{1}, 1},
		{[]byte{3}, []byte{3}, 0},
	}

	for _, test := range tests {
		require.Equal(t, test.res, EndCompare(test.lhs, test.rhs))
	}
}

func TestIntersect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lhs heartbeatpb.TableSpan
		rhs heartbeatpb.TableSpan
		// Set nil for non-intersect
		res *heartbeatpb.TableSpan
	}{
		{
			lhs: *heartbeatpb.NewTableSpan(0, nil, []byte{1}, 0),
			rhs: *heartbeatpb.NewTableSpan(0, []byte{1}, nil, 0),
			res: nil,
		},
		{
			lhs: *heartbeatpb.NewTableSpan(0, nil, nil, 0),
			rhs: *heartbeatpb.NewTableSpan(0, nil, nil, 0),
			res: heartbeatpb.NewTableSpan(0, nil, nil, 0),
		},
		{
			lhs: *heartbeatpb.NewTableSpan(0, nil, nil, 0),
			rhs: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{2}, 0),
			res: heartbeatpb.NewTableSpan(0, []byte{1}, []byte{2}, 0),
		},
		{
			lhs: *heartbeatpb.NewTableSpan(0, []byte{0}, []byte{3}, 0),
			rhs: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{2}, 0),
			res: heartbeatpb.NewTableSpan(0, []byte{1}, []byte{2}, 0),
		},
		{
			lhs: *heartbeatpb.NewTableSpan(0, []byte{0}, []byte{2}, 0),
			rhs: *heartbeatpb.NewTableSpan(0, []byte{1}, []byte{2}, 0),
			res: heartbeatpb.NewTableSpan(0, []byte{1}, []byte{2}, 0),
		},
	}

	for _, test := range tests {
		t.Logf("running.., %v", test)
		res := GetIntersectSpan(test.lhs, test.rhs)
		if test.res != nil {
			require.Equal(t, *test.res, res)
		}

		// Swap lhs and rhs, should get the same result
		res2 := GetIntersectSpan(test.rhs, test.lhs)
		if test.res != nil {
			require.Equal(t, *test.res, res2)
		}
	}
}

func TestGetTableRange(t *testing.T) {
	t.Parallel()

	startKey, endKey := getTableRange(123)
	require.Equal(t, -1, bytes.Compare(startKey, endKey))
	prefix := []byte(tablecodec.GenTableRecordPrefix(123))
	require.GreaterOrEqual(t, 0, bytes.Compare(startKey, prefix))
	prefix[len(prefix)-1]++
	require.LessOrEqual(t, 0, bytes.Compare(endKey, prefix))
}
