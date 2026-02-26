// Copyright 2026 PingCAP, Inc.
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

package cloudstorage

import (
	"bytes"
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestMutateMessageValueForFailpointRecordClassification(t *testing.T) {
	t.Parallel()

	msg := &common.Message{
		Value: []byte(
			`{"pkNames":["id"],"data":[{"id":"1","c2":"v1"}]}` +
				"\r\n" +
				`{"pkNames":["id"],"data":[{"id":"2","_tidb_origin_ts":"100"}]}`,
		),
	}
	rowRecords := []RowRecord{
		{
			CommitTs:    101,
			PrimaryKeys: map[string]any{"id": "1"},
		},
		{
			CommitTs:    102,
			PrimaryKeys: map[string]any{"id": "2"},
		},
	}

	mutatedRows, originTsMutatedRows := mutateMessageValueForFailpoint(msg, rowRecords)

	require.Len(t, mutatedRows, 1)
	require.Equal(t, uint64(101), mutatedRows[0].CommitTs)
	require.Equal(t, "1", mutatedRows[0].PrimaryKeys["id"])

	require.Len(t, originTsMutatedRows, 1)
	require.Equal(t, uint64(102), originTsMutatedRows[0].CommitTs)
	require.Equal(t, "2", originTsMutatedRows[0].PrimaryKeys["id"])

	require.True(t, bytes.Contains(msg.Value, []byte(`"_tidb_origin_ts":"101"`)))
	require.True(t, bytes.Contains(msg.Value, []byte(`"c2":null`)))
}

func TestSelectColumnToMutateSkipNilOriginTsWhenPossible(t *testing.T) {
	t.Parallel()

	row := map[string]any{
		"id":                       "1",
		commonEvent.OriginTsColumn: nil,
		"c2":                       "v1",
	}
	pkSet := map[string]struct{}{
		"id": {},
	}

	for i := 0; i < 20; i++ {
		col, ok := selectColumnToMutate(row, pkSet)
		require.True(t, ok)
		require.Equal(t, "c2", col)
	}
}

func TestSelectColumnToMutatePreferNonNilOriginTs(t *testing.T) {
	t.Parallel()

	row := map[string]any{
		"id":                       "1",
		commonEvent.OriginTsColumn: "100",
		"c2":                       "v1",
	}
	pkSet := map[string]struct{}{
		"id": {},
	}

	for i := 0; i < 20; i++ {
		col, ok := selectColumnToMutate(row, pkSet)
		require.True(t, ok)
		require.Equal(t, commonEvent.OriginTsColumn, col)
	}
}

func TestSelectColumnToMutateSkipNilNonPKColumns(t *testing.T) {
	t.Parallel()

	row := map[string]any{
		"id": "1",
		"c1": nil,
		"c2": "v2",
	}
	pkSet := map[string]struct{}{
		"id": {},
	}

	for i := 0; i < 20; i++ {
		col, ok := selectColumnToMutate(row, pkSet)
		require.True(t, ok)
		require.Equal(t, "c2", col)
	}
}

func TestSelectColumnToMutateNoCandidateWhenAllNonPKColumnsNil(t *testing.T) {
	t.Parallel()

	row := map[string]any{
		"id":                       "1",
		commonEvent.OriginTsColumn: nil,
		"c1":                       nil,
	}
	pkSet := map[string]struct{}{
		"id": {},
	}

	col, ok := selectColumnToMutate(row, pkSet)
	require.False(t, ok)
	require.Empty(t, col)
}
