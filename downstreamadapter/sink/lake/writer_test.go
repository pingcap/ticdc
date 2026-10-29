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

package lake

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestTableWriterAppliesOnlyAfterAllRawAndMerge(t *testing.T) {
	var statements []string
	execute := func(_ context.Context, statement string, args []any) (mutationResult, error) {
		statements = append(statements, statement)
		if strings.HasPrefix(statement, "INSERT") {
			require.Len(t, args, 6)
		}
		return mutationResult{state: committed}, nil
	}
	w, err := newTableWriter(testSchema(), execute, 1, 3, 0)
	require.NoError(t, err)
	result, err := w.applyBatch(context.Background(), []normalizedRow{
		testRow(t, deleteAction, 100, 1), testRow(t, insertAction, 100, 2),
	}, time.Now())
	require.NoError(t, err)
	require.True(t, result.applied)
	require.Len(t, statements, 4)
	require.True(t, strings.HasPrefix(statements[0], "INSERT"))
	require.True(t, strings.HasPrefix(statements[1], "INSERT"))
	require.True(t, strings.HasPrefix(statements[2], "MERGE"))
	require.True(t, strings.HasPrefix(statements[3], "DELETE"))
	require.EqualValues(t, 2, w.written)
	require.Equal(t, w.written, w.merged)
}

func TestTableWriterStopsUnknownBeforeInnerRetry(t *testing.T) {
	for _, failAt := range []int{1, 2, 3} {
		t.Run([]string{"", "first raw", "second raw", "merge"}[failAt], func(t *testing.T) {
			calls := 0
			execute := func(context.Context, string, []any) (mutationResult, error) {
				calls++
				if calls == failAt {
					return mutationResult{state: commitUnknown, retryable: true}, context.DeadlineExceeded
				}
				return mutationResult{state: committed}, nil
			}
			w, err := newTableWriter(testSchema(), execute, 1, 10, 0)
			require.NoError(t, err)
			rows := []normalizedRow{testRow(t, insertAction, 100, 1), testRow(t, updateAction, 120, 2)}
			result, err := w.applyBatch(context.Background(), rows, time.Now())
			require.Error(t, err)
			code, ok := errors.RFCCode(err)
			require.True(t, ok)
			require.Equal(t, errors.ErrLakeCommitUnknown.RFCCode(), code)
			require.False(t, result.applied)
			require.Equal(t, failAt, calls, "unknown must not reach an inner retry, MERGE, or compaction")
			require.Zero(t, w.merged)
			_, err = w.applyBatch(context.Background(), rows, time.Now())
			require.Error(t, err)
			require.Equal(t, failAt, calls, "a stopped lane cannot take a new batch")
		})
	}
}

func TestTableWriterRetriesOnlyConfirmedNotCommitted(t *testing.T) {
	calls := 0
	execute := func(context.Context, string, []any) (mutationResult, error) {
		calls++
		if calls == 1 {
			return mutationResult{state: notCommitted, retryable: true}, errors.ErrLakeInvalidData.GenWithStackByArgs("confirmed rollback")
		}
		return mutationResult{state: committed, observationErr: errors.ErrLakeInvalidData.GenWithStackByArgs("statistics unavailable")}, nil
	}
	w, err := newTableWriter(testSchema(), execute, 1, 2, 0)
	require.NoError(t, err)
	result, err := w.applyBatch(context.Background(), []normalizedRow{testRow(t, insertAction, 100, 1)}, time.Now())
	require.NoError(t, err)
	require.True(t, result.applied)
	require.Equal(t, 4, calls, "one proven rollback retry, then raw/MERGE/compaction; no statistics retry")

	calls = 0
	w, err = newTableWriter(testSchema(), func(context.Context, string, []any) (mutationResult, error) {
		calls++
		return mutationResult{state: notCommitted, retryable: true}, errors.ErrLakeInvalidData.GenWithStackByArgs("confirmed rollback")
	}, 1, 2, 0)
	require.NoError(t, err)
	result, err = w.applyBatch(context.Background(), []normalizedRow{testRow(t, insertAction, 100, 1)}, time.Now())
	require.Error(t, err)
	require.False(t, result.applied)
	require.Equal(t, 2, calls)
}

func TestTableWriterDoesNotRevokeAppliedOnCompactionFailure(t *testing.T) {
	calls := 0
	execute := func(context.Context, string, []any) (mutationResult, error) {
		calls++
		if calls == 3 {
			return mutationResult{state: notCommitted}, errors.ErrLakeInvalidData.GenWithStackByArgs("compaction denied")
		}
		return mutationResult{state: committed}, nil
	}
	w, err := newTableWriter(testSchema(), execute, 10, 1, 0)
	require.NoError(t, err)
	result, err := w.applyBatch(context.Background(), []normalizedRow{testRow(t, insertAction, 100, 1)}, time.Now())
	require.Error(t, err)
	require.True(t, result.applied, "MERGE committed; maintenance does not roll it back")
	require.Equal(t, w.written, w.merged)
	_, err = w.applyBatch(context.Background(), []normalizedRow{testRow(t, insertAction, 120, 2)}, time.Now())
	require.Error(t, err)
	require.Equal(t, 3, calls)
}

func TestTableWriterRejectsMissingOrContradictoryCommitEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result mutationResult
		err    error
	}{
		{name: "missing terminal evidence"},
		{name: "contradictory evidence", result: mutationResult{state: committed}, err: errors.ErrLakeInvalidData.GenWithStackByArgs("execution failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			w, err := newTableWriter(testSchema(), func(context.Context, string, []any) (mutationResult, error) {
				calls++
				return tc.result, tc.err
			}, 10, 3, 0)
			require.NoError(t, err)
			result, err := w.applyBatch(context.Background(), []normalizedRow{testRow(t, insertAction, 100, 1)}, time.Now())
			require.Error(t, err)
			code, ok := errors.RFCCode(err)
			require.True(t, ok)
			require.Equal(t, errors.ErrLakeCommitUnknown.RFCCode(), code)
			require.False(t, result.applied)
			require.Equal(t, 1, calls)
		})
	}
}

func TestTableWriterAppliedEpochAndValidation(t *testing.T) {
	calls := 0
	w, err := newTableWriter(testSchema(), func(context.Context, string, []any) (mutationResult, error) {
		calls++
		return mutationResult{state: committed}, nil
	}, 10, 1, 100)
	require.NoError(t, err)
	result, err := w.applyBatch(context.Background(), []normalizedRow{testRow(t, insertAction, 100, 1)}, time.Now())
	require.NoError(t, err)
	require.True(t, result.applied)
	require.Zero(t, calls)
	_, err = w.applyBatch(context.Background(), []normalizedRow{{id: "1", payload: "not-json", commitTs: 120, action: insertAction}}, time.Now())
	require.Error(t, err)
	require.Zero(t, calls)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = w.applyBatch(ctx, []normalizedRow{testRow(t, insertAction, 120, 1)}, time.Now())
	require.Error(t, err)
	require.False(t, result.applied)
	require.Zero(t, calls)
}
