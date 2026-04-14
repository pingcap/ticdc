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

package pdutil

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

type mockStrictSessionPDClient struct {
	pd.Client
	physical int64
	logical  int64
	err      error
}

func (m *mockStrictSessionPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	return m.physical, m.logical, m.err
}

func TestGenerateStrictSessionEpochUsesPDTSO(t *testing.T) {
	t.Parallel()

	pdClient := &mockStrictSessionPDClient{
		physical: 100,
		logical:  2,
	}

	sessionEpoch, err := GenerateStrictSessionEpoch(context.Background(), pdClient, 0)
	require.NoError(t, err)
	require.Equal(t, oracle.ComposeTS(100, 2), sessionEpoch)
}

func TestGenerateStrictSessionEpochRemainsMonotonic(t *testing.T) {
	t.Parallel()

	pdClient := &mockStrictSessionPDClient{
		physical: 100,
		logical:  1,
	}

	sessionEpoch, err := GenerateStrictSessionEpoch(context.Background(), pdClient, oracle.ComposeTS(100, 5))
	require.NoError(t, err)
	require.Equal(t, oracle.ComposeTS(100, 5)+1, sessionEpoch)
}

func TestGenerateStrictSessionEpochReturnsErrorWithoutPDTSO(t *testing.T) {
	t.Parallel()

	_, err := GenerateStrictSessionEpoch(context.Background(), &mockStrictSessionPDClient{
		err: errors.New("pd unavailable"),
	}, 0)
	require.Error(t, err)

	_, err = GenerateStrictSessionEpoch(context.Background(), nil, 0)
	require.Error(t, err)
}
