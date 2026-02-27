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

//go:build !nextgen
// +build !nextgen

package gc

import (
	"context"
	stderrors "errors"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
)

func TestTryUpdateServiceGCSafepointReturnsErrorOnUpdateFailure(t *testing.T) {
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())

	updateCalls := 0
	pdClient := &MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			updateCalls++
			return 0, stderrors.New("pd is unstable")
		},
	}

	m := NewManager("test-service", pdClient)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := m.TryUpdateServiceGCSafepoint(ctx, common.Ts(100))
	require.Error(t, err)
	errCode, ok := cerrors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerrors.ErrUpdateServiceSafepointFailed.RFCCode(), errCode)
	require.Equal(t, 1, updateCalls)
}

func TestTryUpdateServiceGCSafepointAlwaysExecutesUpdate(t *testing.T) {
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())

	updateCalls := 0
	pdClient := &MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			updateCalls++
			return safePoint, nil
		},
	}
	m := NewManager("test-service", pdClient)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, m.TryUpdateServiceGCSafepoint(ctx, common.Ts(100)))
	require.NoError(t, m.TryUpdateServiceGCSafepoint(ctx, common.Ts(101)))
	require.Equal(t, 2, updateCalls)
}

func TestTryUpdateServiceGCSafepointDoesNotReturnSnapshotLost(t *testing.T) {
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())

	pdClient := &MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			return safePoint + 100, nil
		},
	}
	m := NewManager("test-service", pdClient)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	checkpointTs := common.Ts(100)
	require.NoError(t, m.TryUpdateServiceGCSafepoint(ctx, checkpointTs))

	cfID := common.NewChangeFeedIDWithName("test-changefeed", "test")
	err := m.CheckStaleCheckpointTs(cfID, checkpointTs)
	require.Error(t, err)
	errCode, ok := cerrors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerrors.ErrSnapshotLostByGC.RFCCode(), errCode)
}
