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

package gc

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

func TestCheckSafetyOfStartTs(t *testing.T) {
	t.Parallel()

	pdCli := &mockPdClientForServiceGCSafePoint{
		serviceSafePoint: make(map[string]uint64),
		gcBarriers:       make(map[string]uint64),
		txnSafePoint:     60,
	}

	ctx := context.Background()

	TTL := int64(1)

	if kerneltype.IsClassic() {
		// assume no pd leader switch
		pdCli.UpdateServiceGCSafePoint(ctx, "service1", 10, 60) //nolint:errcheck
		err := EnsureChangefeedStartTsSafety(ctx, pdCli,
			"ticdc-creating-",
			0,
			common.NewChangeFeedIDWithName("changefeed1", "default"), TTL, 50)
		require.Equal(t,
			"[CDC:ErrStartTsBeforeGC]fail to create or maintain changefeed "+
				"because start-ts 50 is earlier than or equal to GC safepoint at 60", err.Error())
		pdCli.UpdateServiceGCSafePoint(ctx, "service2", 10, 80) //nolint:errcheck
		pdCli.UpdateServiceGCSafePoint(ctx, "service3", 10, 70) //nolint:errcheck
		err = EnsureChangefeedStartTsSafety(ctx, pdCli,
			"ticdc-creating-",
			0,
			common.NewChangeFeedIDWithName("changefeed2", "default"), TTL, 65)
		require.Nil(t, err)
		require.Equal(t, pdCli.serviceSafePoint, map[string]uint64{
			"service1":                           60,
			"service2":                           80,
			"service3":                           70,
			"ticdc-creating-default_changefeed2": 65,
		})
		err = UndoEnsureChangefeedStartTsSafety(ctx, pdCli,
			0,
			"ticdc-creating-",
			common.NewChangeFeedIDWithName("changefeed2", "default"))
		require.Nil(t, err)
		require.Equal(t, pdCli.serviceSafePoint, map[string]uint64{
			"service1":                           60,
			"service2":                           80,
			"service3":                           70,
			"ticdc-creating-default_changefeed2": math.MaxUint64,
		})

		pdCli.enableLeaderSwitch = true

		pdCli.retryThreshold = 1
		pdCli.retryCount = 0
		err = EnsureChangefeedStartTsSafety(ctx, pdCli,
			"ticdc-creating-",
			0,
			common.NewChangeFeedIDWithName("changefeed2", "default"), TTL, 65)
		require.Nil(t, err)

		pdCli.retryThreshold = gcServiceMaxRetries + 1
		pdCli.retryCount = 0
		err = EnsureChangefeedStartTsSafety(ctx, pdCli,
			"ticdc-creating-",
			0,
			common.NewChangeFeedIDWithName("changefeed2", "default"), TTL, 65)
		require.NotNil(t, err)
		require.Equal(t, err.Error(),
			"[CDC:ErrReachMaxTry]reach maximum try: 9, error: not pd leader: not pd leader")

		pdCli.retryThreshold = 3
		pdCli.retryCount = 0
		err = EnsureChangefeedStartTsSafety(ctx, pdCli,
			"ticdc-creating-",
			0,
			common.NewChangeFeedIDWithName("changefeed1", "default"), TTL, 50)
		require.Equal(t, err.Error(),
			"[CDC:ErrStartTsBeforeGC]fail to create or maintain changefeed "+
				"because start-ts 50 is earlier than or equal to GC safepoint at 60")
		return
	}

	err := EnsureChangefeedStartTsSafety(ctx, pdCli,
		"ticdc-creating-",
		0,
		common.NewChangeFeedIDWithName("changefeed1", "default"), TTL, 50)
	require.True(t, cerror.ErrStartTsBeforeGC.Equal(errors.Cause(err)))

	err = EnsureChangefeedStartTsSafety(ctx, pdCli,
		"ticdc-creating-",
		0,
		common.NewChangeFeedIDWithName("changefeed2", "default"), TTL, 65)
	require.NoError(t, err)
	require.Equal(t, uint64(65), pdCli.gcBarriers["ticdc-creating-default_changefeed2"])

	err = UndoEnsureChangefeedStartTsSafety(ctx, pdCli,
		0,
		"ticdc-creating-",
		common.NewChangeFeedIDWithName("changefeed2", "default"))
	require.NoError(t, err)
	_, ok := pdCli.gcBarriers["ticdc-creating-default_changefeed2"]
	require.False(t, ok)

	pdCli.enableLeaderSwitch = true

	pdCli.retryThreshold = 1
	pdCli.retryCount = 0
	err = EnsureChangefeedStartTsSafety(ctx, pdCli,
		"ticdc-creating-",
		0,
		common.NewChangeFeedIDWithName("changefeed2", "default"), TTL, 65)
	require.NoError(t, err)

	pdCli.retryThreshold = gcServiceMaxRetries + 1
	pdCli.retryCount = 0
	err = EnsureChangefeedStartTsSafety(ctx, pdCli,
		"ticdc-creating-",
		0,
		common.NewChangeFeedIDWithName("changefeed2", "default"), TTL, 65)
	require.True(t, cerror.ErrStartTsBeforeGC.Equal(errors.Cause(err)))

	pdCli.retryThreshold = 3
	pdCli.retryCount = 0
	err = EnsureChangefeedStartTsSafety(ctx, pdCli,
		"ticdc-creating-",
		0,
		common.NewChangeFeedIDWithName("changefeed1", "default"), TTL, 50)
	require.True(t, cerror.ErrStartTsBeforeGC.Equal(errors.Cause(err)))
}

type mockPdClientForServiceGCSafePoint struct {
	pd.Client
	serviceSafePoint   map[string]uint64
	gcBarriers         map[string]uint64
	txnSafePoint       uint64
	enableLeaderSwitch bool
	retryCount         int
	retryThreshold     int
}

func (m *mockPdClientForServiceGCSafePoint) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	defer func() { m.retryCount++ }()
	minSafePoint := uint64(math.MaxUint64)
	if m.enableLeaderSwitch && m.retryCount < m.retryThreshold {
		// simulate pd leader switch error
		return minSafePoint, errors.New("not pd leader")
	}

	for _, safePoint := range m.serviceSafePoint {
		if minSafePoint > safePoint {
			minSafePoint = safePoint
		}
	}
	if safePoint < minSafePoint && len(m.serviceSafePoint) != 0 {
		return minSafePoint, nil
	}
	m.serviceSafePoint[serviceID] = safePoint
	return minSafePoint, nil
}

func (m *mockPdClientForServiceGCSafePoint) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	return &mockGCStatesClient{
		keyspaceID: keyspaceID,
		parent:     m,
	}
}

type mockGCStatesClient struct {
	keyspaceID uint32
	parent     *mockPdClientForServiceGCSafePoint
}

func (m *mockGCStatesClient) SetGCBarrier(ctx context.Context, barrierID string, barrierTS uint64, ttl time.Duration) (*pdgc.GCBarrierInfo, error) {
	defer func() { m.parent.retryCount++ }()
	if m.parent.enableLeaderSwitch && m.parent.retryCount < m.parent.retryThreshold {
		// simulate pd leader switch error
		return nil, errors.New("not pd leader")
	}
	if barrierTS < m.parent.txnSafePoint {
		return nil, errors.New("ErrGCBarrierTSBehindTxnSafePoint")
	}
	m.parent.gcBarriers[barrierID] = barrierTS
	return pdgc.NewGCBarrierInfo(barrierID, barrierTS, ttl, time.Now()), nil
}

func (m *mockGCStatesClient) DeleteGCBarrier(ctx context.Context, barrierID string) (*pdgc.GCBarrierInfo, error) {
	if m.parent.enableLeaderSwitch && m.parent.retryCount < m.parent.retryThreshold {
		// simulate pd leader switch error
		return nil, errors.New("not pd leader")
	}
	barrierTS, ok := m.parent.gcBarriers[barrierID]
	if !ok {
		return nil, nil
	}
	delete(m.parent.gcBarriers, barrierID)
	return pdgc.NewGCBarrierInfo(barrierID, barrierTS, 0, time.Now()), nil
}

func (m *mockGCStatesClient) GetGCState(ctx context.Context) (pdgc.GCState, error) {
	gcBarriers := make([]*pdgc.GCBarrierInfo, 0, len(m.parent.gcBarriers))
	for id, ts := range m.parent.gcBarriers {
		gcBarriers = append(gcBarriers, pdgc.NewGCBarrierInfo(id, ts, 0, time.Now()))
	}

	return pdgc.GCState{
		KeyspaceID:   m.keyspaceID,
		TxnSafePoint: m.parent.txnSafePoint,
		GCBarriers:   gcBarriers,
	}, nil
}
