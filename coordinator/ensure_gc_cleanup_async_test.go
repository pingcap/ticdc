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

package coordinator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/gccleaner"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type noopGCManager struct{}

var _ gc.Manager = noopGCManager{}

func (noopGCManager) TryUpdateServiceGCSafepoint(ctx context.Context, checkpointTs common.Ts) error {
	return nil
}

func (noopGCManager) CheckStaleCheckpointTs(changefeedID common.ChangeFeedID, checkpointTs common.Ts) error {
	return nil
}

type blockingPdClient struct {
	pd.Client

	startedOnce sync.Once
	undoStarted chan struct{}
	releaseUndo chan struct{}
}

func (c *blockingPdClient) markStarted() {
	c.startedOnce.Do(func() { close(c.undoStarted) })
}

func (c *blockingPdClient) UpdateServiceGCSafePoint(
	ctx context.Context, serviceID string, ttl int64, safePoint uint64,
) (uint64, error) {
	c.markStarted()
	select {
	case <-c.releaseUndo:
		return safePoint, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func TestTryClearEnsureGCSafepointDoesNotBlockChangefeedChanges(t *testing.T) {
	undoStarted := make(chan struct{})
	releaseUndo := make(chan struct{})
	pdClient := &blockingPdClient{
		undoStarted: undoStarted,
		releaseUndo: releaseUndo,
	}

	co := &coordinator{
		controller: &Controller{
			changefeedDB: changefeed.NewChangefeedDB(1),
		},
		gcServiceID:        "test-gc-service",
		gcManager:          noopGCManager{},
		pdClient:           pdClient,
		pdClock:            pdutil.NewClock4Test(),
		changefeedChangeCh: make(chan []*changefeedChange),
		gcCleaner:          gccleaner.New(pdClient, "test-gc-service"),
		lastTickTime:       time.Now(),
		gcTickInterval:     100 * time.Millisecond,
	}

	cfID := common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName)
	co.gcCleaner.Add(cfID, 1, gc.EnsureGCServiceCreating)

	errCh := make(chan error, 2)
	ctx, cancel := context.WithCancel(context.Background())

	go func() { errCh <- co.gcCleaner.Run(ctx) }()
	go func() { errCh <- co.run(ctx) }()
	t.Cleanup(func() {
		cancel()
		close(releaseUndo)
		<-errCh
		<-errCh
	})

	select {
	case <-undoStarted:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "timeout waiting for undo ensure gc safepoint")
	}

	select {
	case co.changefeedChangeCh <- nil:
	case <-time.After(200 * time.Millisecond):
		require.FailNow(t, "changefeedChangeCh send blocked by tryClearEnsureGCSafepoint")
	}
}
