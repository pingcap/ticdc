// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package dispatcherorchestrator

import (
	"context"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/dispatchermanager"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
)

func (m *DispatcherOrchestrator) snapshotManager(id common.ChangeFeedID) (*dispatchermanager.DispatcherManager, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	e := m.dispatcherManagers[id]
	if e == nil || m.fenced.Load() || m.closed.Load() {
		return nil, protocol.Invalid("capture is unavailable")
	}
	return e, nil
}

func (m *DispatcherOrchestrator) ApplySnapshot(ctx context.Context, id common.ChangeFeedID, epoch uint64, spec protocol.Spec, ref protocol.ObjectRef, ex protocol.Export) (uint64, error) {
	e, err := m.snapshotManager(id)
	if err != nil {
		return 0, err
	}
	return e.ApplySnapshot(ctx, epoch, spec, ref, ex)
}

func (m *DispatcherOrchestrator) DrainSnapshot(ctx context.Context, id common.ChangeFeedID, epoch uint64) error {
	e, err := m.snapshotManager(id)
	if err != nil {
		return err
	}
	return e.DrainSnapshot(ctx, epoch)
}

// Abort waits for closure before releasing the single-host writer lock.
func (m *DispatcherOrchestrator) AbortSnapshot(id common.ChangeFeedID) {
	m.mutex.Lock()
	e := m.dispatcherManagers[id]
	m.mutex.Unlock()
	if e == nil {
		return
	}
	e.LocalFence()
	for !e.TryClose(false) {
		time.Sleep(50 * time.Millisecond)
	}
}
