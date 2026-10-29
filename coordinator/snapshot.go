// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package coordinator

import (
	"context"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/snapshot/bootstrap"
	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
)

func (c *coordinator) snapshotOwner(id common.ChangeFeedID, epoch uint64) error {
	if c.closed.Load() {
		return protocol.Invalid("coordinator stopped")
	}
	cf := c.controller.getChangefeed(id)
	if cf == nil || cf.GetNodeID() != c.nodeInfo.ID || cf.GetInfo().Epoch != epoch {
		return protocol.Invalid("demo snapshot requires local current maintainer")
	}
	return nil
}

func (c *coordinator) LoadSnapshot(ctx context.Context, id common.ChangeFeedID, epoch uint64) (*protocol.State, error) {
	if e := c.snapshotOwner(id, epoch); e != nil {
		return nil, e
	}
	b, ok := c.backend.(bootstrap.Repository)
	if !ok {
		return nil, protocol.Invalid("backend does not support snapshots")
	}
	return b.LoadSnapshot(ctx, id, epoch)
}

func (c *coordinator) SaveSnapshot(ctx context.Context, id common.ChangeFeedID, epoch uint64, s *protocol.State) error {
	if e := c.snapshotOwner(id, epoch); e != nil {
		return e
	}
	b, ok := c.backend.(bootstrap.Repository)
	if !ok {
		return protocol.Invalid("backend does not support snapshots")
	}
	return b.SaveSnapshot(ctx, id, epoch, s)
}
