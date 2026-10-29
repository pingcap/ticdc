// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package bootstrap

import (
	"context"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
)

const RepositoryService = "SnapshotRuntimeRepository"

// Repository is implemented by Coordinator, using the existing changefeed
// runtime status key and ownership epoch. Mutate preserves concurrent fields.
type Repository interface {
	LoadSnapshot(context.Context, common.ChangeFeedID, uint64) (*protocol.State, error)
	SaveSnapshot(context.Context, common.ChangeFeedID, uint64, *protocol.State) error
}
type Capture interface {
	AbortSnapshot(common.ChangeFeedID)
	ApplySnapshot(context.Context, common.ChangeFeedID, uint64, protocol.Spec, protocol.ObjectRef, protocol.Export) (uint64, error)
	DrainSnapshot(context.Context, common.ChangeFeedID, uint64) error
}
type Schema struct {
	TableID int64  `json:"physical_table_id"`
	Data    []byte `json:"table_info"`
}
type SchemaBundle struct {
	SnapshotTS uint64   `json:"snapshot_ts"`
	Tables     []Schema `json:"tables"`
}
