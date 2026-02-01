// Copyright 2025 PingCAP, Inc.
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

package iceberg

import (
	"context"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

// TruncateTable overwrites a table with an empty snapshot at the given resolved-ts.
func (w *TableWriter) TruncateTable(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	tableInfo *common.TableInfo,
	physicalTableID int64,
	resolvedTs uint64,
) (*CommitResult, error) {
	if w == nil || w.cfg == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg config is nil")
	}
	if tableInfo == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}

	lastCommitted, err := w.GetLastCommittedResolvedTs(ctx, tableInfo)
	if err != nil {
		return nil, err
	}
	if lastCommitted >= resolvedTs {
		return nil, nil
	}

	_, icebergSchema, lastColumnID, err := buildChangelogSchemas(tableInfo, w.cfg.EmitMetadataColumns)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	tableRootRel, tableRootLocation, _, currentMetadata, err := w.resolveTableRoot(ctx, tableInfo.GetSchemaName(), tableInfo.GetTableName())
	if err != nil {
		return nil, err
	}
	if err := w.enforceTableOwner(tableInfo.GetSchemaName(), tableInfo.GetTableName(), currentMetadata, changefeedID); err != nil {
		return nil, err
	}

	baseSequenceNumber := int64(0)
	if currentMetadata != nil {
		baseSequenceNumber = currentMetadata.LastSequenceNumber
	}
	snapshotSequenceNumber := baseSequenceNumber + 1

	now := time.Now().UTC()
	commitUUID := uuid.NewString()
	committedAt := now.Format(time.RFC3339Nano)
	snapshotID := nextSnapshotID(now, currentMetadata)

	manifestListFile, err := w.writeManifestListFile(ctx, tableRootRel, commitUUID, snapshotID, nil)
	if err != nil {
		return nil, err
	}

	summary := map[string]string{
		"operation":                   "overwrite",
		"tidb.changefeed_id":          changefeedID.String(),
		"tidb.changefeed_gid":         changefeedID.ID().String(),
		"tidb.keyspace":               changefeedID.Keyspace(),
		"tidb.table_id":               strconv.FormatInt(physicalTableID, 10),
		summaryKeyCommittedResolvedTs: strconv.FormatUint(resolvedTs, 10),
		"tidb.commit_uuid":            commitUUID,
	}

	metadataBytes, metadataLocation, err := w.commitSnapshot(
		ctx,
		changefeedID,
		tableInfo.GetSchemaName(),
		tableInfo.GetTableName(),
		tableRootRel,
		tableRootLocation,
		lastColumnID,
		icebergSchema,
		summary,
		snapshotID,
		snapshotSequenceNumber,
		now.UnixMilli(),
		manifestListFile.Location,
	)
	if err != nil {
		return nil, err
	}

	return &CommitResult{
		SnapshotID:       snapshotID,
		CommitUUID:       commitUUID,
		MetadataLocation: metadataLocation,
		CommittedAt:      committedAt,
		BytesWritten:     manifestListFile.SizeBytes + metadataBytes,
	}, nil
}
