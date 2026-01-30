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
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

func (w *TableWriter) Upsert(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	tableInfo *common.TableInfo,
	physicalTableID int64,
	dataRows []ChangeRow,
	deleteRows []ChangeRow,
	equalityFieldIDs []int,
	resolvedTs uint64,
) (*CommitResult, error) {
	if tableInfo == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}
	if len(equalityFieldIDs) == 0 {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("equality field ids are empty")
	}

	now := time.Now().UTC()
	commitUUID := uuid.NewString()
	committedAt := now.Format(time.RFC3339Nano)

	_, icebergSchema, lastColumnID, err := buildChangelogSchemas(tableInfo, w.cfg.EmitMetadataColumns)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	icebergSchema.IdentifierFieldIDs = equalityFieldIDs

	tableRootRel, tableRootLocation, _, currentMetadata, err := w.resolveTableRoot(ctx, tableInfo.GetSchemaName(), tableInfo.GetTableName())
	if err != nil {
		return nil, err
	}
	if err := w.enforceTableOwner(tableInfo.GetSchemaName(), tableInfo.GetTableName(), currentMetadata, changefeedID); err != nil {
		return nil, err
	}
	resolvedSpec, err := resolvePartitionSpec(w.cfg, icebergSchema)
	if err != nil {
		return nil, err
	}
	if !resolvedSpec.isSafeForEqualityDeletes(equalityFieldIDs) {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("upsert requires partitioning uses only handle key columns or unpartitioned")
	}
	if err := ensurePartitionSpecMatches(currentMetadata, resolvedSpec); err != nil {
		return nil, err
	}
	baseSequenceNumber := int64(0)
	if currentMetadata != nil {
		baseSequenceNumber = currentMetadata.LastSequenceNumber
	}
	snapshotSequenceNumber := baseSequenceNumber + 1
	snapshotID := now.UnixMilli()

	icebergSchemaBytes, err := json.Marshal(icebergSchema)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	entries := make([]manifestListEntryInput, 0, 2)
	totalBytes := int64(0)
	dataFilesWritten := 0
	deleteFilesWritten := 0
	dataBytes := int64(0)
	deleteBytes := int64(0)

	if len(dataRows) > 0 {
		partitionGroups, err := resolvedSpec.groupRows(dataRows)
		if err != nil {
			return nil, err
		}
		for _, group := range partitionGroups {
			rowChunks := splitRowsByTargetSize(group.rows, w.cfg.TargetFileSizeBytes, w.cfg.EmitMetadataColumns)
			for _, chunkRows := range rowChunks {
				dataUUID := fmt.Sprintf("%s-%06d", commitUUID, dataFilesWritten)
				dataFilesWritten++
				dataFile, err := w.writeDataFile(ctx, tableRootRel, dataUUID, snapshotFilePrefix, tableInfo, chunkRows)
				if err != nil {
					return nil, err
				}
				dataBytes += dataFile.SizeBytes

				dataManifestUUID := uuid.NewString()
				dataManifestFile, err := w.writeManifestFile(
					ctx,
					tableRootRel,
					dataManifestUUID,
					snapshotID,
					snapshotSequenceNumber,
					snapshotSequenceNumber,
					dataFileContentData,
					nil,
					dataFile,
					icebergSchemaBytes,
					group.partition,
					resolvedSpec.partitionSpecJSON,
					resolvedSpec.manifestEntrySchemaV2,
				)
				if err != nil {
					return nil, err
				}
				entries = append(entries, manifestListEntryInput{
					manifestFile:      dataManifestFile,
					partitionSpecID:   int32(resolvedSpec.spec.SpecID),
					content:           manifestContentData,
					sequenceNumber:    snapshotSequenceNumber,
					minSequenceNumber: snapshotSequenceNumber,
				})
				totalBytes += dataFile.SizeBytes + dataManifestFile.SizeBytes
			}
		}
	}

	if len(deleteRows) > 0 {
		deletePartitionSpecID := int32(resolvedSpec.spec.SpecID)
		deletePartitionSpecJSON := resolvedSpec.partitionSpecJSON
		deleteManifestEntrySchema := resolvedSpec.manifestEntrySchemaV2

		var (
			deleteGroups []partitionGroup
			groupErr     error
		)
		if resolvedSpec.isSafeForEqualityDeletes(equalityFieldIDs) {
			deleteGroups, groupErr = resolvedSpec.groupRows(deleteRows)
		} else {
			deleteGroups = []partitionGroup{{partition: resolvedSpec.emptyPartitionRecord(), rows: deleteRows}}
		}
		if groupErr != nil {
			return nil, groupErr
		}

		for _, group := range deleteGroups {
			rowChunks := splitRowsByTargetSize(group.rows, w.cfg.TargetFileSizeBytes, w.cfg.EmitMetadataColumns)
			for _, chunkRows := range rowChunks {
				deleteUUID := fmt.Sprintf("%s-%06d", commitUUID, deleteFilesWritten)
				deleteFilesWritten++
				deleteFile, err := w.writeDataFile(ctx, tableRootRel, deleteUUID, deleteFilePrefix, tableInfo, chunkRows)
				if err != nil {
					return nil, err
				}
				deleteBytes += deleteFile.SizeBytes

				deleteManifestUUID := uuid.NewString()
				deleteManifestFile, err := w.writeManifestFile(
					ctx,
					tableRootRel,
					deleteManifestUUID,
					snapshotID,
					baseSequenceNumber,
					snapshotSequenceNumber,
					dataFileContentEqualityDeletes,
					equalityFieldIDs,
					deleteFile,
					icebergSchemaBytes,
					group.partition,
					deletePartitionSpecJSON,
					deleteManifestEntrySchema,
				)
				if err != nil {
					return nil, err
				}
				entries = append(entries, manifestListEntryInput{
					manifestFile:      deleteManifestFile,
					partitionSpecID:   deletePartitionSpecID,
					content:           manifestContentDeletes,
					sequenceNumber:    snapshotSequenceNumber,
					minSequenceNumber: baseSequenceNumber,
				})
				totalBytes += deleteFile.SizeBytes + deleteManifestFile.SizeBytes
			}
		}
	}

	manifestListFile, err := w.writeManifestListFile(ctx, tableRootRel, commitUUID, snapshotID, entries)
	if err != nil {
		return nil, err
	}
	totalBytes += manifestListFile.SizeBytes

	summary := map[string]string{
		"operation":                   "delta",
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
	totalBytes += metadataBytes

	return &CommitResult{
		SnapshotID:         snapshotID,
		CommitUUID:         commitUUID,
		MetadataLocation:   metadataLocation,
		CommittedAt:        committedAt,
		BytesWritten:       totalBytes,
		DataFilesWritten:   dataFilesWritten,
		DeleteFilesWritten: deleteFilesWritten,
		DataBytesWritten:   dataBytes,
		DeleteBytesWritten: deleteBytes,
	}, nil
}
