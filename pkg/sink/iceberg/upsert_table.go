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

// Upsert writes data and equality deletes for upsert mode.
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
	if w == nil || w.cfg == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg config is nil")
	}
	if tableInfo == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}
	if len(equalityFieldIDs) == 0 {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("equality field ids are empty")
	}
	if len(dataRows) == 0 && len(deleteRows) == 0 {
		return nil, nil
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
	snapshotID := nextSnapshotID(now, currentMetadata)

	icebergSchemaBytes, err := json.Marshal(icebergSchema)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	totalBytes := int64(0)
	dataFilesWritten := 0
	deleteFilesWritten := 0
	dataBytes := int64(0)
	deleteBytes := int64(0)
	manifestListEntries := make([]manifestListEntryInput, 0, 2)

	if len(dataRows) > 0 {
		partitionGroups, err := resolvedSpec.groupRows(dataRows)
		if err != nil {
			return nil, err
		}
		dataEntries := make([]manifestEntryInput, 0, len(partitionGroups))
		targetSize := w.effectiveTargetFileSizeBytes(physicalTableID, false)
		for _, group := range partitionGroups {
			rowChunks := splitRowsByTargetSize(group.rows, targetSize, w.cfg.EmitMetadataColumns)
			for _, chunk := range rowChunks {
				chunkRows := chunk.rows
				dataUUID := fmt.Sprintf("%s-%06d", commitUUID, dataFilesWritten)
				dataFilesWritten++
				dataFile, err := w.writeDataFile(ctx, tableRootRel, dataUUID, snapshotFilePrefix, tableInfo, chunkRows)
				if err != nil {
					return nil, err
				}
				dataBytes += dataFile.SizeBytes
				totalBytes += dataFile.SizeBytes
				w.updateFileSizeTuner(physicalTableID, false, chunk.estimatedBytes, dataFile.SizeBytes)

				dataEntries = append(dataEntries, manifestEntryInput{
					snapshotID:         snapshotID,
					dataSequenceNumber: snapshotSequenceNumber,
					fileSequenceNumber: snapshotSequenceNumber,
					fileContent:        dataFileContentData,
					dataFile:           dataFile,
					partitionRecord:    group.partition,
				})
			}
		}
		dataManifestEntries, manifestBytes, err := w.writeManifestFiles(
			ctx,
			tableRootRel,
			dataEntries,
			icebergSchemaBytes,
			resolvedSpec.partitionSpecJSON,
			resolvedSpec.manifestEntrySchemaV2,
			int32(resolvedSpec.spec.SpecID),
			manifestContentData,
			snapshotSequenceNumber,
			snapshotSequenceNumber,
		)
		if err != nil {
			return nil, err
		}
		totalBytes += manifestBytes
		manifestListEntries = append(manifestListEntries, dataManifestEntries...)
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

		deleteEntries := make([]manifestEntryInput, 0, len(deleteGroups))
		targetSize := w.effectiveTargetFileSizeBytes(physicalTableID, true)
		for _, group := range deleteGroups {
			rowChunks := splitRowsByTargetSize(group.rows, targetSize, w.cfg.EmitMetadataColumns)
			for _, chunk := range rowChunks {
				chunkRows := chunk.rows
				deleteUUID := fmt.Sprintf("%s-%06d", commitUUID, deleteFilesWritten)
				deleteFilesWritten++
				deleteFile, err := w.writeDataFile(ctx, tableRootRel, deleteUUID, deleteFilePrefix, tableInfo, chunkRows)
				if err != nil {
					return nil, err
				}
				deleteBytes += deleteFile.SizeBytes
				totalBytes += deleteFile.SizeBytes
				w.updateFileSizeTuner(physicalTableID, true, chunk.estimatedBytes, deleteFile.SizeBytes)

				deleteEntries = append(deleteEntries, manifestEntryInput{
					snapshotID:         snapshotID,
					dataSequenceNumber: baseSequenceNumber,
					fileSequenceNumber: snapshotSequenceNumber,
					fileContent:        dataFileContentEqualityDeletes,
					equalityFieldIDs:   equalityFieldIDs,
					dataFile:           deleteFile,
					partitionRecord:    group.partition,
				})
			}
		}
		deleteManifestEntries, manifestBytes, err := w.writeManifestFiles(
			ctx,
			tableRootRel,
			deleteEntries,
			icebergSchemaBytes,
			deletePartitionSpecJSON,
			deleteManifestEntrySchema,
			deletePartitionSpecID,
			manifestContentDeletes,
			snapshotSequenceNumber,
			baseSequenceNumber,
		)
		if err != nil {
			return nil, err
		}
		totalBytes += manifestBytes
		manifestListEntries = append(manifestListEntries, deleteManifestEntries...)
	}

	manifestListFile, err := w.writeManifestListFile(ctx, tableRootRel, commitUUID, snapshotID, manifestListEntries)
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
