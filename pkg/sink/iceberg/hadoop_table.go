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
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"go.uber.org/zap"
)

const (
	metadataDirName    = "metadata"
	dataDirName        = "data"
	versionHintFile    = "version-hint.text"
	manifestFilePrefix = "manifest"
	snapshotFilePrefix = "snap"
	deleteFilePrefix   = "delete"

	avroCompressionSnappy = "snappy"
)

const (
	icebergFormatVersion                = 2
	icebergSchemaID                     = 0
	icebergPartitionSpecID              = 0
	icebergSortOrderID                  = int64(0)
	icebergLastPartitionIDUnpartitioned = 999

	manifestStatusAdded    = int32(1)
	manifestContentData    = int32(0)
	manifestContentDeletes = int32(1)

	dataFileContentData            = int32(0)
	dataFileContentEqualityDeletes = int32(2)
)

const (
	icebergMetaIDOp         = 2000000001
	icebergMetaIDCommitTs   = 2000000002
	icebergMetaIDCommitTime = 2000000003
)

const (
	summaryKeyCommittedResolvedTs = "tidb.committed_resolved_ts"
)

const (
	tablePropertyChangefeedID  = "tidb.changefeed_id"
	tablePropertyChangefeedGID = "tidb.changefeed_gid"
)

const (
	maxManifestEntriesPerFile = 1000
	fileSizeTunerAlpha        = 0.2
	fileSizeTunerMinRatio     = 0.1
	fileSizeTunerMaxRatio     = 10.0
)

// ChangeRow carries the row fields used for Iceberg writes.
type ChangeRow struct {
	Op         string
	CommitTs   string
	CommitTime string
	Columns    map[string]*string
}

// FileInfo describes a file written to the Iceberg table.
type FileInfo struct {
	Location       string
	RelativePath   string
	RecordCount    int64
	SizeBytes      int64
	FileFormatName string
}

// CommitResult summarizes a successful Iceberg commit.
type CommitResult struct {
	SnapshotID       int64
	CommitUUID       string
	MetadataLocation string
	CommittedAt      string
	BytesWritten     int64

	DataFilesWritten   int
	DeleteFilesWritten int

	DataBytesWritten   int64
	DeleteBytesWritten int64
}

type tableFileSizeTuner struct {
	dataRatio   float64
	deleteRatio float64
}

// TableWriter writes TiCDC change rows into Iceberg tables.
type TableWriter struct {
	cfg     *Config
	storage storage.ExternalStorage

	glueOnce   sync.Once
	glueClient any
	glueErr    error

	checkpointOnce      sync.Once
	checkpointTableInfo *common.TableInfo
	checkpointTableErr  error

	globalCheckpointOnce      sync.Once
	globalCheckpointTableInfo *common.TableInfo
	globalCheckpointTableErr  error

	tunerMu        sync.Mutex
	fileSizeTuners map[int64]*tableFileSizeTuner
}

// NewTableWriter creates a TableWriter for the given config and storage.
func NewTableWriter(cfg *Config, storage storage.ExternalStorage) *TableWriter {
	return &TableWriter{
		cfg:            cfg,
		storage:        storage,
		fileSizeTuners: make(map[int64]*tableFileSizeTuner),
	}
}

func (w *TableWriter) effectiveTargetFileSizeBytes(tableID int64, isDelete bool) int64 {
	if w == nil || w.cfg == nil {
		return defaultTargetFileSizeBytes
	}
	target := w.cfg.TargetFileSizeBytes
	if !w.cfg.AutoTuneFileSize || tableID == 0 {
		return target
	}

	ratio := 1.0
	w.tunerMu.Lock()
	if tuner := w.fileSizeTuners[tableID]; tuner != nil {
		if isDelete {
			if tuner.deleteRatio > 0 {
				ratio = tuner.deleteRatio
			}
		} else {
			if tuner.dataRatio > 0 {
				ratio = tuner.dataRatio
			}
		}
	}
	w.tunerMu.Unlock()

	if ratio < fileSizeTunerMinRatio {
		ratio = fileSizeTunerMinRatio
	}
	if ratio > fileSizeTunerMaxRatio {
		ratio = fileSizeTunerMaxRatio
	}
	adjusted := float64(target) / ratio
	if adjusted < float64(minTargetFileSizeBytes) {
		adjusted = float64(minTargetFileSizeBytes)
	}
	if adjusted > float64(maxTargetFileSizeBytes) {
		adjusted = float64(maxTargetFileSizeBytes)
	}
	return int64(adjusted)
}

func (w *TableWriter) updateFileSizeTuner(tableID int64, isDelete bool, estimatedBytes, actualBytes int64) {
	if w == nil || w.cfg == nil || !w.cfg.AutoTuneFileSize {
		return
	}
	if tableID == 0 || estimatedBytes <= 0 || actualBytes <= 0 {
		return
	}

	newRatio := float64(actualBytes) / float64(estimatedBytes)
	if newRatio < fileSizeTunerMinRatio {
		newRatio = fileSizeTunerMinRatio
	}
	if newRatio > fileSizeTunerMaxRatio {
		newRatio = fileSizeTunerMaxRatio
	}

	w.tunerMu.Lock()
	tuner := w.fileSizeTuners[tableID]
	if tuner == nil {
		tuner = &tableFileSizeTuner{}
		w.fileSizeTuners[tableID] = tuner
	}
	if isDelete {
		if tuner.deleteRatio <= 0 {
			tuner.deleteRatio = newRatio
		} else {
			tuner.deleteRatio = tuner.deleteRatio*(1-fileSizeTunerAlpha) + newRatio*fileSizeTunerAlpha
		}
	} else {
		if tuner.dataRatio <= 0 {
			tuner.dataRatio = newRatio
		} else {
			tuner.dataRatio = tuner.dataRatio*(1-fileSizeTunerAlpha) + newRatio*fileSizeTunerAlpha
		}
	}
	w.tunerMu.Unlock()
}

// GetLastCommittedResolvedTs returns the last committed resolved-ts for a table.
func (w *TableWriter) GetLastCommittedResolvedTs(ctx context.Context, tableInfo *common.TableInfo) (uint64, error) {
	if tableInfo == nil {
		return 0, cerror.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}

	metadataDirRel := path.Join(w.tableRootPath(tableInfo.GetSchemaName(), tableInfo.GetTableName()), metadataDirName)
	_, m, err := w.loadTableMetadata(ctx, tableInfo.GetSchemaName(), tableInfo.GetTableName(), metadataDirRel)
	if err != nil {
		return 0, err
	}
	if m == nil {
		return 0, nil
	}

	s := m.currentSnapshot()
	if s == nil || len(s.Summary) == 0 {
		return 0, nil
	}

	raw := strings.TrimSpace(s.Summary[summaryKeyCommittedResolvedTs])
	if raw == "" {
		return 0, nil
	}
	resolvedTs, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	return resolvedTs, nil
}

// EnsureTable ensures the target Iceberg table exists and schema is aligned.
func (w *TableWriter) EnsureTable(ctx context.Context, changefeedID common.ChangeFeedID, tableInfo *common.TableInfo) error {
	if tableInfo == nil {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}

	_, desiredSchema, lastColumnID, err := buildChangelogSchemas(tableInfo, w.cfg.EmitMetadataColumns)
	if err != nil {
		return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	tableRootRel, tableRootLocation, currentVersion, currentMetadata, err := w.resolveTableRoot(ctx, tableInfo.GetSchemaName(), tableInfo.GetTableName())
	if err != nil {
		return err
	}
	if err := w.enforceTableOwner(tableInfo.GetSchemaName(), tableInfo.GetTableName(), currentMetadata, changefeedID); err != nil {
		return err
	}
	partitionSpec, err := resolvePartitionSpec(w.cfg, desiredSchema)
	if err != nil {
		return err
	}
	if err := ensurePartitionSpecMatches(currentMetadata, partitionSpec); err != nil {
		return err
	}

	metadataDirRel := path.Join(tableRootRel, metadataDirName)

	if currentMetadata == nil {
		newMetadata := newTableMetadata(tableRootLocation, lastColumnID, desiredSchema)
		if err := ensurePartitionSpecMatches(newMetadata, partitionSpec); err != nil {
			return err
		}
		w.ensureTableProperties(newMetadata)
		w.setTableOwnerIfNeeded(tableInfo.GetSchemaName(), tableInfo.GetTableName(), newMetadata, changefeedID)

		nextVersion, err := w.nextAvailableMetadataVersion(ctx, metadataDirRel, 1)
		if err != nil {
			return err
		}
		metadataRel := path.Join(metadataDirRel, fmt.Sprintf("v%d.metadata.json", nextVersion))
		metadataLocation, err := joinLocation(w.cfg.WarehouseLocation, metadataRel)
		if err != nil {
			return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		newMetadata.SelfMetadataLocation = metadataLocation

		metadataBytes, err := json.Marshal(newMetadata)
		if err != nil {
			return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		if err := w.storage.WriteFile(ctx, metadataRel, metadataBytes); err != nil {
			return cerror.Trace(err)
		}

		if err := w.ensureGlueTable(ctx, changefeedID, tableInfo.GetSchemaName(), tableInfo.GetTableName(), tableRootLocation, metadataLocation); err != nil {
			return err
		}
		return w.writeVersionHint(ctx, metadataDirRel, nextVersion)
	}

	w.ensureTableProperties(currentMetadata)
	w.setTableOwnerIfNeeded(tableInfo.GetSchemaName(), tableInfo.GetTableName(), currentMetadata, changefeedID)
	if err := validateSchemaEvolution(w.cfg.SchemaMode, currentMetadata.currentSchema(), desiredSchema); err != nil {
		return err
	}
	if !applySchemaIfChanged(currentMetadata, desiredSchema, lastColumnID) {
		return nil
	}

	nextVersion, err := w.nextAvailableMetadataVersion(ctx, metadataDirRel, currentVersion+1)
	if err != nil {
		return err
	}
	currentMetadata.MetadataLog = append(currentMetadata.MetadataLog, metadataLogEntry{
		MetadataFile: currentMetadata.SelfMetadataLocation,
		TimestampMs:  currentMetadata.LastUpdatedMs,
	})
	currentMetadata.LastUpdatedMs = time.Now().UnixMilli()
	currentMetadata.LastColumnID = maxInt(currentMetadata.LastColumnID, lastColumnID)
	ensureMainBranchRef(currentMetadata)

	metadataRel := path.Join(metadataDirRel, fmt.Sprintf("v%d.metadata.json", nextVersion))
	metadataLocation, err := joinLocation(w.cfg.WarehouseLocation, metadataRel)
	if err != nil {
		return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	currentMetadata.SelfMetadataLocation = metadataLocation

	metadataBytes, err := json.Marshal(currentMetadata)
	if err != nil {
		return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	if err := w.storage.WriteFile(ctx, metadataRel, metadataBytes); err != nil {
		return cerror.Trace(err)
	}

	if err := w.ensureGlueTable(ctx, changefeedID, tableInfo.GetSchemaName(), tableInfo.GetTableName(), tableRootLocation, metadataLocation); err != nil {
		return err
	}
	return w.writeVersionHint(ctx, metadataDirRel, nextVersion)
}

// AppendChangelog writes append-mode changelog rows to Iceberg.
func (w *TableWriter) AppendChangelog(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	tableInfo *common.TableInfo,
	physicalTableID int64,
	rows []ChangeRow,
	resolvedTs uint64,
) (*CommitResult, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	if tableInfo == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}

	now := time.Now().UTC()
	commitUUID := uuid.NewString()
	committedAt := now.Format(time.RFC3339Nano)

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
	partitionSpec, err := resolvePartitionSpec(w.cfg, icebergSchema)
	if err != nil {
		return nil, err
	}
	if err := ensurePartitionSpecMatches(currentMetadata, partitionSpec); err != nil {
		return nil, err
	}
	snapshotSequenceNumber := int64(1)
	if currentMetadata != nil {
		snapshotSequenceNumber = currentMetadata.LastSequenceNumber + 1
	}

	snapshotID := nextSnapshotID(now, currentMetadata)
	icebergSchemaBytes, err := json.Marshal(icebergSchema)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	var (
		totalBytes int64
		dataBytes  int64
	)
	partitionGroups, err := partitionSpec.groupRows(rows)
	if err != nil {
		return nil, err
	}
	manifestEntries := make([]manifestEntryInput, 0, len(partitionGroups))
	dataFilesWritten := 0
	targetSize := w.effectiveTargetFileSizeBytes(physicalTableID, false)
	for _, group := range partitionGroups {
		rowChunks := splitRowsByTargetSize(group.rows, targetSize, w.cfg.EmitMetadataColumns)
		for _, chunk := range rowChunks {
			chunkRows := chunk.rows
			fileUUID := fmt.Sprintf("%s-%06d", commitUUID, dataFilesWritten)
			dataFilesWritten++
			dataFile, err := w.writeDataFile(ctx, tableRootRel, fileUUID, snapshotFilePrefix, tableInfo, chunkRows)
			if err != nil {
				return nil, err
			}
			totalBytes += dataFile.SizeBytes
			dataBytes += dataFile.SizeBytes
			w.updateFileSizeTuner(physicalTableID, false, chunk.estimatedBytes, dataFile.SizeBytes)
			manifestEntries = append(manifestEntries, manifestEntryInput{
				snapshotID:         snapshotID,
				dataSequenceNumber: snapshotSequenceNumber,
				fileSequenceNumber: snapshotSequenceNumber,
				fileContent:        dataFileContentData,
				dataFile:           dataFile,
				partitionRecord:    group.partition,
			})
		}
	}

	manifestListEntries, manifestBytes, err := w.writeManifestFiles(
		ctx,
		tableRootRel,
		manifestEntries,
		icebergSchemaBytes,
		partitionSpec.partitionSpecJSON,
		partitionSpec.manifestEntrySchemaV2,
		int32(partitionSpec.spec.SpecID),
		manifestContentData,
		snapshotSequenceNumber,
		snapshotSequenceNumber,
	)
	if err != nil {
		return nil, err
	}
	totalBytes += manifestBytes

	manifestListFile, err := w.writeManifestListFile(ctx, tableRootRel, commitUUID, snapshotID, manifestListEntries)
	if err != nil {
		return nil, err
	}
	totalBytes += manifestListFile.SizeBytes

	summary := map[string]string{
		"operation":                   "append",
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
		SnapshotID:       snapshotID,
		CommitUUID:       commitUUID,
		MetadataLocation: metadataLocation,
		CommittedAt:      committedAt,
		BytesWritten:     totalBytes,
		DataFilesWritten: dataFilesWritten,
		DataBytesWritten: dataBytes,
	}, nil
}

func (w *TableWriter) tableRootPath(schemaName, tableName string) string {
	return path.Join(
		escapePathSegment(w.cfg.Namespace),
		escapePathSegment(schemaName),
		escapePathSegment(tableName),
	)
}

func (w *TableWriter) resolveTableRoot(
	ctx context.Context,
	schemaName string,
	tableName string,
) (string, string, int, *tableMetadata, error) {
	if w == nil || w.cfg == nil {
		return "", "", 0, nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg config is nil")
	}

	defaultRel := w.tableRootPath(schemaName, tableName)
	defaultLocation, err := joinLocation(w.cfg.WarehouseLocation, defaultRel)
	if err != nil {
		return "", "", 0, nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	if w.cfg.Catalog != CatalogGlue {
		metadataDirRel := path.Join(defaultRel, metadataDirName)
		version, m, err := w.loadTableMetadata(ctx, schemaName, tableName, metadataDirRel)
		if err != nil {
			return "", "", 0, nil, err
		}
		return defaultRel, defaultLocation, version, m, nil
	}

	version, m, err := w.loadTableMetadataFromGlue(ctx, schemaName, tableName)
	if err != nil {
		return "", "", 0, nil, err
	}
	if m == nil || strings.TrimSpace(m.Location) == "" {
		return defaultRel, defaultLocation, version, m, nil
	}
	rel, err := w.relativePathFromLocation(m.Location)
	if err != nil {
		return "", "", 0, nil, err
	}
	return rel, m.Location, version, m, nil
}

func (w *TableWriter) enforceTableOwner(schemaName, tableName string, currentMetadata *tableMetadata, changefeedID common.ChangeFeedID) error {
	if w == nil || w.cfg == nil || currentMetadata == nil || strings.TrimSpace(changefeedID.String()) == "" {
		return nil
	}
	if schemaName == checkpointSchemaName && (tableName == checkpointTableName || tableName == globalCheckpointTableName) {
		return nil
	}

	expectedOwners := []string{
		strings.TrimSpace(changefeedID.String()),
		strings.TrimSpace(changefeedID.ID().String()),
	}

	var owner string
	if currentMetadata.Properties != nil {
		owner = strings.TrimSpace(currentMetadata.Properties[tablePropertyChangefeedID])
	}
	if owner == "" {
		s := currentMetadata.currentSnapshot()
		if s != nil && s.Summary != nil {
			owner = strings.TrimSpace(s.Summary[tablePropertyChangefeedID])
		}
	}
	if owner == "" || owner == expectedOwners[0] || owner == expectedOwners[1] {
		return nil
	}
	if w.cfg.AllowTakeover {
		log.Warn("iceberg table owner mismatch, takeover enabled",
			zap.String("schema", schemaName),
			zap.String("table", tableName),
			zap.String("ownerChangefeedID", owner),
			zap.String("changefeedID", expectedOwners[0]))
		return nil
	}
	return cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg table is owned by another changefeed (set allow-takeover=true to override)")
}

func (w *TableWriter) setTableOwnerIfNeeded(schemaName, tableName string, m *tableMetadata, changefeedID common.ChangeFeedID) {
	if w == nil || w.cfg == nil || m == nil || strings.TrimSpace(changefeedID.String()) == "" {
		return
	}
	if schemaName == checkpointSchemaName && (tableName == checkpointTableName || tableName == globalCheckpointTableName) {
		return
	}

	if m.Properties == nil {
		m.Properties = make(map[string]string)
	}
	current := strings.TrimSpace(m.Properties[tablePropertyChangefeedID])
	displayID := strings.TrimSpace(changefeedID.String())
	gid := strings.TrimSpace(changefeedID.ID().String())
	if current == "" || ((current != displayID && current != gid) && w.cfg.AllowTakeover) {
		m.Properties[tablePropertyChangefeedID] = displayID
		if gid != "" {
			m.Properties[tablePropertyChangefeedGID] = gid
		}
	}
}

func escapePathSegment(segment string) string {
	return url.PathEscape(segment)
}

func joinLocation(baseLocation, relPath string) (string, error) {
	base, err := url.Parse(baseLocation)
	if err != nil {
		return "", err
	}
	base.Path = path.Join(base.Path, relPath)
	return base.String(), nil
}

func (w *TableWriter) writeDataFile(
	ctx context.Context,
	tableRootRel string,
	commitUUID string,
	fileNamePrefix string,
	tableInfo *common.TableInfo,
	rows []ChangeRow,
) (*FileInfo, error) {
	if tableInfo == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}

	dataBytes, err := encodeParquetRows(tableInfo, w.cfg.EmitMetadataColumns, rows)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	dataRel := path.Join(tableRootRel, dataDirName, fmt.Sprintf("%s-%s.parquet", fileNamePrefix, commitUUID))
	dataLocation, err := joinLocation(w.cfg.WarehouseLocation, dataRel)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	if err := w.storage.WriteFile(ctx, dataRel, dataBytes); err != nil {
		return nil, cerror.Trace(err)
	}

	return &FileInfo{
		Location:       dataLocation,
		RelativePath:   dataRel,
		RecordCount:    int64(len(rows)),
		SizeBytes:      int64(len(dataBytes)),
		FileFormatName: "PARQUET",
	}, nil
}

func (w *TableWriter) writeManifestFile(
	ctx context.Context,
	tableRootRel string,
	manifestUUID string,
	entries []manifestEntryInput,
	icebergSchemaJSON []byte,
	partitionSpecJSON []byte,
	manifestEntrySchema string,
) (*FileInfo, error) {
	if len(entries) == 0 {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("manifest entries are empty")
	}

	records := make([]any, 0, len(entries))
	for _, entry := range entries {
		if entry.dataFile == nil {
			return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("manifest data file is nil")
		}

		var equalityIDs any
		if len(entry.equalityFieldIDs) > 0 {
			ids := make([]any, 0, len(entry.equalityFieldIDs))
			for _, id := range entry.equalityFieldIDs {
				ids = append(ids, int32(id))
			}
			equalityIDs = wrapUnion("array", ids)
		}

		records = append(records, map[string]any{
			"status":               manifestStatusAdded,
			"snapshot_id":          wrapUnion("long", entry.snapshotID),
			"sequence_number":      wrapUnion("long", entry.dataSequenceNumber),
			"file_sequence_number": wrapUnion("long", entry.fileSequenceNumber),
			"data_file": map[string]any{
				"content":            entry.fileContent,
				"file_path":          entry.dataFile.Location,
				"file_format":        entry.dataFile.FileFormatName,
				"partition":          entry.partitionRecord,
				"record_count":       entry.dataFile.RecordCount,
				"file_size_in_bytes": entry.dataFile.SizeBytes,
				"equality_ids":       equalityIDs,
				"sort_order_id":      nil,
			},
		})
	}

	meta := map[string][]byte{
		"schema":         icebergSchemaJSON,
		"partition-spec": partitionSpecJSON,
	}

	manifestBytes, err := writeOCF(manifestEntrySchema, meta, avroCompressionSnappy, records)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	manifestRel := path.Join(tableRootRel, metadataDirName, fmt.Sprintf("%s-%s.avro", manifestFilePrefix, manifestUUID))
	manifestLocation, err := joinLocation(w.cfg.WarehouseLocation, manifestRel)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	if err := w.storage.WriteFile(ctx, manifestRel, manifestBytes); err != nil {
		return nil, cerror.Trace(err)
	}

	return &FileInfo{
		Location:       manifestLocation,
		RelativePath:   manifestRel,
		RecordCount:    int64(len(entries)),
		SizeBytes:      int64(len(manifestBytes)),
		FileFormatName: "AVRO",
	}, nil
}

func (w *TableWriter) writeManifestFiles(
	ctx context.Context,
	tableRootRel string,
	entries []manifestEntryInput,
	icebergSchemaJSON []byte,
	partitionSpecJSON []byte,
	manifestEntrySchema string,
	partitionSpecID int32,
	content int32,
	sequenceNumber int64,
	minSequenceNumber int64,
) ([]manifestListEntryInput, int64, error) {
	if len(entries) == 0 {
		return nil, 0, nil
	}

	if partitionSpecID == 0 {
		partitionSpecID = int32(icebergPartitionSpecID)
	}

	manifestEntries := make([]manifestListEntryInput, 0, (len(entries)+maxManifestEntriesPerFile-1)/maxManifestEntriesPerFile)
	var totalBytes int64
	for start := 0; start < len(entries); start += maxManifestEntriesPerFile {
		end := start + maxManifestEntriesPerFile
		if end > len(entries) {
			end = len(entries)
		}
		manifestUUID := uuid.NewString()
		manifestFile, err := w.writeManifestFile(
			ctx,
			tableRootRel,
			manifestUUID,
			entries[start:end],
			icebergSchemaJSON,
			partitionSpecJSON,
			manifestEntrySchema,
		)
		if err != nil {
			return nil, 0, err
		}
		totalBytes += manifestFile.SizeBytes
		manifestEntries = append(manifestEntries, manifestListEntryInput{
			manifestFile:      manifestFile,
			partitionSpecID:   partitionSpecID,
			content:           content,
			sequenceNumber:    sequenceNumber,
			minSequenceNumber: minSequenceNumber,
		})
	}
	return manifestEntries, totalBytes, nil
}

func (w *TableWriter) writeManifestListFile(
	ctx context.Context,
	tableRootRel string,
	commitUUID string,
	snapshotID int64,
	entries []manifestListEntryInput,
) (*FileInfo, error) {
	records := make([]any, 0, len(entries))
	for _, entry := range entries {
		if entry.manifestFile == nil {
			return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("manifest file is nil")
		}
		specID := entry.partitionSpecID
		if specID == 0 {
			specID = int32(icebergPartitionSpecID)
		}
		records = append(records, map[string]any{
			"manifest_path":       entry.manifestFile.Location,
			"manifest_length":     entry.manifestFile.SizeBytes,
			"partition_spec_id":   specID,
			"content":             entry.content,
			"sequence_number":     entry.sequenceNumber,
			"min_sequence_number": entry.minSequenceNumber,
			"added_snapshot_id":   snapshotID,
		})
	}

	meta := map[string][]byte{
		"snapshot-id": []byte(strconv.FormatInt(snapshotID, 10)),
	}

	manifestListBytes, err := writeOCF(manifestListSchemaV2, meta, avroCompressionSnappy, records)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	manifestListRel := path.Join(tableRootRel, metadataDirName, fmt.Sprintf("%s-%d-%s.avro", snapshotFilePrefix, snapshotID, commitUUID))
	manifestListLocation, err := joinLocation(w.cfg.WarehouseLocation, manifestListRel)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	if err := w.storage.WriteFile(ctx, manifestListRel, manifestListBytes); err != nil {
		return nil, cerror.Trace(err)
	}

	return &FileInfo{
		Location:       manifestListLocation,
		RelativePath:   manifestListRel,
		RecordCount:    int64(len(records)),
		SizeBytes:      int64(len(manifestListBytes)),
		FileFormatName: "AVRO",
	}, nil
}

type manifestListEntryInput struct {
	manifestFile      *FileInfo
	partitionSpecID   int32
	content           int32
	sequenceNumber    int64
	minSequenceNumber int64
}

type manifestEntryInput struct {
	snapshotID         int64
	dataSequenceNumber int64
	fileSequenceNumber int64
	fileContent        int32
	equalityFieldIDs   []int
	dataFile           *FileInfo
	partitionRecord    map[string]any
}

func (w *TableWriter) commitSnapshot(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	schemaName string,
	tableName string,
	tableRootRel string,
	tableRootLocation string,
	lastColumnID int,
	schema *icebergSchema,
	summary map[string]string,
	snapshotID int64,
	snapshotSequenceNumber int64,
	timestampMs int64,
	manifestListLocation string,
) (int64, string, error) {
	metadataDirRel := path.Join(tableRootRel, metadataDirName)

	currentVersion, currentMetadata, err := w.loadTableMetadata(ctx, schemaName, tableName, metadataDirRel)
	if err != nil {
		return 0, "", err
	}
	if err := w.enforceTableOwner(schemaName, tableName, currentMetadata, changefeedID); err != nil {
		return 0, "", err
	}

	nextVersion, err := w.nextAvailableMetadataVersion(ctx, metadataDirRel, currentVersion+1)
	if err != nil {
		return 0, "", err
	}
	now := time.Now().UnixMilli()

	var parentSnapshotID *int64
	if currentMetadata != nil && currentMetadata.CurrentSnapshotID != nil {
		parent := *currentMetadata.CurrentSnapshotID
		parentSnapshotID = &parent
	}

	var newMetadata *tableMetadata
	if currentMetadata == nil {
		newMetadata = newTableMetadata(tableRootLocation, lastColumnID, schema)
	} else {
		newMetadata = currentMetadata
		newMetadata.MetadataLog = append(newMetadata.MetadataLog, metadataLogEntry{
			MetadataFile: currentMetadata.SelfMetadataLocation,
			TimestampMs:  currentMetadata.LastUpdatedMs,
		})
	}

	if err := validateSchemaEvolution(w.cfg.SchemaMode, newMetadata.currentSchema(), schema); err != nil {
		return 0, "", err
	}
	applySchemaIfChanged(newMetadata, schema, lastColumnID)

	if newMetadata.FormatVersion != icebergFormatVersion {
		newMetadata.FormatVersion = icebergFormatVersion
	}
	partitionSpec, err := resolvePartitionSpec(w.cfg, schema)
	if err != nil {
		return 0, "", err
	}
	if err := ensurePartitionSpecMatches(newMetadata, partitionSpec); err != nil {
		return 0, "", err
	}
	w.ensureTableProperties(newMetadata)
	w.setTableOwnerIfNeeded(schemaName, tableName, newMetadata, changefeedID)
	if len(newMetadata.SortOrders) == 0 {
		newMetadata.SortOrders = []sortOrder{{OrderID: icebergSortOrderID, Fields: []any{}}}
		newMetadata.DefaultSortOrderID = icebergSortOrderID
	}

	newMetadata.SelfMetadataLocation = ""
	newMetadata.LastUpdatedMs = now
	newMetadata.LastColumnID = maxInt(newMetadata.LastColumnID, lastColumnID)
	if snapshotSequenceNumber <= newMetadata.LastSequenceNumber {
		snapshotSequenceNumber = newMetadata.LastSequenceNumber + 1
	}
	newMetadata.LastSequenceNumber = snapshotSequenceNumber

	newSnapshot := snapshot{
		SnapshotID:       snapshotID,
		ParentSnapshotID: parentSnapshotID,
		SequenceNumber:   snapshotSequenceNumber,
		TimestampMs:      timestampMs,
		ManifestList:     manifestListLocation,
		Summary:          summary,
	}
	newMetadata.Snapshots = append(newMetadata.Snapshots, newSnapshot)
	newMetadata.CurrentSnapshotID = &snapshotID
	newMetadata.SnapshotLog = append(newMetadata.SnapshotLog, snapshotLogEntry{
		TimestampMs: timestampMs,
		SnapshotID:  snapshotID,
	})
	ensureMainBranchRef(newMetadata)

	metadataRel := path.Join(metadataDirRel, fmt.Sprintf("v%d.metadata.json", nextVersion))
	metadataLocation, err := joinLocation(w.cfg.WarehouseLocation, metadataRel)
	if err != nil {
		return 0, "", cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	newMetadata.SelfMetadataLocation = metadataLocation

	metadataBytes, err := json.Marshal(newMetadata)
	if err != nil {
		return 0, "", cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	if err := w.storage.WriteFile(ctx, metadataRel, metadataBytes); err != nil {
		return 0, "", cerror.Trace(err)
	}

	if err := w.ensureGlueTable(ctx, changefeedID, schemaName, tableName, tableRootLocation, metadataLocation); err != nil {
		return 0, "", err
	}

	if err := w.writeVersionHint(ctx, metadataDirRel, nextVersion); err != nil {
		return 0, "", err
	}

	return int64(len(metadataBytes)) + int64(len(strconv.Itoa(nextVersion))), metadataLocation, nil
}

func (w *TableWriter) ensureTableProperties(m *tableMetadata) {
	if w == nil || w.cfg == nil || m == nil {
		return
	}
	if m.Properties == nil {
		m.Properties = make(map[string]string)
	}
	if strings.TrimSpace(m.Properties["write.format.default"]) == "" {
		m.Properties["write.format.default"] = "parquet"
	}
	if strings.TrimSpace(m.Properties["write.parquet.compression-codec"]) == "" {
		m.Properties["write.parquet.compression-codec"] = "zstd"
	}
	if strings.TrimSpace(m.Properties["write.update.mode"]) == "" {
		m.Properties["write.update.mode"] = "merge-on-read"
	}
	if strings.TrimSpace(m.Properties["write.delete.mode"]) == "" {
		m.Properties["write.delete.mode"] = "merge-on-read"
	}
	if w.cfg.TargetFileSizeBytes > 0 {
		m.Properties["write.target-file-size-bytes"] = strconv.FormatInt(w.cfg.TargetFileSizeBytes, 10)
	}
}

func (w *TableWriter) nextAvailableMetadataVersion(ctx context.Context, metadataDirRel string, startVersion int) (int, error) {
	if w == nil || w.storage == nil {
		return 0, cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg storage is nil")
	}
	if startVersion <= 0 {
		startVersion = 1
	}
	version := startVersion
	for i := 0; i < 1024; i++ {
		metadataRel := path.Join(metadataDirRel, fmt.Sprintf("v%d.metadata.json", version))
		exists, err := w.storage.FileExists(ctx, metadataRel)
		if err != nil {
			return 0, cerror.Trace(err)
		}
		if !exists {
			return version, nil
		}
		version++
	}
	return 0, cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg metadata version overflow")
}

func (w *TableWriter) writeVersionHint(ctx context.Context, metadataDirRel string, version int) error {
	if w == nil || w.storage == nil {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg storage is nil")
	}
	hintRel := path.Join(metadataDirRel, versionHintFile)
	hintBytes := []byte(strconv.Itoa(version))
	if err := w.storage.WriteFile(ctx, hintRel, hintBytes); err != nil {
		if w.cfg != nil && w.cfg.Catalog == CatalogGlue {
			log.Warn("write iceberg version hint failed", zap.Error(err))
			return nil
		}
		return cerror.Trace(err)
	}
	return nil
}

func (w *TableWriter) loadTableMetadata(ctx context.Context, schemaName, tableName, metadataDirRel string) (int, *tableMetadata, error) {
	if w == nil || w.cfg == nil {
		return 0, nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg config is nil")
	}

	if w.cfg.Catalog == CatalogGlue {
		return w.loadTableMetadataFromGlue(ctx, schemaName, tableName)
	}
	return w.loadTableMetadataFromHint(ctx, metadataDirRel)
}

func (w *TableWriter) loadTableMetadataFromHint(ctx context.Context, metadataDirRel string) (int, *tableMetadata, error) {
	hintRel := path.Join(metadataDirRel, versionHintFile)

	exists, err := w.storage.FileExists(ctx, hintRel)
	if err != nil {
		return 0, nil, cerror.Trace(err)
	}
	if !exists {
		return w.loadLatestTableMetadata(ctx, metadataDirRel)
	}

	hintBytes, err := w.storage.ReadFile(ctx, hintRel)
	if err != nil {
		return 0, nil, cerror.Trace(err)
	}
	versionStr := strings.TrimSpace(string(hintBytes))
	version, err := strconv.Atoi(versionStr)
	if err != nil {
		return 0, nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	metadataRel := path.Join(metadataDirRel, fmt.Sprintf("v%d.metadata.json", version))
	metadataBytes, err := w.storage.ReadFile(ctx, metadataRel)
	if err != nil {
		return 0, nil, cerror.Trace(err)
	}

	var m tableMetadata
	if err := json.Unmarshal(metadataBytes, &m); err != nil {
		return 0, nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	metadataLocation, err := joinLocation(w.cfg.WarehouseLocation, metadataRel)
	if err != nil {
		return 0, nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	m.SelfMetadataLocation = metadataLocation
	return version, &m, nil
}

func (w *TableWriter) loadLatestTableMetadata(ctx context.Context, metadataDirRel string) (int, *tableMetadata, error) {
	var (
		maxVersion int
		maxRel     string
	)
	err := w.storage.WalkDir(ctx, &storage.WalkOption{SubDir: metadataDirRel, ObjPrefix: "v"}, func(relPath string, _ int64) error {
		if version, ok := metadataVersionFromFileName(path.Base(relPath)); ok {
			if version > maxVersion {
				maxVersion = version
				maxRel = relPath
			}
		}
		return nil
	})
	if err != nil {
		return 0, nil, cerror.Trace(err)
	}
	if maxVersion == 0 || maxRel == "" {
		return 0, nil, nil
	}

	metadataBytes, err := w.storage.ReadFile(ctx, maxRel)
	if err != nil {
		return 0, nil, cerror.Trace(err)
	}

	var m tableMetadata
	if err := json.Unmarshal(metadataBytes, &m); err != nil {
		return 0, nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	metadataLocation, err := joinLocation(w.cfg.WarehouseLocation, maxRel)
	if err != nil {
		return 0, nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	m.SelfMetadataLocation = metadataLocation
	return maxVersion, &m, nil
}

func (w *TableWriter) loadTableMetadataFromGlue(ctx context.Context, schemaName, tableName string) (int, *tableMetadata, error) {
	metadataLocation, found, err := w.getGlueMetadataLocation(ctx, schemaName, tableName)
	if err != nil {
		return 0, nil, err
	}
	if !found || strings.TrimSpace(metadataLocation) == "" {
		return 0, nil, nil
	}

	metadataRel, err := w.relativePathFromLocation(metadataLocation)
	if err != nil {
		return 0, nil, err
	}
	version, ok := metadataVersionFromFileName(path.Base(metadataRel))
	if !ok {
		return 0, nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("invalid iceberg metadata filename")
	}

	metadataBytes, err := w.storage.ReadFile(ctx, metadataRel)
	if err != nil {
		return 0, nil, cerror.Trace(err)
	}

	var m tableMetadata
	if err := json.Unmarshal(metadataBytes, &m); err != nil {
		return 0, nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	m.SelfMetadataLocation = metadataLocation
	return version, &m, nil
}

func (w *TableWriter) relativePathFromLocation(location string) (string, error) {
	if w == nil || w.cfg == nil {
		return "", cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg config is nil")
	}
	base, err := url.Parse(w.cfg.WarehouseLocation)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	target, err := url.Parse(location)
	if err != nil {
		return "", cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}

	if !strings.EqualFold(base.Scheme, target.Scheme) || !strings.EqualFold(base.Host, target.Host) {
		return "", cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg location is outside warehouse")
	}

	basePath := path.Clean(base.Path)
	if basePath == "." {
		basePath = ""
	}
	targetPath := path.Clean(target.Path)

	if basePath == "" || basePath == "/" {
		rel := strings.TrimPrefix(targetPath, "/")
		if rel == "" {
			return "", cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg location is warehouse root")
		}
		return rel, nil
	}

	prefix := strings.TrimSuffix(basePath, "/") + "/"
	if !strings.HasPrefix(targetPath, prefix) {
		return "", cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg location is outside warehouse")
	}
	rel := strings.TrimPrefix(targetPath, prefix)
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" {
		return "", cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg location is warehouse root")
	}
	return rel, nil
}

func metadataVersionFromFileName(fileName string) (int, bool) {
	s := strings.TrimSpace(fileName)
	if !strings.HasPrefix(s, "v") || !strings.HasSuffix(s, ".metadata.json") {
		return 0, false
	}
	s = strings.TrimPrefix(s, "v")
	s = strings.TrimSuffix(s, ".metadata.json")
	if s == "" {
		return 0, false
	}
	version, err := strconv.Atoi(s)
	if err != nil || version <= 0 {
		return 0, false
	}
	return version, true
}

type tableMetadata struct {
	FormatVersion      int             `json:"format-version"`
	TableUUID          string          `json:"table-uuid"`
	Location           string          `json:"location"`
	LastSequenceNumber int64           `json:"last-sequence-number"`
	LastUpdatedMs      int64           `json:"last-updated-ms"`
	LastColumnID       int             `json:"last-column-id"`
	Schemas            []icebergSchema `json:"schemas"`
	CurrentSchemaID    int             `json:"current-schema-id"`

	PartitionSpecs  []partitionSpec `json:"partition-specs"`
	DefaultSpecID   int             `json:"default-spec-id"`
	LastPartitionID int             `json:"last-partition-id"`

	Properties map[string]string `json:"properties,omitempty"`

	CurrentSnapshotID *int64             `json:"current-snapshot-id,omitempty"`
	Snapshots         []snapshot         `json:"snapshots,omitempty"`
	SnapshotLog       []snapshotLogEntry `json:"snapshot-log,omitempty"`
	MetadataLog       []metadataLogEntry `json:"metadata-log,omitempty"`

	SortOrders          []sortOrder                  `json:"sort-orders"`
	DefaultSortOrderID  int64                        `json:"default-sort-order-id"`
	Refs                map[string]snapshotReference `json:"refs,omitempty"`
	Statistics          []any                        `json:"statistics,omitempty"`
	PartitionStatistics []any                        `json:"partition-statistics,omitempty"`

	SelfMetadataLocation string `json:"-"`
}

type icebergSchema struct {
	Type               string         `json:"type"`
	SchemaID           int            `json:"schema-id"`
	IdentifierFieldIDs []int          `json:"identifier-field-ids,omitempty"`
	Fields             []icebergField `json:"fields"`
}

type icebergField struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Required bool   `json:"required"`
	Type     string `json:"type"`
}

type partitionSpec struct {
	SpecID int   `json:"spec-id"`
	Fields []any `json:"fields"`
}

type sortOrder struct {
	OrderID int64 `json:"order-id"`
	Fields  []any `json:"fields"`
}

type snapshotReference struct {
	SnapshotID         int64  `json:"snapshot-id"`
	Type               string `json:"type"`
	MinSnapshotsToKeep *int32 `json:"min-snapshots-to-keep,omitempty"`
	MaxSnapshotAgeMs   *int64 `json:"max-snapshot-age-ms,omitempty"`
	MaxRefAgeMs        *int64 `json:"max-ref-age-ms,omitempty"`
}

type snapshot struct {
	SnapshotID       int64             `json:"snapshot-id"`
	ParentSnapshotID *int64            `json:"parent-snapshot-id,omitempty"`
	SequenceNumber   int64             `json:"sequence-number"`
	TimestampMs      int64             `json:"timestamp-ms"`
	ManifestList     string            `json:"manifest-list"`
	Summary          map[string]string `json:"summary,omitempty"`
	SchemaID         *int              `json:"schema-id,omitempty"`
}

type snapshotLogEntry struct {
	TimestampMs int64 `json:"timestamp-ms"`
	SnapshotID  int64 `json:"snapshot-id"`
}

type metadataLogEntry struct {
	TimestampMs  int64  `json:"timestamp-ms"`
	MetadataFile string `json:"metadata-file"`
}

func newTableMetadata(location string, lastColumnID int, schema *icebergSchema) *tableMetadata {
	return &tableMetadata{
		FormatVersion:      icebergFormatVersion,
		TableUUID:          uuid.NewString(),
		Location:           location,
		LastSequenceNumber: 0,
		LastUpdatedMs:      time.Now().UnixMilli(),
		LastColumnID:       lastColumnID,
		Schemas:            []icebergSchema{*schema},
		CurrentSchemaID:    schema.SchemaID,
		PartitionSpecs:     []partitionSpec{{SpecID: 0, Fields: []any{}}},
		DefaultSpecID:      0,
		LastPartitionID:    icebergLastPartitionIDUnpartitioned,
		Properties: map[string]string{
			"write.format.default":            "parquet",
			"write.parquet.compression-codec": "zstd",
		},
		SortOrders:         []sortOrder{{OrderID: icebergSortOrderID, Fields: []any{}}},
		DefaultSortOrderID: icebergSortOrderID,
		Snapshots:          []snapshot{},
		SnapshotLog:        []snapshotLogEntry{},
		MetadataLog:        []metadataLogEntry{},
	}
}

func buildChangelogSchemas(tableInfo *common.TableInfo, emitMetadata bool) (string, *icebergSchema, int, error) {
	fields := make([]icebergField, 0, len(tableInfo.GetColumns())+3)
	avroFields := make([]map[string]any, 0, len(tableInfo.GetColumns())+3)
	lastID := 0

	requiredColumnIDs := make(map[int64]struct{})
	for _, id := range tableInfo.GetOrderedHandleKeyColumnIDs() {
		requiredColumnIDs[id] = struct{}{}
	}

	if emitMetadata {
		meta := []struct {
			id          int
			name        string
			icebergType string
		}{
			{id: icebergMetaIDOp, name: "_tidb_op", icebergType: "string"},
			{id: icebergMetaIDCommitTs, name: "_tidb_commit_ts", icebergType: "long"},
			{id: icebergMetaIDCommitTime, name: "_tidb_commit_time", icebergType: "timestamp"},
		}
		for _, m := range meta {
			fields = append(fields, icebergField{ID: m.id, Name: m.name, Required: false, Type: m.icebergType})
			avroFields = append(avroFields, map[string]any{
				"name":     m.name,
				"type":     []any{"null", icebergTypeToAvroType(m.icebergType)},
				"default":  nil,
				"field-id": m.id,
			})
			if m.id > lastID {
				lastID = m.id
			}
		}
	}

	for _, colInfo := range tableInfo.GetColumns() {
		if colInfo == nil || colInfo.IsVirtualGenerated() {
			continue
		}
		colID := int(colInfo.ID)
		_, required := requiredColumnIDs[colInfo.ID]
		mapped := mapTiDBFieldType(&colInfo.FieldType)
		fields = append(fields, icebergField{ID: colID, Name: colInfo.Name.O, Required: required, Type: mapped.icebergType})
		avroField := map[string]any{
			"name":     colInfo.Name.O,
			"field-id": colID,
		}
		if required {
			avroField["type"] = icebergTypeToAvroType(mapped.icebergType)
		} else {
			avroField["type"] = []any{"null", icebergTypeToAvroType(mapped.icebergType)}
			avroField["default"] = nil
		}
		avroFields = append(avroFields, avroField)
		if colID > lastID {
			lastID = colID
		}
	}

	avroSchemaObj := map[string]any{
		"type":      "record",
		"name":      "row",
		"namespace": "com.pingcap.ticdc.iceberg",
		"fields":    avroFields,
	}
	avroSchemaBytes, err := json.Marshal(avroSchemaObj)
	if err != nil {
		return "", nil, 0, err
	}

	schema := &icebergSchema{
		Type:     "struct",
		SchemaID: 0,
		Fields:   fields,
	}
	return string(avroSchemaBytes), schema, lastID, nil
}

func applySchemaIfChanged(m *tableMetadata, desired *icebergSchema, lastColumnID int) bool {
	if m == nil || desired == nil {
		return false
	}

	current := m.currentSchema()
	if current != nil && icebergSchemaEqual(current, desired) {
		m.LastColumnID = maxInt(m.LastColumnID, lastColumnID)
		return false
	}

	nextID := nextSchemaID(m)
	newSchema := *desired
	newSchema.SchemaID = nextID
	m.Schemas = append(m.Schemas, newSchema)
	m.CurrentSchemaID = newSchema.SchemaID
	m.LastColumnID = maxInt(m.LastColumnID, lastColumnID)
	return true
}

func (m *tableMetadata) currentSchema() *icebergSchema {
	for i := range m.Schemas {
		if m.Schemas[i].SchemaID == m.CurrentSchemaID {
			return &m.Schemas[i]
		}
	}
	if len(m.Schemas) == 0 {
		return nil
	}
	return &m.Schemas[len(m.Schemas)-1]
}

func (m *tableMetadata) currentSnapshot() *snapshot {
	if m == nil || m.CurrentSnapshotID == nil {
		return nil
	}
	for i := range m.Snapshots {
		if m.Snapshots[i].SnapshotID == *m.CurrentSnapshotID {
			return &m.Snapshots[i]
		}
	}
	return nil
}

func ensureMainBranchRef(m *tableMetadata) {
	if m == nil || m.CurrentSnapshotID == nil {
		return
	}
	if m.Refs == nil {
		m.Refs = make(map[string]snapshotReference)
	}
	m.Refs["main"] = snapshotReference{
		SnapshotID: *m.CurrentSnapshotID,
		Type:       "branch",
	}
}

func icebergSchemaEqual(a, b *icebergSchema) bool {
	if a == nil || b == nil {
		return a == b
	}
	if len(a.IdentifierFieldIDs) != len(b.IdentifierFieldIDs) {
		return false
	}
	for i := range a.IdentifierFieldIDs {
		if a.IdentifierFieldIDs[i] != b.IdentifierFieldIDs[i] {
			return false
		}
	}
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		if a.Fields[i] != b.Fields[i] {
			return false
		}
	}
	return true
}

func nextSchemaID(m *tableMetadata) int {
	maxID := m.CurrentSchemaID
	for i := range m.Schemas {
		if m.Schemas[i].SchemaID > maxID {
			maxID = m.Schemas[i].SchemaID
		}
	}
	return maxID + 1
}

func nextSnapshotID(now time.Time, m *tableMetadata) int64 {
	snapshotID := now.UnixMilli()
	if m == nil {
		return snapshotID
	}
	lastID := int64(0)
	if m.CurrentSnapshotID != nil {
		lastID = *m.CurrentSnapshotID
	} else {
		for i := range m.Snapshots {
			if m.Snapshots[i].SnapshotID > lastID {
				lastID = m.Snapshots[i].SnapshotID
			}
		}
	}
	if snapshotID <= lastID {
		snapshotID = lastID + 1
	}
	return snapshotID
}

func icebergColumnNames(tableInfo *common.TableInfo) []string {
	if tableInfo == nil {
		return nil
	}
	cols := tableInfo.GetColumns()
	if len(cols) == 0 {
		return nil
	}
	names := make([]string, 0, len(cols))
	for _, colInfo := range cols {
		if colInfo == nil || colInfo.IsVirtualGenerated() {
			continue
		}
		names = append(names, colInfo.Name.O)
	}
	return names
}

func maxInt(a, b int) int {
	if a >= b {
		return a
	}
	return b
}
