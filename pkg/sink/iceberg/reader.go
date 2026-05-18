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

package iceberg

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/linkedin/goavro/v2"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// Column is a simplified Iceberg column definition exposed to readers.
type Column struct {
	ID               int
	Name             string
	Type             string
	Required         bool
	OriginalTableCol *cloudstorage.TableCol
}

// TableIdentifier describes an Iceberg table discovered under a Hadoop warehouse.
type TableIdentifier struct {
	SchemaName            string
	TableName             string
	LatestMetadataVersion int
}

// TableVersion describes one Iceberg metadata version plus the data files added by that snapshot.
type TableVersion struct {
	SchemaName          string
	TableName           string
	MetadataVersion     int
	CommittedResolvedTs uint64
	Columns             []Column
	DataFiles           []string
}

// ListHadoopTables lists Iceberg tables by scanning the warehouse layout produced by the TiCDC Iceberg sink.
// Both Hadoop and Glue catalogs use the same on-storage metadata/data layout.
func ListHadoopTables(
	ctx context.Context,
	cfg *Config,
	extStorage storage.ExternalStorage,
) ([]TableIdentifier, error) {
	if cfg == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg config is nil")
	}
	if extStorage == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("external storage is nil")
	}
	if !supportsReaderCatalog(cfg.Catalog) {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("storage consumer only supports iceberg hadoop or glue catalog")
	}

	namespacePrefix := escapePathSegment(cfg.Namespace)
	tables := make(map[string]TableIdentifier)
	err := extStorage.WalkDir(ctx, &storage.WalkOption{SubDir: namespacePrefix}, func(relPath string, _ int64) error {
		schemaName, tableName, ok := parseHadoopTableMetadataPath(cfg.Namespace, relPath)
		if !ok {
			return nil
		}

		latestVersion := 0
		fileName := path.Base(relPath)
		switch {
		case fileName == versionHintFile:
			hintBytes, err := extStorage.ReadFile(ctx, relPath)
			if err != nil {
				return cerror.Trace(err)
			}
			latestVersion, err = strconv.Atoi(strings.TrimSpace(string(hintBytes)))
			if err != nil {
				return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
			}
		default:
			version, ok := metadataVersionFromFileName(fileName)
			if !ok {
				return nil
			}
			latestVersion = version
		}

		key := schemaName + "\x00" + tableName
		current := tables[key]
		if latestVersion > current.LatestMetadataVersion {
			tables[key] = TableIdentifier{
				SchemaName:            schemaName,
				TableName:             tableName,
				LatestMetadataVersion: latestVersion,
			}
		}
		return nil
	})
	if err != nil {
		return nil, cerror.Trace(err)
	}

	result := make([]TableIdentifier, 0, len(tables))
	for _, table := range tables {
		if table.LatestMetadataVersion == 0 {
			continue
		}
		result = append(result, table)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].SchemaName != result[j].SchemaName {
			return result[i].SchemaName < result[j].SchemaName
		}
		return result[i].TableName < result[j].TableName
	})
	return result, nil
}

// LoadTableVersion reads one Iceberg metadata version and returns the data files added by that snapshot.
func LoadTableVersion(
	ctx context.Context,
	cfg *Config,
	extStorage storage.ExternalStorage,
	schemaName string,
	tableName string,
	metadataVersion int,
) (*TableVersion, error) {
	if cfg == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg config is nil")
	}
	if extStorage == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("external storage is nil")
	}
	if !supportsReaderCatalog(cfg.Catalog) {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("storage consumer only supports iceberg hadoop or glue catalog")
	}
	if metadataVersion <= 0 {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("invalid iceberg metadata version")
	}

	tableWriter := NewTableWriter(cfg, extStorage)
	metadataRel := path.Join(
		tableWriter.tableRootPath(schemaName, tableName),
		metadataDirName,
		fmt.Sprintf("v%d.metadata.json", metadataVersion),
	)
	metadataBytes, err := extStorage.ReadFile(ctx, metadataRel)
	if err != nil {
		return nil, cerror.Trace(err)
	}

	var metadata tableMetadata
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	metadataLocation, err := joinLocation(cfg.WarehouseLocation, metadataRel)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	metadata.SelfMetadataLocation = metadataLocation

	currentSchema := metadata.currentSchema()
	if currentSchema == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg metadata schema is empty")
	}

	version := &TableVersion{
		SchemaName:      schemaName,
		TableName:       tableName,
		MetadataVersion: metadataVersion,
		Columns:         make([]Column, 0, len(currentSchema.Fields)),
	}
	for _, field := range currentSchema.Fields {
		if isIcebergMetadataColumn(field.Name) {
			continue
		}
		originalTableCol, err := decodeOriginalTableCol(field.Doc)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		version.Columns = append(version.Columns, Column{
			ID:               field.ID,
			Name:             field.Name,
			Type:             field.Type,
			Required:         field.Required,
			OriginalTableCol: originalTableCol,
		})
	}

	currentSnapshot := metadata.currentSnapshot()
	if currentSnapshot == nil {
		return version, nil
	}
	if metadataVersion > 1 {
		prevMetadata, err := loadMetadataFile(ctx, cfg, extStorage, schemaName, tableName, metadataVersion-1)
		if err != nil {
			return nil, err
		}
		if prevMetadata != nil &&
			prevMetadata.CurrentSnapshotID != nil &&
			metadata.CurrentSnapshotID != nil &&
			*prevMetadata.CurrentSnapshotID == *metadata.CurrentSnapshotID {
			return version, nil
		}
	}

	if len(currentSnapshot.Summary) != 0 {
		rawResolvedTs := strings.TrimSpace(currentSnapshot.Summary[summaryKeyCommittedResolvedTs])
		if rawResolvedTs != "" {
			resolvedTs, err := strconv.ParseUint(rawResolvedTs, 10, 64)
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
			}
			version.CommittedResolvedTs = resolvedTs
		}
	}

	dataFiles, err := readAddedDataFiles(ctx, extStorage, tableWriter, currentSnapshot)
	if err != nil {
		return nil, err
	}
	version.DataFiles = dataFiles
	return version, nil
}

func loadMetadataFile(
	ctx context.Context,
	cfg *Config,
	extStorage storage.ExternalStorage,
	schemaName string,
	tableName string,
	metadataVersion int,
) (*tableMetadata, error) {
	if metadataVersion <= 0 {
		return nil, nil
	}

	tableWriter := NewTableWriter(cfg, extStorage)
	metadataRel := path.Join(
		tableWriter.tableRootPath(schemaName, tableName),
		metadataDirName,
		fmt.Sprintf("v%d.metadata.json", metadataVersion),
	)
	metadataBytes, err := extStorage.ReadFile(ctx, metadataRel)
	if err != nil {
		return nil, cerror.Trace(err)
	}

	var metadata tableMetadata
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	metadataLocation, err := joinLocation(cfg.WarehouseLocation, metadataRel)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	metadata.SelfMetadataLocation = metadataLocation
	return &metadata, nil
}

// DecodeParquetFile decodes one parquet changelog file written by the Iceberg storage sink.
func DecodeParquetFile(
	ctx context.Context,
	extStorage storage.ExternalStorage,
	relPath string,
) ([]ChangeRow, error) {
	if extStorage == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("external storage is nil")
	}
	if strings.TrimSpace(relPath) == "" {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("parquet path is empty")
	}

	content, err := extStorage.ReadFile(ctx, relPath)
	if err != nil {
		return nil, cerror.Trace(err)
	}

	reader, err := file.NewParquetReader(
		bytes.NewReader(content),
		file.WithReadProps(parquet.NewReaderProperties(memory.DefaultAllocator)),
	)
	if err != nil {
		return nil, cerror.Trace(err)
	}
	defer reader.Close()

	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, cerror.Trace(err)
	}

	table, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return nil, cerror.Trace(err)
	}
	defer table.Release()

	fields := table.Schema().Fields()
	tableReader := array.NewTableReader(table, 0)
	defer tableReader.Release()

	rows := make([]ChangeRow, 0, table.NumRows())
	for tableReader.Next() {
		record := tableReader.Record()
		columns := record.Columns()
		for rowIdx := 0; rowIdx < int(record.NumRows()); rowIdx++ {
			row := ChangeRow{
				Columns: make(map[string]*string),
			}
			for colIdx, field := range fields {
				value, err := arrowValueToString(columns[colIdx], rowIdx)
				if err != nil {
					return nil, err
				}
				switch field.Name {
				case "_tidb_op":
					if value != nil {
						row.Op = *value
					}
				case "_tidb_commit_ts":
					if value != nil {
						row.CommitTs = *value
					}
				case "_tidb_commit_time":
					if value != nil {
						row.CommitTime = *value
					}
				case "_tidb_table_version":
					if value != nil {
						row.TableVersion = *value
					}
				case "_tidb_row_identity":
					if value != nil {
						row.RowIdentity = *value
					}
				case "_tidb_old_row_identity":
					row.OldRowIdentity = value
				case "_tidb_identity_kind":
					if value != nil {
						row.IdentityKind = *value
					}
				default:
					row.Columns[field.Name] = value
				}
			}
			rows = append(rows, row)
		}
	}
	if err := tableReader.Err(); err != nil {
		return nil, cerror.Trace(err)
	}
	return rows, nil
}

func parseHadoopTableMetadataPath(namespace, relPath string) (string, string, bool) {
	cleaned := path.Clean(relPath)
	parts := strings.Split(cleaned, "/")
	if len(parts) != 5 {
		return "", "", false
	}
	if parts[0] != escapePathSegment(namespace) || parts[3] != metadataDirName {
		return "", "", false
	}
	fileName := parts[4]
	if fileName != versionHintFile {
		if _, ok := metadataVersionFromFileName(fileName); !ok {
			return "", "", false
		}
	}

	schemaName, err := url.PathUnescape(parts[1])
	if err != nil {
		return "", "", false
	}
	tableName, err := url.PathUnescape(parts[2])
	if err != nil {
		return "", "", false
	}
	return schemaName, tableName, true
}

func isIcebergMetadataColumn(name string) bool {
	switch name {
	case "_tidb_op", "_tidb_commit_ts", "_tidb_commit_time",
		"_tidb_table_version", "_tidb_row_identity", "_tidb_old_row_identity", "_tidb_identity_kind":
		return true
	default:
		return false
	}
}

func supportsReaderCatalog(catalog CatalogType) bool {
	switch catalog {
	case CatalogHadoop, CatalogGlue:
		return true
	default:
		return false
	}
}

func readAddedDataFiles(
	ctx context.Context,
	extStorage storage.ExternalStorage,
	tableWriter *TableWriter,
	currentSnapshot *snapshot,
) ([]string, error) {
	if currentSnapshot == nil || strings.TrimSpace(currentSnapshot.ManifestList) == "" {
		return nil, nil
	}

	manifestListRel, err := tableWriter.relativePathFromLocation(currentSnapshot.ManifestList)
	if err != nil {
		return nil, err
	}
	manifestListBytes, err := extStorage.ReadFile(ctx, manifestListRel)
	if err != nil {
		return nil, cerror.Trace(err)
	}
	manifestListRecords, err := readOCFRecords(manifestListBytes)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{})
	dataFiles := make([]string, 0, len(manifestListRecords))
	for _, manifest := range manifestListRecords {
		content, ok := toInt32(manifest["content"])
		if !ok || content != manifestContentData {
			continue
		}

		manifestPath, ok := manifest["manifest_path"].(string)
		if !ok || strings.TrimSpace(manifestPath) == "" {
			continue
		}
		manifestRel, err := tableWriter.relativePathFromLocation(manifestPath)
		if err != nil {
			return nil, err
		}
		manifestBytes, err := extStorage.ReadFile(ctx, manifestRel)
		if err != nil {
			return nil, cerror.Trace(err)
		}
		manifestRecords, err := readOCFRecords(manifestBytes)
		if err != nil {
			return nil, err
		}
		for _, entry := range manifestRecords {
			status, ok := toInt32(entry["status"])
			if !ok || status != manifestStatusAdded {
				continue
			}

			dataFileRecord, ok := entry["data_file"].(map[string]any)
			if !ok {
				continue
			}
			filePath, ok := dataFileRecord["file_path"].(string)
			if !ok || strings.TrimSpace(filePath) == "" {
				continue
			}
			relPath, err := tableWriter.relativePathFromLocation(filePath)
			if err != nil {
				return nil, err
			}
			if _, exists := seen[relPath]; exists {
				continue
			}
			seen[relPath] = struct{}{}
			dataFiles = append(dataFiles, relPath)
		}
	}
	sort.Strings(dataFiles)
	return dataFiles, nil
}

func readOCFRecords(data []byte) ([]map[string]any, error) {
	reader, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		return nil, cerror.Trace(err)
	}

	records := make([]map[string]any, 0)
	for reader.Scan() {
		record, err := reader.Read()
		if err != nil {
			return nil, cerror.Trace(err)
		}
		recordMap, ok := record.(map[string]any)
		if !ok {
			return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("unexpected avro record type")
		}
		records = append(records, recordMap)
	}
	return records, nil
}

func toInt32(v any) (int32, bool) {
	switch x := v.(type) {
	case int32:
		return x, true
	case int64:
		return int32(x), true
	case int:
		return int32(x), true
	default:
		return 0, false
	}
}

func arrowValueToString(arr arrow.Array, rowIdx int) (*string, error) {
	if arr.IsNull(rowIdx) {
		return nil, nil
	}

	var value string
	switch typed := arr.(type) {
	case *array.String:
		value = typed.Value(rowIdx)
	case *array.Int32:
		value = strconv.FormatInt(int64(typed.Value(rowIdx)), 10)
	case *array.Int64:
		value = strconv.FormatInt(typed.Value(rowIdx), 10)
	case *array.Float32:
		value = strconv.FormatFloat(float64(typed.Value(rowIdx)), 'f', -1, 32)
	case *array.Float64:
		value = strconv.FormatFloat(typed.Value(rowIdx), 'f', -1, 64)
	case *array.Binary:
		value = base64.StdEncoding.EncodeToString(typed.Value(rowIdx))
	case *array.Decimal128:
		dt, ok := typed.DataType().(*arrow.Decimal128Type)
		if !ok {
			return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("unexpected decimal parquet type")
		}
		value = typed.Value(rowIdx).ToString(dt.Scale)
	case *array.Date32:
		seconds := int64(typed.Value(rowIdx)) * 24 * 60 * 60
		value = time.Unix(seconds, 0).UTC().Format("2006-01-02")
	case *array.Timestamp:
		value = time.UnixMicro(int64(typed.Value(rowIdx))).UTC().Format(time.RFC3339Nano)
	default:
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs(
			fmt.Sprintf("unsupported parquet column type: %s", arr.DataType().Name()),
		)
	}
	return &value, nil
}
