// Copyright 2023 PingCAP, Inc.
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

package cloudstorage

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	// 3 is the length of "CDC", and the file number contains
	// at least 6 digits (e.g. CDC000001.csv).
	minFileNamePrefixLen                 = 3 + config.MinFileIndexWidth
	defaultTableAcrossNodesIndexFileName = "meta/CDC_%s.index"
	defaultIndexFileName                 = "meta/CDC.index"

	// The following constants are used to generate file paths.
	schemaFileNameFormat = "schema_%d_%010d.json"
	// The database schema is stored in the following path:
	// <schema>/meta/schema_{tableVersion}_{checksum}.json
	dbSchemaPrefix = "%s/meta/"
	// The table-level schema file is stored in the following path:
	// <schema>/<table>/meta/schema_{tableVersion}_{checksum}.json
	tableMetaPrefix = "%s/%s/meta/"
	// When use-table-id-as-path, schema is omitted: <table_id>/meta/...
	tableIDPrefix = "%s/meta/"
)

var schemaRE = regexp.MustCompile(`meta/schema_\d+_\d{10}\.json$`)

// IsSchemaFile reports whether path matches a schema file under a meta
// directory.
func IsSchemaFile(path string) bool {
	return schemaRE.MatchString(path)
}

// mustParseSchemaFileName returns tableVersion and checksum encoded in a schema
// file name. Invalid names panic.
func mustParseSchemaFileName(path string) (uint64, uint32) {
	reportErr := func(reason string, fields ...zap.Field) {
		fields = append([]zap.Field{
			zap.String("schemaPath", path),
			zap.String("reason", reason),
		}, fields...)
		log.Panic("failed to parse schema file name", fields...)
	}

	// For <schema>/<table>/meta/schema_{tableVersion}_{checksum}.json, the parts
	// should be ["<schema>/<table>/meta/schema", "{tableVersion}", "{checksum}.json"].
	parts := strings.Split(path, "_")
	if len(parts) < 3 {
		reportErr("invalid path format")
	}

	checksum := strings.TrimSuffix(parts[len(parts)-1], ".json")
	tableChecksum, err := strconv.ParseUint(checksum, 10, 32)
	if err != nil {
		reportErr("invalid checksum", zap.Error(err))
	}
	version := parts[len(parts)-2]
	tableVersion, err := strconv.ParseUint(version, 10, 64)
	if err != nil {
		reportErr("invalid table version", zap.Error(err))
	}
	return tableVersion, uint32(tableChecksum)
}

// generateSchemaFilePath returns the schema file path.
// When table is empty, output is <schema>/meta/schema_<version>_<checksum>.json.
// When omitSchema is true, output is <table-path>/meta/schema_<version>_<checksum>.json.
// Otherwise output is <schema>/<table>/meta/schema_<version>_<checksum>.json.
func generateSchemaFilePath(
	schema, table string, tableVersion uint64, checksum uint32, omitSchema bool,
) string {
	name := fmt.Sprintf(schemaFileNameFormat, tableVersion, checksum)
	if omitSchema {
		return path.Join(fmt.Sprintf(tableIDPrefix, table), name)
	}
	if table == "" {
		return path.Join(fmt.Sprintf(dbSchemaPrefix, schema), name)
	}
	return path.Join(fmt.Sprintf(tableMetaPrefix, schema, table), name)
}

// generateTablePath returns either the table name or physical table ID path
// segment according to useTableIDAsPath.
func generateTablePath(tableName string, tableID int64, useTableIDAsPath bool) string {
	if useTableIDAsPath {
		return fmt.Sprintf("%d", tableID)
	}
	return tableName
}

// generateDataFileName returns CDC<index><extension> or
// CDC_<dispatcherID>_<index><extension>. fileIndexWidth controls zero padding.
func generateDataFileName(enableTableAcrossNodes bool, dispatcherID string, index uint64, extension string, fileIndexWidth int) string {
	indexFmt := "%0" + strconv.Itoa(fileIndexWidth) + "d"
	if enableTableAcrossNodes {
		return fmt.Sprintf("CDC_%s_"+indexFmt+"%s", dispatcherID, index, extension)
	}
	return fmt.Sprintf("CDC"+indexFmt+"%s", index, extension)
}

func generateIndexFileName(enableTableAcrossNodes bool, dispatcherID string) string {
	if enableTableAcrossNodes {
		return fmt.Sprintf(defaultTableAcrossNodesIndexFileName, dispatcherID)
	}
	return defaultIndexFileName
}

type indexWithDate struct {
	// index is the current max data file sequence in one date bucket.
	index uint64
	// currDate is the latest date bucket requested by GenerateDataFilePath.
	// prevDate is the previously used bucket to detect rollover and reset index.
	currDate, prevDate string
}

// VersionedTableName is used to wrap TableNameWithPhysicTableID with a version.
type VersionedTableName struct {
	// Because we need to generate different file paths for different
	// tables, we need to use the physical table ID instead of the
	// logical table ID.(Especially when the table is a partitioned table).
	TableNameWithPhysicTableID commonType.TableName
	// TableInfoVersion is the schema file version carried with incoming DML.
	// Source:
	// 1. DDL finishedTs for schema-changing DDLs.
	// 2. Checkpoint/startTs during dispatcher recover/move.
	// Usage:
	// 1. CheckOrWriteSchema uses it to detect whether incoming DML is older than
	//    the latest schema version already stored.
	// 2. If no exact schema file exists but an equivalent schema checksum exists,
	//    storage sink may reuse an older/newer existing schema version as the
	//    output directory version via versionMap.
	TableInfoVersion uint64
	// DispatcherID identifies the dispatcher producing this table stream.
	// It participates in index/data file names when table-across-nodes is enabled.
	DispatcherID commonType.DispatcherID
}

// FilePathGenerator generates schema, data, and index paths for one storage
// sink.
type FilePathGenerator struct {
	changefeedID commonType.ChangeFeedID
	extension    string
	config       *Config
	pdClock      pdutil.Clock
	storage      storeapi.Storage
	// fileIndex caches the last emitted data file index for one
	// VersionedTableName and date bucket.
	fileIndex map[VersionedTableName]*indexWithDate

	// versionMap maps an input VersionedTableName to the effective table version
	// used in output directory:
	// <schema>/<table>/<effectiveTableVersion>/...
	// This can differ from TableInfoVersion when reusing an existing schema file
	// with the same checksum.
	versionMap map[VersionedTableName]uint64
}

// NewFilePathGenerator creates a FilePathGenerator for one changefeed storage
// sink. extension is the data file suffix used by GenerateDataFilePath.
func NewFilePathGenerator(
	changefeedID commonType.ChangeFeedID,
	config *Config,
	storage storeapi.Storage,
	extension string,
) *FilePathGenerator {
	pdClock := appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock)
	return &FilePathGenerator{
		changefeedID: changefeedID,
		config:       config,
		extension:    extension,
		storage:      storage,
		pdClock:      pdClock,
		fileIndex:    make(map[VersionedTableName]*indexWithDate),
		versionMap:   make(map[VersionedTableName]uint64),
	}
}

// CheckOrWriteSchema ensures the schema file for table/tableInfo exists.
// It records the effective table version used by later data/index path
// generation. It returns true when storage already contains a newer schema file
// with the same checksum and a version greater than table.TableInfoVersion.
func (f *FilePathGenerator) CheckOrWriteSchema(
	ctx context.Context,
	table VersionedTableName,
	tableInfo *commonType.TableInfo,
) (bool, error) {
	if _, ok := f.versionMap[table]; ok {
		return false, nil
	}

	keyspace := f.changefeedID.Keyspace()
	changefeed := f.changefeedID.Name()

	event := &commonEvent.DDLEvent{
		SchemaName: tableInfo.GetTargetSchemaName(),
		TableName:  tableInfo.GetTargetTableName(),
		TableInfo:  tableInfo,
		FinishedTs: table.TableInfoVersion,
	}
	var schemaFile SchemaFile
	schemaFile.Build(event, f.config.OutputColumnID)

	// Case 1: point check if the schema file exists.
	schemaFilePath := schemaFile.Path(f.config.UseTableIDAsPath, table.TableNameWithPhysicTableID.TableID)
	exist, err := f.storage.FileExists(ctx, schemaFilePath)
	if err != nil {
		return false, err
	}
	if exist {
		f.versionMap[table] = table.TableInfoVersion
		return false, nil
	}
	// walk the table meta path to find the last schema file
	_, checksum := mustParseSchemaFileName(schemaFilePath)
	schemaFileCount := 0
	lastVersion := uint64(0)
	tablePathPart := generateTablePath(schemaFile.Table, table.TableNameWithPhysicTableID.TableID, f.config.UseTableIDAsPath)
	subDir := fmt.Sprintf(tableMetaPrefix, schemaFile.Schema, tablePathPart)
	if f.config.UseTableIDAsPath {
		subDir = fmt.Sprintf(tableIDPrefix, tablePathPart)
	}
	checksumSuffix := fmt.Sprintf("%010d.json", checksum)
	hasNewerSchemaVersion := false
	err = f.storage.WalkDir(ctx, &storeapi.WalkOption{
		SubDir:    subDir, /* use subDir to prevent walk the whole storage */
		ObjPrefix: "schema_",
	}, func(path string, _ int64) error {
		schemaFileCount++
		if !strings.HasSuffix(path, checksumSuffix) {
			return nil
		}
		version, _ := mustParseSchemaFileName(path)
		if version > table.TableInfoVersion {
			hasNewerSchemaVersion = true
		}
		if version > lastVersion {
			lastVersion = version
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	if hasNewerSchemaVersion {
		return true, nil
	}

	// Case 2: the table meta path is not empty.
	if schemaFileCount != 0 && lastVersion != 0 {
		log.Info("schema file with exact version not found, using latest available",
			zap.String("keyspace", keyspace),
			zap.String("changefeedID", changefeed),
			zap.Any("versionedTableName", table),
			zap.Uint64("tableVersion", lastVersion),
			zap.Uint32("checksum", checksum))
		// record the last version of the schema file.
		// we don't need to write schema file to external storage again.
		f.versionMap[table] = lastVersion
		return false, nil
	}

	// Case 3: the table meta path is empty, which happens when:
	//  a. the table is existed before changefeed started. We need to write schema file to external storage.
	//  b. the schema file is deleted by the consumer. We write schema file to external storage too.
	if schemaFileCount != 0 && lastVersion == 0 {
		log.Warn("no schema file found in a non-empty meta path",
			zap.String("keyspace", keyspace),
			zap.String("changefeedID", changefeed),
			zap.Any("versionedTableName", table),
			zap.Uint32("checksum", checksum))
	}
	encodedSchemaFile := schemaFile.Marshal()
	f.versionMap[table] = table.TableInfoVersion
	return false, f.storage.WriteFile(ctx, schemaFilePath, encodedSchemaFile)
}

// SetClock sets the clock used by GenerateDateStr. It is used by tests.
func (f *FilePathGenerator) SetClock(pdClock pdutil.Clock) {
	f.pdClock = pdClock
}

// GenerateDateStr returns the current date bucket from the date-separator
// config: "2006", "2006-01", "2006-01-02", or "" when disabled.
func (f *FilePathGenerator) GenerateDateStr() string {
	var dateStr string

	currTime := f.pdClock.CurrentTime()
	// Note: `dateStr` is formatted using local TZ.
	switch f.config.DateSeparator {
	case config.DateSeparatorYear.String():
		dateStr = currTime.Format("2006")
	case config.DateSeparatorMonth.String():
		dateStr = currTime.Format("2006-01")
	case config.DateSeparatorDay.String():
		dateStr = currTime.Format("2006-01-02")
	default:
	}

	return dateStr
}

// GenerateIndexFilePath returns the index file path for tbl and date.
// The directory uses the effective table version recorded by CheckOrWriteSchema.
// Output is <data-dir>/meta/CDC.index or
// <data-dir>/meta/CDC_<dispatcherID>.index.
func (f *FilePathGenerator) GenerateIndexFilePath(tbl VersionedTableName, date string) string {
	dir := f.generateDataDirPath(tbl, date)
	return path.Join(dir, generateIndexFileName(f.config.EnableTableAcrossNodes, tbl.DispatcherID.String()))
}

// GenerateDataFilePath returns the next available data file path for tbl and
// date. It updates the in-memory file index and may read storage to avoid
// colliding with existing files. Storage errors are returned.
func (f *FilePathGenerator) GenerateDataFilePath(
	ctx context.Context, tbl VersionedTableName, date string,
) (string, error) {
	dir := f.generateDataDirPath(tbl, date)
	loadedIndexFile := false
	idx, ok := f.fileIndex[tbl]
	if !ok {
		fileIdx, err := f.getFileIdxFromIndexFile(ctx, tbl, date)
		if err != nil {
			return "", err
		}
		f.fileIndex[tbl] = &indexWithDate{
			prevDate: date,
			currDate: date,
			index:    fileIdx,
		}
		loadedIndexFile = true
	}
	if ok {
		idx.currDate = date
	}
	// if date changed, reset the counter
	if f.fileIndex[tbl].prevDate != f.fileIndex[tbl].currDate {
		f.fileIndex[tbl].prevDate = f.fileIndex[tbl].currDate
		f.fileIndex[tbl].index = 0
	}
	triedIndexResync := loadedIndexFile
	// A local file index can lag behind existing data files after sink restart or
	// dispatcher ownership transfer. If the local cache collides with an existing
	// data file, reload the index file once before falling back to consecutive
	// probes for the index-stale recovery path.
	for {
		f.fileIndex[tbl].index++
		name := generateDataFileName(f.config.EnableTableAcrossNodes, tbl.DispatcherID.String(), f.fileIndex[tbl].index, f.extension, f.config.FileIndexWidth)
		dataFile := path.Join(dir, name)
		exist, err := f.storage.FileExists(ctx, dataFile)
		if err != nil {
			return "", err
		}
		if !exist {
			return dataFile, nil
		}
		if !triedIndexResync {
			fileIdx, err := f.getFileIdxFromIndexFile(ctx, tbl, date)
			if err != nil {
				return "", err
			}
			triedIndexResync = true
			if fileIdx >= f.fileIndex[tbl].index {
				f.fileIndex[tbl].index = fileIdx
				continue
			}
		}
		if loadedIndexFile {
			log.Warn("the data file exists and the index file is stale",
				zap.String("keyspace", f.changefeedID.Keyspace()),
				zap.String("changefeedID", f.changefeedID.Name()),
				zap.Any("versionedTableName", tbl),
				zap.String("dataFile", dataFile))
			loadedIndexFile = false
		}
	}
}

// generateDataDirPath returns the data directory for tbl and date.
// The table version comes from versionMap, which is set by CheckOrWriteSchema.
// Output matches DMLPathKey.generateDMLDataDirPath.
func (f *FilePathGenerator) generateDataDirPath(tbl VersionedTableName, date string) string {
	tableVersion := f.versionMap[tbl]
	if f.config.UseTableIDAsPath {
		tableIDPathPart := generateTablePath(
			tbl.TableNameWithPhysicTableID.Table,
			tbl.TableNameWithPhysicTableID.TableID,
			true,
		)
		return DMLPathKey{
			SchemaPathKey: SchemaPathKey{
				Schema:       tableIDPathPart,
				TableVersion: tableVersion,
			},
			UseTableIDAsPath: true,
			TableID:          tbl.TableNameWithPhysicTableID.TableID,
			Date:             date,
		}.generateDMLDataDirPath()
	}

	tablePathPart := generateTablePath(
		tbl.TableNameWithPhysicTableID.Table,
		tbl.TableNameWithPhysicTableID.TableID,
		false,
	)
	var partitionNum int64
	if f.config.EnablePartitionSeparator && tbl.TableNameWithPhysicTableID.IsPartition {
		partitionNum = tbl.TableNameWithPhysicTableID.TableID
	}
	return DMLPathKey{
		SchemaPathKey: SchemaPathKey{
			Schema:       tbl.TableNameWithPhysicTableID.Schema,
			Table:        tablePathPart,
			TableVersion: tableVersion,
		},
		PartitionNum: partitionNum,
		Date:         date,
	}.generateDMLDataDirPath()
}

// getFileIdxFromIndexFile returns the max file index recorded in the index
// file for tbl/date. Missing index files return 0.
func (f *FilePathGenerator) getFileIdxFromIndexFile(
	ctx context.Context, tbl VersionedTableName, date string,
) (uint64, error) {
	indexFile := f.GenerateIndexFilePath(tbl, date)
	exist, err := f.storage.FileExists(ctx, indexFile)
	if err != nil {
		return 0, err
	}
	if !exist {
		return 0, nil
	}

	data, err := f.storage.ReadFile(ctx, indexFile)
	if err != nil {
		return 0, err
	}
	fileName := strings.TrimSuffix(string(data), "\n")
	fileIndex, err := ParseFileIndexFromFileName(fileName, f.extension)
	if err != nil {
		return 0, err
	}
	return fileIndex.Idx, nil
}

// ParseFileIndexFromFileName returns the dispatcher ID and numeric index
// encoded in a data file name. extension must match the file suffix.
func ParseFileIndexFromFileName(fileName string, extension string) (FileIndex, error) {
	if len(fileName) < minFileNamePrefixLen+len(extension) ||
		!strings.HasPrefix(fileName, "CDC") ||
		!strings.HasSuffix(fileName, extension) {
		return FileIndex{}, errors.ErrStorageSinkInvalidFileName.GenWithStack(
			"filename in storage sink is invalid: %q", fileName)
	}

	// CDC[_{dispatcherID}_]{num}.fileExtension
	name := strings.TrimSuffix(strings.TrimPrefix(fileName, "CDC"), extension)
	dispatcherID := ""
	idxStr := name
	if strings.HasPrefix(name, "_") {
		idxSep := strings.LastIndex(name, "_")
		if idxSep <= 1 {
			return FileIndex{}, errors.ErrStorageSinkInvalidFileName.GenWithStack(
				"cannot match dml path pattern for %q", fileName)
		}
		dispatcherID = name[1:idxSep]
		idxStr = name[idxSep+1:]
	}
	idx, err := strconv.ParseUint(idxStr, 10, 64)
	if err != nil {
		return FileIndex{}, errors.WrapError(
			errors.ErrStorageSinkInvalidFileName, err, "cannot match dml path pattern for %q", fileName)
	}
	return FileIndex{
		FileIndexKey: FileIndexKey{
			DispatcherID:           dispatcherID,
			EnableTableAcrossNodes: dispatcherID != "",
		},
		Idx: idx,
	}, nil
}

var dateSeparatorDayRegexp *regexp.Regexp

// RemoveExpiredFiles removes expired files from external storage.
func RemoveExpiredFiles(
	ctx context.Context,
	_ commonType.ChangeFeedID,
	storage storeapi.Storage,
	cfg *Config,
	checkpointTs uint64,
) error {
	if cfg.DateSeparator != config.DateSeparatorDay.String() {
		return nil
	}
	if dateSeparatorDayRegexp == nil {
		dateSeparatorDayRegexp = regexp.MustCompile(config.DateSeparatorDay.GetPattern())
	}

	ttl := time.Duration(cfg.FileExpirationDays) * time.Hour * 24
	currTime := oracle.GetTimeFromTS(checkpointTs).Add(-ttl)
	// Note: `expiredDate` is formatted using local TZ.
	expiredDate := currTime.Format("2006-01-02")

	err := util.RemoveFilesIf(ctx, storage, func(path string) bool {
		// the path is like: <schema>/<table>/<tableVersion>/<partitionID>/<date>/CDC_{dispatcher}_{num}.extension
		// or <schema>/<table>/<tableVersion>/<partitionID>/<date>/CDC{num}.extension
		match := dateSeparatorDayRegexp.FindString(path)
		return match != "" && match < expiredDate
	}, nil)
	return err
}

// RemoveEmptyDirs removes empty directories from external storage.
func RemoveEmptyDirs(
	ctx context.Context,
	id commonType.ChangeFeedID,
	target string,
) error {
	err := filepath.Walk(target, func(path string, info fs.FileInfo, err error) error {
		if os.IsNotExist(err) || path == target || info == nil {
			// if path not exists, we should return nil to continue.
			return nil
		}
		if err != nil {
			return err
		}

		if !info.IsDir() {
			return nil
		}

		files, err := os.ReadDir(path)
		if err == nil && len(files) == 0 {
			// Keep best-effort cleanup semantics: ignore remove failures.
			_ = os.Remove(path) // #nosec G122
			return filepath.SkipDir
		}
		return nil
	})

	return err
}
