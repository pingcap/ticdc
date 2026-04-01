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
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/hash"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
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
	// The table schema is stored in the following path:
	// <schema>/<table>/meta/schema_{tableVersion}_{checksum}.json
	tableSchemaPrefix = "%s/%s/meta/"
	// When use-table-id-as-path, schema is omitted: <table_id>/meta/...
	tableIdPrefix = "%s/meta/"
)

var schemaRE = regexp.MustCompile(`meta/schema_\d+_\d{10}\.json$`)

// IsSchemaFile checks whether the file is a schema file.
func IsSchemaFile(path string) bool {
	return schemaRE.MatchString(path)
}

// mustParseSchemaName parses the version from the schema file name.
func mustParseSchemaName(path string) (uint64, uint32) {
	reportErr := func(err error) {
		log.Panic("failed to parse schema file name",
			zap.String("schemaPath", path),
			zap.Any("error", err))
	}

	// For <schema>/<table>/meta/schema_{tableVersion}_{checksum}.json, the parts
	// should be ["<schema>/<table>/meta/schema", "{tableVersion}", "{checksum}.json"].
	parts := strings.Split(path, "_")
	if len(parts) < 3 {
		reportErr(errors.New("invalid path format"))
	}

	checksum := strings.TrimSuffix(parts[len(parts)-1], ".json")
	tableChecksum, err := strconv.ParseUint(checksum, 10, 64)
	if err != nil {
		reportErr(err)
	}
	version := parts[len(parts)-2]
	tableVersion, err := strconv.ParseUint(version, 10, 64)
	if err != nil {
		reportErr(err)
	}
	return tableVersion, uint32(tableChecksum)
}

func generateSchemaFilePath(
	schema, table string, tableVersion uint64, checksum uint32, omitSchema bool,
) (string, error) {
	if schema == "" || tableVersion == 0 {
		return "", errors.ErrInternalCheckFailed.GenWithStackByArgs(
			fmt.Sprintf("invalid schema or tableVersion, schema=%q table=%q tableVersion=%d",
				schema, table, tableVersion),
		)
	}

	var dir string
	if omitSchema {
		if table == "" {
			return "", errors.ErrInternalCheckFailed.GenWithStackByArgs(
				"table cannot be empty when 'use-table-id-as-path' is true",
			)
		}
		// use-table-id-as-path: omit schema, path is <table_id>/meta/
		dir = fmt.Sprintf(tableIdPrefix, table)
	} else {
		if table == "" {
			// Generate db schema file path.
			dir = fmt.Sprintf(dbSchemaPrefix, schema)
		} else {
			// Generate table schema file path.
			dir = fmt.Sprintf(tableSchemaPrefix, schema, table)
		}
	}
	name := fmt.Sprintf(schemaFileNameFormat, tableVersion, checksum)
	return path.Join(dir, name), nil
}

func generateTablePath(tableName string, tableID int64, useTableIDAsPath bool) (string, error) {
	if useTableIDAsPath {
		if tableID <= 0 {
			return "", errors.ErrInternalCheckFailed.GenWithStackByArgs(
				"invalid table id for table-id path",
			)
		}
		return fmt.Sprintf("%d", tableID), nil
	}
	return tableName, nil
}

func generateDataFileName(enableTableAcrossNodes bool, dispatcherID string, index uint64, extension string, fileIndexWidth int) string {
	indexFmt := "%0" + strconv.Itoa(fileIndexWidth) + "d"
	if enableTableAcrossNodes {
		return fmt.Sprintf("CDC_%s_"+indexFmt+"%s", dispatcherID, index, extension)
	}
	return fmt.Sprintf("CDC"+indexFmt+"%s", index, extension)
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
	// TableInfoVersion is the table schema version carried with incoming DML.
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

// FilePathGenerator is used to generate data file path and index file path.
type FilePathGenerator struct {
	changefeedID commonType.ChangeFeedID
	extension    string
	config       *Config
	pdClock      pdutil.Clock
	storage      storage.ExternalStorage
	// fileIndex caches the last emitted data file index for one
	// VersionedTableName and date bucket.
	fileIndex map[VersionedTableName]*indexWithDate

	hasher *hash.PositionInertia
	// versionMap maps an input VersionedTableName to the effective table version
	// used in output directory:
	// <schema>/<table>/<effectiveTableVersion>/...
	// This can differ from TableInfoVersion when reusing an existing schema file
	// with the same checksum.
	versionMap map[VersionedTableName]uint64
}

// NewFilePathGenerator creates a FilePathGenerator.
func NewFilePathGenerator(
	changefeedID commonType.ChangeFeedID,
	config *Config,
	storage storage.ExternalStorage,
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
		hasher:       hash.NewPositionInertia(),
		versionMap:   make(map[VersionedTableName]uint64),
	}
}

// CheckOrWriteSchema checks whether the schema file exists in the storage and
// write scheme.json if necessary.
// It returns true if there is a newer schema version in storage than the passed table version.
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

	var def TableDefinition
	def.FromTableInfo(
		tableInfo.GetTargetSchemaName(),
		tableInfo.GetTargetTableName(),
		tableInfo,
		table.TableInfoVersion,
		f.config.OutputColumnID,
	)
	if !def.IsTableSchema() {
		// only check schema for table
		log.Error("invalid table schema",
			zap.String("keyspace", keyspace),
			zap.String("changefeedID", changefeed),
			zap.Any("versionedTableName", table),
			zap.Any("tableInfo", tableInfo))
		return false, errors.ErrInternalCheckFailed.GenWithStackByArgs("invalid table schema in FilePathGenerator")
	}

	// Case 1: point check if the schema file exists.
	tblSchemaFile, err := def.GenerateSchemaFilePath(f.config.UseTableIDAsPath, table.TableNameWithPhysicTableID.TableID)
	if err != nil {
		return false, err
	}
	exist, err := f.storage.FileExists(ctx, tblSchemaFile)
	if err != nil {
		return false, err
	}
	if exist {
		f.versionMap[table] = table.TableInfoVersion
		return false, nil
	}
	// walk the table meta path to find the last schema file
	_, checksum := mustParseSchemaName(tblSchemaFile)
	schemaFileCnt := 0
	lastVersion := uint64(0)
	tablePathPart, err := generateTablePath(def.Table, table.TableNameWithPhysicTableID.TableID, f.config.UseTableIDAsPath)
	if err != nil {
		return false, err
	}
	var subDir string
	if f.config.UseTableIDAsPath {
		subDir = fmt.Sprintf(tableIdPrefix, tablePathPart)
	} else {
		subDir = fmt.Sprintf(tableSchemaPrefix, def.Schema, tablePathPart)
	}
	checksumSuffix := fmt.Sprintf("%010d.json", checksum)
	hasNewerSchemaVersion := false
	err = f.storage.WalkDir(ctx, &storage.WalkOption{
		SubDir:    subDir, /* use subDir to prevent walk the whole storage */
		ObjPrefix: "schema_",
	}, func(path string, _ int64) error {
		schemaFileCnt++
		if !strings.HasSuffix(path, checksumSuffix) {
			return nil
		}
		version, parsedChecksum := mustParseSchemaName(path)
		if parsedChecksum != checksum {
			log.Error("invalid schema file name",
				zap.String("keyspace", keyspace),
				zap.String("changefeedID", changefeed),
				zap.String("path", path), zap.Any("checksum", checksum))
			errMsg := fmt.Sprintf("invalid schema filename in storage sink, "+
				"expected checksum: %d, actual checksum: %d", checksum, parsedChecksum)
			return errors.ErrInternalCheckFailed.GenWithStackByArgs(errMsg)
		}
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
	if schemaFileCnt != 0 && lastVersion != 0 {
		log.Info("table schema file with exact version not found, using latest available",
			zap.String("keyspace", keyspace),
			zap.String("changefeedID", changefeed),
			zap.Any("versionedTableName", table),
			zap.Uint64("tableVersion", lastVersion),
			zap.Uint32("checksum", checksum))
		// record the last version of the table schema file.
		// we don't need to write schema file to external storage again.
		f.versionMap[table] = lastVersion
		return false, nil
	}

	// Case 3: the table meta path is empty, which happens when:
	//  a. the table is existed before changefeed started. We need to write schema file to external storage.
	//  b. the schema file is deleted by the consumer. We write schema file to external storage too.
	if schemaFileCnt != 0 && lastVersion == 0 {
		log.Warn("no table schema file found in an non-empty meta path",
			zap.String("keyspace", keyspace),
			zap.String("changefeedID", changefeed),
			zap.Any("versionedTableName", table),
			zap.Uint32("checksum", checksum))
	}
	encodedDetail, err := def.MarshalWithQuery()
	if err != nil {
		return false, err
	}
	f.versionMap[table] = table.TableInfoVersion
	return false, f.storage.WriteFile(ctx, tblSchemaFile, encodedDetail)
}

// SetClock is used for unit test
func (f *FilePathGenerator) SetClock(pdClock pdutil.Clock) {
	f.pdClock = pdClock
}

// GenerateDateStr generates a date string base on current time
// and the date-separator configuration item.
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

// GenerateIndexFilePath generates a canonical path for index file.
func (f *FilePathGenerator) GenerateIndexFilePath(tbl VersionedTableName, date string) (string, error) {
	dir, err := f.generateDataDirPath(tbl, date)
	if err != nil {
		return "", err
	}
	name := defaultIndexFileName
	if f.config.EnableTableAcrossNodes {
		name = fmt.Sprintf(defaultTableAcrossNodesIndexFileName, tbl.DispatcherID.String())
	}
	return path.Join(dir, name), nil
}

// GenerateDataFilePath generates a canonical path for data file.
func (f *FilePathGenerator) GenerateDataFilePath(
	ctx context.Context, tbl VersionedTableName, date string,
) (string, error) {
	dir, err := f.generateDataDirPath(tbl, date)
	if err != nil {
		return "", err
	}
	newIndexFile := false
	if idx, ok := f.fileIndex[tbl]; !ok {
		fileIdx, err := f.getFileIdxFromIndexFile(ctx, tbl, date)
		if err != nil {
			return "", err
		}
		f.fileIndex[tbl] = &indexWithDate{
			prevDate: date,
			currDate: date,
			index:    fileIdx,
		}
		newIndexFile = true
	} else {
		idx.currDate = date
	}
	// if date changed, reset the counter
	if f.fileIndex[tbl].prevDate != f.fileIndex[tbl].currDate {
		f.fileIndex[tbl].prevDate = f.fileIndex[tbl].currDate
		f.fileIndex[tbl].index = 0
	}
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
	if newIndexFile {
		log.Warn("the data file exists and the index file is stale",
			zap.String("keyspace", f.changefeedID.Keyspace()),
			zap.String("changefeedID", f.changefeedID.Name()),
			zap.Any("versionedTableName", tbl),
			zap.String("dataFile", dataFile))
	}
	// if the file already exists, which means the fileIndex is stale,
	// we need to delete the file index in memory and re-generate the file path with the updated file index until we find a non-existing file path.
	delete(f.fileIndex, tbl)
	return f.GenerateDataFilePath(ctx, tbl, date)
}

func (f *FilePathGenerator) generateDataDirPath(tbl VersionedTableName, date string) (string, error) {
	var elems []string

	tableVersion, ok := f.versionMap[tbl]
	if !ok || tableVersion == 0 {
		return "", errors.ErrInternalCheckFailed.GenWithStackByArgs(
			"table schema version is not initialized",
		)
	}

	if f.config.UseTableIDAsPath {
		tablePathPart, err := generateTablePath(
			tbl.TableNameWithPhysicTableID.Table,
			tbl.TableNameWithPhysicTableID.TableID,
			true,
		)
		if err != nil {
			return "", err
		}
		elems = append(elems, tablePathPart)
	} else {
		elems = append(elems, tbl.TableNameWithPhysicTableID.Schema)
		tablePathPart, err := generateTablePath(
			tbl.TableNameWithPhysicTableID.Table,
			tbl.TableNameWithPhysicTableID.TableID,
			false,
		)
		if err != nil {
			return "", err
		}
		elems = append(elems, tablePathPart)
	}
	elems = append(elems, fmt.Sprintf("%d", tableVersion))

	if f.config.EnablePartitionSeparator && tbl.TableNameWithPhysicTableID.IsPartition && !f.config.UseTableIDAsPath {
		elems = append(elems, fmt.Sprintf("%d", tbl.TableNameWithPhysicTableID.TableID))
	}

	if len(date) != 0 {
		elems = append(elems, date)
	}

	return path.Join(elems...), nil
}

func (f *FilePathGenerator) getFileIdxFromIndexFile(
	ctx context.Context, tbl VersionedTableName, date string,
) (uint64, error) {
	indexFile, err := f.GenerateIndexFilePath(tbl, date)
	if err != nil {
		return 0, err
	}
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
	return FetchIndexFromFileName(fileName, f.extension)
}

func FetchIndexFromFileName(fileName string, extension string) (uint64, error) {
	if len(fileName) < minFileNamePrefixLen+len(extension) ||
		!strings.HasPrefix(fileName, "CDC") ||
		!strings.HasSuffix(fileName, extension) {
		return 0, errors.WrapError(errors.ErrStorageSinkInvalidFileName,
			fmt.Errorf("'%s' is a invalid file name", fileName))
	}

	// CDC[_{dispatcherID}_]{num}.fileExtension
	pathRE, err := regexp.Compile(`CDC(?:_(\w+)_)?(\d+).\w+`)
	if err != nil {
		return 0, err
	}

	matches := pathRE.FindStringSubmatch(fileName)
	if len(matches) != 3 {
		return 0, fmt.Errorf("cannot match dml path pattern for %s", fileName)
	}
	return strconv.ParseUint(matches[2], 10, 64)
}

var dateSeparatorDayRegexp *regexp.Regexp

// RemoveExpiredFiles removes expired files from external storage.
func RemoveExpiredFiles(
	ctx context.Context,
	_ commonType.ChangeFeedID,
	storage storage.ExternalStorage,
	cfg *Config,
	checkpointTs uint64,
) (uint64, error) {
	if cfg.DateSeparator != config.DateSeparatorDay.String() {
		return 0, nil
	}
	if dateSeparatorDayRegexp == nil {
		dateSeparatorDayRegexp = regexp.MustCompile(config.DateSeparatorDay.GetPattern())
	}

	ttl := time.Duration(cfg.FileExpirationDays) * time.Hour * 24
	currTime := oracle.GetTimeFromTS(checkpointTs).Add(-ttl)
	// Note: `expiredDate` is formatted using local TZ.
	expiredDate := currTime.Format("2006-01-02")

	cnt := uint64(0)
	err := util.RemoveFilesIf(ctx, storage, func(path string) bool {
		// the path is like: <schema>/<table>/<tableVersion>/<partitionID>/<date>/CDC_{dispatcher}_{num}.extension
		// or <schema>/<table>/<tableVersion>/<partitionID>/<date>/CDC{num}.extension
		match := dateSeparatorDayRegexp.FindString(path)
		if match != "" && match < expiredDate {
			cnt++
			return true
		}
		return false
	}, nil)
	return cnt, err
}

// RemoveEmptyDirs removes empty directories from external storage.
func RemoveEmptyDirs(
	ctx context.Context,
	id commonType.ChangeFeedID,
	target string,
) (uint64, error) {
	cnt := uint64(0)
	err := filepath.Walk(target, func(path string, info fs.FileInfo, err error) error {
		if os.IsNotExist(err) || path == target || info == nil {
			// if path not exists, we should return nil to continue.
			return nil
		}
		if err != nil {
			return err
		}
		if info.IsDir() {
			files, err := os.ReadDir(path)
			if err == nil && len(files) == 0 {
				log.Debug("Deleting empty directory",
					zap.String("keyspace", id.Keyspace()),
					zap.String("changeFeedID", id.Name()),
					zap.String("path", path))
				os.Remove(path)
				cnt++
				return filepath.SkipDir
			}
		}
		return nil
	})

	return cnt, err
}
