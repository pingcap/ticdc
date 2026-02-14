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

	defaultPathStateTTL             = 24 * time.Hour
	defaultPathStateCleanupInterval = 10 * time.Minute
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
	schema, table string, tableVersion uint64, checksum uint32,
) string {
	if schema == "" || tableVersion == 0 {
		log.Panic("invalid schema or tableVersion",
			zap.String("schema", schema), zap.String("table", table), zap.Uint64("tableVersion", tableVersion))
	}

	var dir string
	if table == "" {
		// Generate db schema file path.
		dir = fmt.Sprintf(dbSchemaPrefix, schema)
	} else {
		// Generate table schema file path.
		dir = fmt.Sprintf(tableSchemaPrefix, schema, table)
	}
	name := fmt.Sprintf(schemaFileNameFormat, tableVersion, checksum)
	return path.Join(dir, name)
}

func generateDataFileName(enableTableAcrossNodes bool, dispatcherID string, index uint64, extension string, fileIndexWidth int) string {
	indexFmt := "%0" + strconv.Itoa(fileIndexWidth) + "d"
	if enableTableAcrossNodes {
		return fmt.Sprintf("CDC_%s_"+indexFmt+"%s", dispatcherID, index, extension)
	}
	return fmt.Sprintf("CDC"+indexFmt+"%s", index, extension)
}

type indexWithDate struct {
	index              uint64
	currDate, prevDate string
}

type tablePathStateKey struct {
	table        commonType.TableName
	dispatcherID commonType.DispatcherID
}

type tablePathState struct {
	schemaVersion uint64
	fileIndex     indexWithDate
	indexReady    bool
	lastAccess    time.Time
}

// VersionedTableName is used to wrap TableNameWithPhysicTableID with a version.
type VersionedTableName struct {
	// Because we need to generate different file paths for different
	// tables, we need to use the physical table ID instead of the
	// logical table ID.(Especially when the table is a partitioned table).
	TableNameWithPhysicTableID commonType.TableName
	// TableInfoVersion is consistent with the version of TableInfo recorded in
	// schema storage. It can either be finished ts of a DDL event,
	// or be the checkpoint ts when processor is restarted.
	TableInfoVersion uint64
	DispatcherID     commonType.DispatcherID
}

// FilePathGenerator is used to generate data file path and index file path.
type FilePathGenerator struct {
	changefeedID commonType.ChangeFeedID
	extension    string
	config       *Config
	pdClock      pdutil.Clock
	storage      storage.ExternalStorage
	pathState    map[tablePathStateKey]*tablePathState

	stateTTL             time.Duration
	stateCleanupInterval time.Duration
	lastStateCleanupTime time.Time
}

// Path state principles:
//  1. State is keyed by (table, dispatcher), so different dispatchers do not share file index.
//  2. schemaVersion change resets file index state to avoid mixing different schema versions.
//  3. state cleanup is lazy and interval-based to keep hot path overhead low.

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
		pathState:    make(map[tablePathStateKey]*tablePathState),

		stateTTL:             defaultPathStateTTL,
		stateCleanupInterval: defaultPathStateCleanupInterval,
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
	now := f.currentTime()
	f.cleanupExpiredPathState(now)
	state := f.ensurePathState(table, now)
	if state.schemaVersion == table.TableInfoVersion {
		return false, nil
	}

	var def TableDefinition
	def.FromTableInfo(tableInfo.GetSchemaName(), tableInfo.GetTableName(), tableInfo, table.TableInfoVersion, f.config.OutputColumnID)
	if !def.IsTableSchema() {
		// only check schema for table
		log.Error("invalid table schema",
			zap.String("keyspace", f.changefeedID.Keyspace()),
			zap.Stringer("changefeedID", f.changefeedID.ID()),
			zap.Any("versionedTableName", table),
			zap.Any("tableInfo", tableInfo))
		return false, errors.ErrInternalCheckFailed.GenWithStackByArgs("invalid table schema in FilePathGenerator")
	}

	// Case 1: point check if the schema file exists.
	tblSchemaFile, err := def.GenerateSchemaFilePath()
	if err != nil {
		return false, errors.Trace(err)
	}
	exist, err := f.storage.FileExists(ctx, tblSchemaFile)
	if err != nil {
		return false, errors.Trace(err)
	}
	if exist {
		f.updateSchemaVersion(state, table.TableInfoVersion, now)
		return false, nil
	}

	// walk the table meta path to find the last schema file
	_, checksum := mustParseSchemaName(tblSchemaFile)
	schemaFileCnt := 0
	lastVersion := uint64(0)
	subDir := fmt.Sprintf(tableSchemaPrefix, def.Schema, def.Table)
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
				zap.String("keyspace", f.changefeedID.Keyspace()),
				zap.Stringer("changefeedID", f.changefeedID.ID()),
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
		return false, errors.Trace(err)
	}
	if hasNewerSchemaVersion {
		return true, nil
	}

	// Case 2: the table meta path is not empty.
	if schemaFileCnt != 0 && lastVersion != 0 {
		log.Info("table schema file with exact version not found, using latest available",
			zap.String("keyspace", f.changefeedID.Keyspace()),
			zap.Stringer("changefeedID", f.changefeedID.ID()),
			zap.Any("versionedTableName", table),
			zap.Uint64("tableVersion", lastVersion),
			zap.Uint32("checksum", checksum))
		// record the last version of the table schema file.
		// we don't need to write schema file to external storage again.
		f.updateSchemaVersion(state, lastVersion, now)
		return false, nil
	}

	// Case 3: the table meta path is empty, which happens when:
	//  a. the table is existed before changefeed started. We need to write schema file to external storage.
	//  b. the schema file is deleted by the consumer. We write schema file to external storage too.
	if schemaFileCnt != 0 && lastVersion == 0 {
		log.Warn("no table schema file found in an non-empty meta path",
			zap.String("keyspace", f.changefeedID.Keyspace()),
			zap.Stringer("changefeedID", f.changefeedID.ID()),
			zap.Any("versionedTableName", table),
			zap.Uint32("checksum", checksum))
	}
	encodedDetail, err := def.MarshalWithQuery()
	if err != nil {
		return false, errors.Trace(err)
	}
	if err := f.storage.WriteFile(ctx, tblSchemaFile, encodedDetail); err != nil {
		return false, errors.Trace(err)
	}
	f.updateSchemaVersion(state, table.TableInfoVersion, now)
	return false, nil
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
func (f *FilePathGenerator) GenerateIndexFilePath(tbl VersionedTableName, date string) string {
	dir := f.generateDataDirPath(tbl, date)
	name := defaultIndexFileName
	if f.config.EnableTableAcrossNodes {
		name = fmt.Sprintf(defaultTableAcrossNodesIndexFileName, tbl.DispatcherID.String())
	}
	return path.Join(dir, name)
}

// GenerateDataFilePath generates a canonical path for data file.
func (f *FilePathGenerator) GenerateDataFilePath(
	ctx context.Context, tbl VersionedTableName, date string,
) (string, error) {
	dir := f.generateDataDirPath(tbl, date)
	name, err := f.generateDataFileName(ctx, tbl, date)
	if err != nil {
		return "", err
	}
	return path.Join(dir, name), nil
}

func (f *FilePathGenerator) generateDataDirPath(tbl VersionedTableName, date string) string {
	var elems []string
	now := f.currentTime()
	state := f.ensurePathState(tbl, now)
	version := state.schemaVersion
	if version == 0 {
		version = tbl.TableInfoVersion
		f.updateSchemaVersion(state, version, now)
	}

	elems = append(elems, tbl.TableNameWithPhysicTableID.Schema)
	elems = append(elems, tbl.TableNameWithPhysicTableID.Table)
	elems = append(elems, fmt.Sprintf("%d", version))

	if f.config.EnablePartitionSeparator && tbl.TableNameWithPhysicTableID.IsPartition {
		elems = append(elems, fmt.Sprintf("%d", tbl.TableNameWithPhysicTableID.TableID))
	}

	if len(date) != 0 {
		elems = append(elems, date)
	}

	return path.Join(elems...)
}

func (f *FilePathGenerator) generateDataFileName(
	ctx context.Context, tbl VersionedTableName, date string,
) (string, error) {
	now := f.currentTime()
	f.cleanupExpiredPathState(now)
	state := f.ensurePathState(tbl, now)
	if !state.indexReady {
		fileIdx, err := f.getFileIdxFromIndexFile(ctx, tbl, date)
		if err != nil {
			return "", err
		}
		state.fileIndex = indexWithDate{
			prevDate: date,
			currDate: date,
			index:    fileIdx,
		}
		state.indexReady = true
	} else {
		state.fileIndex.currDate = date
	}

	// if date changed, reset the counter
	if state.fileIndex.prevDate != state.fileIndex.currDate {
		state.fileIndex.prevDate = state.fileIndex.currDate
		state.fileIndex.index = 0
	}
	// Invariant: file index increases monotonically within the same (table, dispatcher, date) state.
	state.fileIndex.index++
	state.lastAccess = now
	return generateDataFileName(f.config.EnableTableAcrossNodes, tbl.DispatcherID.String(), state.fileIndex.index, f.extension, f.config.FileIndexWidth), nil
}

func (f *FilePathGenerator) getFileIdxFromIndexFile(
	ctx context.Context, tbl VersionedTableName, date string,
) (uint64, error) {
	indexFile := f.GenerateIndexFilePath(tbl, date)
	exist, err := f.storage.FileExists(ctx, indexFile)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !exist {
		return 0, nil
	}

	data, err := f.storage.ReadFile(ctx, indexFile)
	if err != nil {
		return 0, errors.Trace(err)
	}
	fileName := strings.TrimSuffix(string(data), "\n")
	return FetchIndexFromFileName(fileName, f.extension)
}

func (f *FilePathGenerator) setPathStateCleanupConfig(ttl, interval time.Duration) {
	if ttl > 0 {
		f.stateTTL = ttl
	}
	if interval > 0 {
		f.stateCleanupInterval = interval
	}
}

func (f *FilePathGenerator) pathStateCount() int {
	return len(f.pathState)
}

func (f *FilePathGenerator) currentSchemaVersion(tbl VersionedTableName) uint64 {
	state := f.pathState[f.pathStateKey(tbl)]
	if state == nil {
		return 0
	}
	return state.schemaVersion
}

func (f *FilePathGenerator) setCurrentSchemaVersion(tbl VersionedTableName, version uint64) {
	now := f.currentTime()
	state := f.ensurePathState(tbl, now)
	f.updateSchemaVersion(state, version, now)
}

func (f *FilePathGenerator) currentTime() time.Time {
	if f.pdClock == nil {
		return time.Now()
	}
	return f.pdClock.CurrentTime()
}

func (f *FilePathGenerator) pathStateKey(tbl VersionedTableName) tablePathStateKey {
	return tablePathStateKey{
		table:        tbl.TableNameWithPhysicTableID,
		dispatcherID: tbl.DispatcherID,
	}
}

func (f *FilePathGenerator) ensurePathState(tbl VersionedTableName, now time.Time) *tablePathState {
	key := f.pathStateKey(tbl)
	state := f.pathState[key]
	if state == nil {
		state = &tablePathState{}
		f.pathState[key] = state
	}
	state.lastAccess = now
	return state
}

func (f *FilePathGenerator) updateSchemaVersion(state *tablePathState, version uint64, now time.Time) {
	if state.schemaVersion != version {
		state.schemaVersion = version
		state.fileIndex = indexWithDate{}
		state.indexReady = false
	}
	state.lastAccess = now
}

func (f *FilePathGenerator) cleanupExpiredPathState(now time.Time) {
	if f.stateTTL <= 0 || f.stateCleanupInterval <= 0 {
		return
	}
	if !f.lastStateCleanupTime.IsZero() && now.Sub(f.lastStateCleanupTime) < f.stateCleanupInterval {
		return
	}
	f.lastStateCleanupTime = now
	for key, state := range f.pathState {
		if state == nil {
			delete(f.pathState, key)
			continue
		}
		if now.Sub(state.lastAccess) >= f.stateTTL {
			delete(f.pathState, key)
		}
	}
}

func FetchIndexFromFileName(fileName string, extension string) (uint64, error) {
	if len(fileName) < minFileNamePrefixLen+len(extension) ||
		!strings.HasPrefix(fileName, "CDC") ||
		!strings.HasSuffix(fileName, extension) {
		return 0, errors.WrapError(errors.ErrStorageSinkInvalidFileName,
			fmt.Errorf("'%s' is a invalid file name", fileName))
	}

	fileName = strings.TrimSuffix(fileName, extension)
	indexPart := strings.TrimPrefix(fileName, "CDC")

	if strings.HasPrefix(indexPart, "_") {
		trimmed := strings.TrimPrefix(indexPart, "_")
		separatorIdx := strings.LastIndex(trimmed, "_")
		if separatorIdx <= 0 || separatorIdx >= len(trimmed)-1 {
			return 0, errors.WrapError(errors.ErrStorageSinkInvalidFileName,
				fmt.Errorf("'%s' is a invalid file name", fileName))
		}
		indexPart = trimmed[separatorIdx+1:]
	}

	if len(indexPart) < config.MinFileIndexWidth || !isNumberString(indexPart) {
		return 0, errors.WrapError(errors.ErrStorageSinkInvalidFileName,
			fmt.Errorf("'%s' is a invalid file name", fileName))
	}

	return strconv.ParseUint(indexPart, 10, 64)
}

func isNumberString(value string) bool {
	if value == "" {
		return false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
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
					zap.Stringer("changeFeedID", id.ID()),
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
