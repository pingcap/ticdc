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

package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"sync"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/recorder"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/tidb/br/pkg/storage"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type (
	fileIndexRange  map[cloudstorage.FileIndexKey]indexRange
	fileIndexKeyMap map[cloudstorage.FileIndexKey]uint64
)

type indexRange struct {
	start uint64
	end   uint64
}

func updateTableDMLIdxMap(
	tableDMLIdxMap map[cloudstorage.DmlPathKey]fileIndexKeyMap,
	dmlkey cloudstorage.DmlPathKey,
	fileIdx *cloudstorage.FileIndex,
) {
	m, ok := tableDMLIdxMap[dmlkey]
	if !ok {
		tableDMLIdxMap[dmlkey] = fileIndexKeyMap{
			fileIdx.FileIndexKey: fileIdx.Idx,
		}
	} else if fileIdx.Idx > m[fileIdx.FileIndexKey] {
		m[fileIdx.FileIndexKey] = fileIdx.Idx
	}
}

type schemaDefinition struct {
	path             string
	columnFieldTypes map[string]*ptypes.FieldType
}

type schemaKey struct {
	schema string
	table  string
}

var ErrWalkDirEnd = perrors.Normalize("walk dir end", perrors.RFCCodeText("CDC:ErrWalkDirEnd"))

type CurrentTableVersion struct {
	mu                     sync.RWMutex
	currentTableVersionMap map[schemaKey]types.VersionKey
}

func NewCurrentTableVersion() *CurrentTableVersion {
	return &CurrentTableVersion{
		currentTableVersionMap: make(map[schemaKey]types.VersionKey),
	}
}

// GetCurrentTableVersion returns the current table version for a given schema and table
func (cvt *CurrentTableVersion) GetCurrentTableVersion(schema, table string) types.VersionKey {
	cvt.mu.RLock()
	defer cvt.mu.RUnlock()
	return cvt.currentTableVersionMap[schemaKey{schema: schema, table: table}]
}

// UpdateCurrentTableVersion updates the current table version for a given schema and table
func (cvt *CurrentTableVersion) UpdateCurrentTableVersion(schema, table string, version types.VersionKey) {
	cvt.mu.Lock()
	defer cvt.mu.Unlock()
	cvt.currentTableVersionMap[schemaKey{schema: schema, table: table}] = version
}

type SchemaDefinitions struct {
	mu                  sync.RWMutex
	schemaDefinitionMap map[cloudstorage.SchemaPathKey]schemaDefinition
}

func NewSchemaDefinitions() *SchemaDefinitions {
	return &SchemaDefinitions{
		schemaDefinitionMap: make(map[cloudstorage.SchemaPathKey]schemaDefinition),
	}
}

// GetColumnFieldTypes returns the pre-parsed column field types for a given schema and table version
func (sp *SchemaDefinitions) GetColumnFieldTypes(schema, table string, version uint64) (map[string]*ptypes.FieldType, error) {
	schemaPathKey := cloudstorage.SchemaPathKey{
		Schema:       schema,
		Table:        table,
		TableVersion: version,
	}
	sp.mu.RLock()
	schemaDefinition, ok := sp.schemaDefinitionMap[schemaPathKey]
	sp.mu.RUnlock()
	if !ok {
		return nil, errors.Errorf("schema definition not found for schema: %s, table: %s, version: %d", schema, table, version)
	}
	return schemaDefinition.columnFieldTypes, nil
}

// SetSchemaDefinition sets the schema definition for a given schema and table version.
// It pre-parses the column field types from the table definition for later use by the decoder.
func (sp *SchemaDefinitions) SetSchemaDefinition(schemaPathKey cloudstorage.SchemaPathKey, filePath string, tableDefinition *cloudstorage.TableDefinition) error {
	columnFieldTypes := make(map[string]*ptypes.FieldType)
	if tableDefinition != nil {
		for i, col := range tableDefinition.Columns {
			colInfo, err := col.ToTiColumnInfo(int64(i))
			if err != nil {
				return errors.Annotatef(err, "failed to convert column %s to FieldType", col.Name)
			}
			columnFieldTypes[col.Name] = &colInfo.FieldType
		}
	}
	sp.mu.Lock()
	sp.schemaDefinitionMap[schemaPathKey] = schemaDefinition{
		path:             filePath,
		columnFieldTypes: columnFieldTypes,
	}
	sp.mu.Unlock()
	return nil
}

// RemoveSchemaDefinitionWithCondition removes the schema definition for a given condition
func (sp *SchemaDefinitions) RemoveSchemaDefinitionWithCondition(condition func(schemaPathKey cloudstorage.SchemaPathKey) bool) {
	sp.mu.Lock()
	for schemaPathkey := range sp.schemaDefinitionMap {
		if condition(schemaPathkey) {
			delete(sp.schemaDefinitionMap, schemaPathkey)
		}
	}
	sp.mu.Unlock()
}

type TableDMLIdx struct {
	mu             sync.Mutex
	tableDMLIdxMap map[cloudstorage.DmlPathKey]fileIndexKeyMap
}

func NewTableDMLIdx() *TableDMLIdx {
	return &TableDMLIdx{
		tableDMLIdxMap: make(map[cloudstorage.DmlPathKey]fileIndexKeyMap),
	}
}

func (t *TableDMLIdx) UpdateDMLIdxMapByStartPath(dmlkey cloudstorage.DmlPathKey, fileIdx *cloudstorage.FileIndex) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if originalFileIndexKeyMap, ok := t.tableDMLIdxMap[dmlkey]; !ok {
		t.tableDMLIdxMap[dmlkey] = fileIndexKeyMap{
			fileIdx.FileIndexKey: fileIdx.Idx,
		}
	} else {
		if fileIdx.Idx > originalFileIndexKeyMap[fileIdx.FileIndexKey] {
			originalFileIndexKeyMap[fileIdx.FileIndexKey] = fileIdx.Idx
		}
	}
}

func (t *TableDMLIdx) DiffNewTableDMLIdxMap(
	newTableDMLIdxMap map[cloudstorage.DmlPathKey]fileIndexKeyMap,
) map[cloudstorage.DmlPathKey]fileIndexRange {
	resMap := make(map[cloudstorage.DmlPathKey]fileIndexRange)
	t.mu.Lock()
	defer t.mu.Unlock()
	for newDMLPathKey, newFileIndexKeyMap := range newTableDMLIdxMap {
		origFileIndexKeyMap, ok := t.tableDMLIdxMap[newDMLPathKey]
		if !ok {
			t.tableDMLIdxMap[newDMLPathKey] = newFileIndexKeyMap
			resMap[newDMLPathKey] = make(fileIndexRange)
			for indexKey, newEndVal := range newFileIndexKeyMap {
				resMap[newDMLPathKey][indexKey] = indexRange{
					start: 1,
					end:   newEndVal,
				}
			}
			continue
		}
		for indexKey, newEndVal := range newFileIndexKeyMap {
			origEndVal := origFileIndexKeyMap[indexKey]
			if newEndVal > origEndVal {
				origFileIndexKeyMap[indexKey] = newEndVal
				if _, ok := resMap[newDMLPathKey]; !ok {
					resMap[newDMLPathKey] = make(fileIndexRange)
				}
				resMap[newDMLPathKey][indexKey] = indexRange{
					start: origEndVal + 1,
					end:   newEndVal,
				}
			}
		}
	}
	return resMap
}

type S3Consumer struct {
	s3Storage      storage.ExternalStorage
	fileExtension  string
	dateSeparator  string
	fileIndexWidth int
	tables         map[string][]string

	// skip the first round data download
	skipDownloadData bool

	currentTableVersion *CurrentTableVersion
	tableDMLIdx         *TableDMLIdx
	schemaDefinitions   *SchemaDefinitions
}

func NewS3Consumer(
	s3Storage storage.ExternalStorage,
	tables map[string][]string,
) *S3Consumer {
	return &S3Consumer{
		s3Storage:      s3Storage,
		fileExtension:  ".json",
		dateSeparator:  config.DateSeparatorDay.String(),
		fileIndexWidth: config.DefaultFileIndexWidth,
		tables:         tables,

		skipDownloadData: true,

		currentTableVersion: NewCurrentTableVersion(),
		tableDMLIdx:         NewTableDMLIdx(),
		schemaDefinitions:   NewSchemaDefinitions(),
	}
}

func (c *S3Consumer) InitializeFromCheckpoint(
	ctx context.Context, clusterID string, checkpoint *recorder.Checkpoint,
) (map[cloudstorage.DmlPathKey]types.IncrementalData, error) {
	if checkpoint == nil {
		return nil, nil
	}
	if checkpoint.CheckpointItems[2] == nil {
		return nil, nil
	}
	c.skipDownloadData = false
	scanRanges, err := checkpoint.ToScanRange(clusterID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var mu sync.Mutex
	// Combine DML data and schema data into result
	result := make(map[cloudstorage.DmlPathKey]types.IncrementalData)
	eg, egCtx := errgroup.WithContext(ctx)
	for schemaTableKey, scanRange := range scanRanges {
		eg.Go(func() error {
			scanVersions, err := c.downloadSchemaFilesWithScanRange(
				egCtx, schemaTableKey.Schema, schemaTableKey.Table, scanRange.StartVersionKey, scanRange.EndVersionKey, scanRange.EndDataPath)
			if err != nil {
				return errors.Trace(err)
			}
			err = c.downloadDataFilesWithScanRange(
				egCtx, schemaTableKey.Schema, schemaTableKey.Table, scanVersions, scanRange,
				func(
					dmlPathKey cloudstorage.DmlPathKey,
					dmlSlices map[cloudstorage.FileIndexKey][][]byte,
					columnFieldTypes map[string]*ptypes.FieldType,
				) {
					mu.Lock()
					result[dmlPathKey] = types.IncrementalData{
						DataContentSlices: dmlSlices,
						ColumnFieldTypes:  columnFieldTypes,
					}
					mu.Unlock()
				},
			)
			if err != nil {
				return errors.Trace(err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, errors.Trace(err)
	}
	return result, nil
}

func (c *S3Consumer) downloadSchemaFilesWithScanRange(
	ctx context.Context,
	schema, table string,
	startVersionKey string,
	endVersionKey string,
	endDataPath string,
) ([]types.VersionKey, error) {
	metaSubDir := fmt.Sprintf("%s/%s/meta/", schema, table)
	opt := &storage.WalkOption{
		SubDir:    metaSubDir,
		ObjPrefix: "schema_",
		// TODO: StartAfter: startVersionKey,
	}

	var startSchemaKey, endSchemaKey cloudstorage.SchemaPathKey
	_, err := startSchemaKey.ParseSchemaFilePath(startVersionKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, err = endSchemaKey.ParseSchemaFilePath(endVersionKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var scanVersions []types.VersionKey
	newVersionPaths := make(map[cloudstorage.SchemaPathKey]string)
	scanVersions = append(scanVersions, types.VersionKey{
		Version:     startSchemaKey.TableVersion,
		VersionPath: startVersionKey,
	})
	newVersionPaths[startSchemaKey] = startVersionKey
	if err := c.s3Storage.WalkDir(ctx, opt, func(filePath string, size int64) error {
		if endVersionKey < filePath {
			return ErrWalkDirEnd
		}
		if !cloudstorage.IsSchemaFile(filePath) {
			return nil
		}
		var schemaKey cloudstorage.SchemaPathKey
		_, err := schemaKey.ParseSchemaFilePath(filePath)
		if err != nil {
			log.Error("failed to parse schema file path, skipping",
				zap.String("path", filePath),
				zap.Error(err))
			return nil
		}
		if schemaKey.TableVersion > startSchemaKey.TableVersion {
			if _, exists := newVersionPaths[schemaKey]; !exists {
				scanVersions = append(scanVersions, types.VersionKey{
					Version:     schemaKey.TableVersion,
					VersionPath: filePath,
				})
			}
			newVersionPaths[schemaKey] = filePath
		}
		return nil
	}); err != nil && !errors.Is(err, ErrWalkDirEnd) {
		return nil, errors.Trace(err)
	}

	if err := c.downloadSchemaFiles(ctx, newVersionPaths); err != nil {
		return nil, errors.Trace(err)
	}

	c.currentTableVersion.UpdateCurrentTableVersion(schema, table, types.VersionKey{
		Version:     endSchemaKey.TableVersion,
		VersionPath: endVersionKey,
		DataPath:    endDataPath,
	})

	return scanVersions, nil
}

// downloadDataFilesWithScanRange downloads data files for a given scan range.
// consumeFunc is called from multiple goroutines concurrently and must be goroutine-safe.
func (c *S3Consumer) downloadDataFilesWithScanRange(
	ctx context.Context,
	schema, table string,
	scanVersions []types.VersionKey,
	scanRange *recorder.ScanRange,
	consumeFunc func(
		dmlPathKey cloudstorage.DmlPathKey,
		dmlSlices map[cloudstorage.FileIndexKey][][]byte,
		columnFieldTypes map[string]*ptypes.FieldType,
	),
) error {
	eg, egCtx := errgroup.WithContext(ctx)
	for _, version := range scanVersions {
		eg.Go(func() error {
			newFiles, err := c.getNewFilesForSchemaPathKeyWithEndPath(egCtx, schema, table, version.Version, scanRange.StartDataPath, scanRange.EndDataPath)
			if err != nil {
				return errors.Trace(err)
			}
			dmlData, err := c.downloadDMLFiles(egCtx, newFiles)
			if err != nil {
				return errors.Trace(err)
			}
			columnFieldTypes, err := c.schemaDefinitions.GetColumnFieldTypes(schema, table, version.Version)
			if err != nil {
				return errors.Trace(err)
			}
			for dmlPathKey, dmlSlices := range dmlData {
				consumeFunc(dmlPathKey, dmlSlices, columnFieldTypes)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *S3Consumer) getNewFilesForSchemaPathKeyWithEndPath(
	ctx context.Context,
	schema, table string,
	version uint64,
	startDataPath string,
	endDataPath string,
) (map[cloudstorage.DmlPathKey]fileIndexRange, error) {
	schemaPrefix := path.Join(schema, table, fmt.Sprintf("%d", version))
	opt := &storage.WalkOption{
		SubDir: schemaPrefix,
		// TODO: StartAfter: startDataPath,
	}
	newTableDMLIdxMap := make(map[cloudstorage.DmlPathKey]fileIndexKeyMap)
	if err := c.s3Storage.WalkDir(ctx, opt, func(filePath string, size int64) error {
		if endDataPath < filePath {
			return ErrWalkDirEnd
		}
		// Try to parse DML file path if it matches the expected extension
		if strings.HasSuffix(filePath, c.fileExtension) {
			var dmlkey cloudstorage.DmlPathKey
			fileIdx, err := dmlkey.ParseDMLFilePath(c.dateSeparator, filePath)
			if err != nil {
				log.Error("failed to parse dml file path, skipping",
					zap.String("path", filePath),
					zap.Error(err))
				return nil
			}
			if filePath == startDataPath {
				c.tableDMLIdx.UpdateDMLIdxMapByStartPath(dmlkey, fileIdx)
			} else {
				updateTableDMLIdxMap(newTableDMLIdxMap, dmlkey, fileIdx)
			}
		}
		return nil
	}); err != nil && !errors.Is(err, ErrWalkDirEnd) {
		return nil, errors.Trace(err)
	}
	return c.tableDMLIdx.DiffNewTableDMLIdxMap(newTableDMLIdxMap), nil
}

// downloadSchemaFiles downloads schema files concurrently for given schema path keys
func (c *S3Consumer) downloadSchemaFiles(
	ctx context.Context,
	newVersionPaths map[cloudstorage.SchemaPathKey]string,
) error {
	eg, egCtx := errgroup.WithContext(ctx)

	log.Debug("starting concurrent schema file download", zap.Int("totalSchemas", len(newVersionPaths)))
	for schemaPathKey, filePath := range newVersionPaths {
		eg.Go(func() error {
			content, err := c.s3Storage.ReadFile(egCtx, filePath)
			if err != nil {
				return errors.Annotatef(err, "failed to read schema file: %s", filePath)
			}

			tableDefinition := &cloudstorage.TableDefinition{}
			if err := json.Unmarshal(content, tableDefinition); err != nil {
				return errors.Annotatef(err, "failed to unmarshal schema file: %s", filePath)
			}
			if err := c.schemaDefinitions.SetSchemaDefinition(schemaPathKey, filePath, tableDefinition); err != nil {
				return errors.Trace(err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *S3Consumer) discoverAndDownloadNewTableVersions(
	ctx context.Context,
	schema, table string,
) ([]types.VersionKey, error) {
	currentVersion := c.currentTableVersion.GetCurrentTableVersion(schema, table)
	metaSubDir := fmt.Sprintf("%s/%s/meta/", schema, table)
	opt := &storage.WalkOption{
		SubDir:    metaSubDir,
		ObjPrefix: "schema_",
		// TODO: StartAfter: currentVersion.versionPath,
	}

	var scanVersions []types.VersionKey
	newVersionPaths := make(map[cloudstorage.SchemaPathKey]string)
	if err := c.s3Storage.WalkDir(ctx, opt, func(filePath string, size int64) error {
		if !cloudstorage.IsSchemaFile(filePath) {
			return nil
		}
		var schemaKey cloudstorage.SchemaPathKey
		_, err := schemaKey.ParseSchemaFilePath(filePath)
		if err != nil {
			log.Error("failed to parse schema file path, skipping",
				zap.String("path", filePath),
				zap.Error(err))
			return nil
		}
		version := schemaKey.TableVersion
		if version > currentVersion.Version {
			if _, exists := newVersionPaths[schemaKey]; !exists {
				scanVersions = append(scanVersions, types.VersionKey{
					Version:     version,
					VersionPath: filePath,
				})
			}
			newVersionPaths[schemaKey] = filePath
		}
		return nil
	}); err != nil {
		return nil, errors.Trace(err)
	}

	// download new version schema files concurrently
	if err := c.downloadSchemaFiles(ctx, newVersionPaths); err != nil {
		return nil, errors.Trace(err)
	}

	if currentVersion.Version > 0 {
		scanVersions = append(scanVersions, currentVersion)
	}
	return scanVersions, nil
}

func (c *S3Consumer) getNewFilesForSchemaPathKey(
	ctx context.Context,
	schema, table string,
	version *types.VersionKey,
) (map[cloudstorage.DmlPathKey]fileIndexRange, error) {
	schemaPrefix := path.Join(schema, table, fmt.Sprintf("%d", version.Version))
	opt := &storage.WalkOption{
		SubDir: schemaPrefix,
		// TODO: StartAfter: version.dataPath,
	}

	newTableDMLIdxMap := make(map[cloudstorage.DmlPathKey]fileIndexKeyMap)
	maxFilePath := ""
	if err := c.s3Storage.WalkDir(ctx, opt, func(filePath string, size int64) error {
		// Try to parse DML file path if it matches the expected extension
		if strings.HasSuffix(filePath, c.fileExtension) {
			var dmlkey cloudstorage.DmlPathKey
			fileIdx, err := dmlkey.ParseDMLFilePath(c.dateSeparator, filePath)
			if err != nil {
				log.Error("failed to parse dml file path, skipping",
					zap.String("path", filePath),
					zap.Error(err))
				return nil
			}
			updateTableDMLIdxMap(newTableDMLIdxMap, dmlkey, fileIdx)
			maxFilePath = filePath
		}
		return nil
	}); err != nil {
		return nil, errors.Trace(err)
	}

	version.DataPath = maxFilePath
	return c.tableDMLIdx.DiffNewTableDMLIdxMap(newTableDMLIdxMap), nil
}

func (c *S3Consumer) downloadDMLFiles(
	ctx context.Context,
	newFiles map[cloudstorage.DmlPathKey]fileIndexRange,
) (map[cloudstorage.DmlPathKey]map[cloudstorage.FileIndexKey][][]byte, error) {
	if len(newFiles) == 0 || c.skipDownloadData {
		return nil, nil
	}

	result := make(map[cloudstorage.DmlPathKey]map[cloudstorage.FileIndexKey][][]byte)
	type downloadTask struct {
		dmlPathKey cloudstorage.DmlPathKey
		fileIndex  cloudstorage.FileIndex
	}

	var tasks []downloadTask
	for dmlPathKey, fileRange := range newFiles {
		for indexKey, indexRange := range fileRange {
			log.Debug("prepare to download new dml file in index range",
				zap.String("schema", dmlPathKey.Schema),
				zap.String("table", dmlPathKey.Table),
				zap.Uint64("version", dmlPathKey.TableVersion),
				zap.Int64("partitionNum", dmlPathKey.PartitionNum),
				zap.String("date", dmlPathKey.Date),
				zap.String("dispatcherID", indexKey.DispatcherID),
				zap.Bool("enableTableAcrossNodes", indexKey.EnableTableAcrossNodes),
				zap.Uint64("startIndex", indexRange.start),
				zap.Uint64("endIndex", indexRange.end))
			for i := indexRange.start; i <= indexRange.end; i++ {
				tasks = append(tasks, downloadTask{
					dmlPathKey: dmlPathKey,
					fileIndex: cloudstorage.FileIndex{
						FileIndexKey: indexKey,
						Idx:          i,
					},
				})
			}
		}
	}

	log.Debug("starting concurrent DML file download", zap.Int("totalFiles", len(tasks)))

	// Concurrently download files
	type fileContent struct {
		dmlPathKey cloudstorage.DmlPathKey
		indexKey   cloudstorage.FileIndexKey
		idx        uint64
		content    []byte
	}

	fileContents := make(chan fileContent, len(tasks))
	eg, egCtx := errgroup.WithContext(ctx)
	for _, task := range tasks {
		eg.Go(func() error {
			filePath := task.dmlPathKey.GenerateDMLFilePath(
				&task.fileIndex,
				c.fileExtension,
				c.fileIndexWidth,
			)

			content, err := c.s3Storage.ReadFile(egCtx, filePath)
			if err != nil {
				return errors.Annotatef(err, "failed to read file: %s", filePath)
			}

			// Channel writes are thread-safe, no mutex needed
			fileContents <- fileContent{
				dmlPathKey: task.dmlPathKey,
				indexKey:   task.fileIndex.FileIndexKey,
				idx:        task.fileIndex.Idx,
				content:    content,
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, errors.Trace(err)
	}

	// Close the channel to signal no more writes
	close(fileContents)

	// Process the downloaded file contents
	for fc := range fileContents {
		if result[fc.dmlPathKey] == nil {
			result[fc.dmlPathKey] = make(map[cloudstorage.FileIndexKey][][]byte)
		}
		result[fc.dmlPathKey][fc.indexKey] = append(
			result[fc.dmlPathKey][fc.indexKey],
			fc.content,
		)
	}

	return result, nil
}

// downloadNewFilesWithVersions downloads new files for given schema versions.
// consumeFunc is called from multiple goroutines concurrently and must be goroutine-safe.
func (c *S3Consumer) downloadNewFilesWithVersions(
	ctx context.Context,
	schema, table string,
	scanVersions []types.VersionKey,
	consumeFunc func(
		dmlPathKey cloudstorage.DmlPathKey,
		dmlSlices map[cloudstorage.FileIndexKey][][]byte,
		columnFieldTypes map[string]*ptypes.FieldType,
	),
) (*types.VersionKey, error) {
	var maxVersion *types.VersionKey
	eg, egCtx := errgroup.WithContext(ctx)
	for _, version := range scanVersions {
		versionp := &version
		if maxVersion == nil || maxVersion.Version < version.Version {
			maxVersion = versionp
		}
		eg.Go(func() error {
			newFiles, err := c.getNewFilesForSchemaPathKey(egCtx, schema, table, versionp)
			if err != nil {
				return errors.Trace(err)
			}
			dmlData, err := c.downloadDMLFiles(egCtx, newFiles)
			if err != nil {
				return errors.Trace(err)
			}
			columnFieldTypes, err := c.schemaDefinitions.GetColumnFieldTypes(schema, table, versionp.Version)
			if err != nil {
				return errors.Trace(err)
			}
			for dmlPathKey, dmlSlices := range dmlData {
				consumeFunc(dmlPathKey, dmlSlices, columnFieldTypes)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, errors.Trace(err)
	}
	if maxVersion != nil {
		c.currentTableVersion.UpdateCurrentTableVersion(schema, table, *maxVersion)
	}
	return maxVersion, nil
}

func (c *S3Consumer) ConsumeNewFiles(
	ctx context.Context,
) (map[cloudstorage.DmlPathKey]types.IncrementalData, map[types.SchemaTableKey]types.VersionKey, error) {
	var mu sync.Mutex
	// Combine DML data and schema data into result
	result := make(map[cloudstorage.DmlPathKey]types.IncrementalData)
	var versionMu sync.Mutex
	maxVersionMap := make(map[types.SchemaTableKey]types.VersionKey)
	eg, egCtx := errgroup.WithContext(ctx)
	for schema, tables := range c.tables {
		for _, table := range tables {
			eg.Go(func() error {
				scanVersions, err := c.discoverAndDownloadNewTableVersions(egCtx, schema, table)
				if err != nil {
					return errors.Trace(err)
				}
				maxVersion, err := c.downloadNewFilesWithVersions(
					egCtx, schema, table, scanVersions,
					func(
						dmlPathKey cloudstorage.DmlPathKey,
						dmlSlices map[cloudstorage.FileIndexKey][][]byte,
						columnFieldTypes map[string]*ptypes.FieldType,
					) {
						mu.Lock()
						result[dmlPathKey] = types.IncrementalData{
							DataContentSlices: dmlSlices,
							ColumnFieldTypes:  columnFieldTypes,
						}
						mu.Unlock()
					},
				)
				if err != nil {
					return errors.Trace(err)
				}
				if maxVersion != nil {
					versionMu.Lock()
					maxVersionMap[types.SchemaTableKey{Schema: schema, Table: table}] = *maxVersion
					versionMu.Unlock()
				}
				return nil
			})
		}
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, errors.Trace(err)
	}
	c.skipDownloadData = false
	return result, maxVersionMap, nil
}
