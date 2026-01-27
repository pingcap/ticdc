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
	"fmt"
	"maps"
	"path"
	"strings"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/parser"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/tidb/br/pkg/storage"
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

type schemaParser struct {
	path   string
	parser *parser.TableParser
}

type schemaKey struct {
	schema string
	table  string
}

type IncrementalData struct {
	DataContentSlices map[cloudstorage.FileIndexKey][][]byte
	Parser            *parser.TableParser
}

type Consumer struct {
	s3Storage      storage.ExternalStorage
	fileExtension  string
	dateSeparator  string
	fileIndexWidth int
	tables         map[string]map[string]struct{}

	versionMapMu           sync.RWMutex
	currentTableVersionMap map[schemaKey]uint64
	tableDMLIdxMapMu       sync.Mutex
	tableDMLIdxMap         map[cloudstorage.DmlPathKey]fileIndexKeyMap
	schemaParserMapMu      sync.RWMutex
	schemaParserMap        map[cloudstorage.SchemaPathKey]schemaParser
}

func NewConsumer(
	s3Storage storage.ExternalStorage,
	tables map[string]map[string]struct{},
) *Consumer {
	return &Consumer{
		s3Storage:              s3Storage,
		fileExtension:          ".csv",
		dateSeparator:          config.DateSeparatorDay.String(),
		fileIndexWidth:         config.DefaultFileIndexWidth,
		tables:                 tables,
		currentTableVersionMap: make(map[schemaKey]uint64, 0),
		tableDMLIdxMap:         make(map[cloudstorage.DmlPathKey]fileIndexKeyMap),
		schemaParserMap:        make(map[cloudstorage.SchemaPathKey]schemaParser),
	}
}

// getCurrentTableVersion returns the current table version for a given schema and table
func (c *Consumer) getCurrentTableVersion(schema, table string) uint64 {
	tableKey := schemaKey{
		schema: schema,
		table:  table,
	}
	c.versionMapMu.RLock()
	currentVersion := c.currentTableVersionMap[tableKey]
	c.versionMapMu.RUnlock()
	return currentVersion
}

// updateCurrentTableVersion updates the current table version for a given schema and table
func (c *Consumer) updateCurrentTableVersion(schema, table string, version uint64) {
	tableKey := schemaKey{
		schema: schema,
		table:  table,
	}
	c.versionMapMu.Lock()
	c.currentTableVersionMap[tableKey] = version
	c.versionMapMu.Unlock()
}

// getSchemaParser returns the schema parser for a given schema and table version
func (c *Consumer) getSchemaParser(schema, table string, version uint64) (*parser.TableParser, error) {
	schemaPathKey := cloudstorage.SchemaPathKey{
		Schema:       schema,
		Table:        table,
		TableVersion: version,
	}
	c.schemaParserMapMu.RLock()
	schemaParser, ok := c.schemaParserMap[schemaPathKey]
	c.schemaParserMapMu.RUnlock()
	if !ok {
		return nil, errors.Errorf("schema parser not found for schema: %s, table: %s, version: %d", schema, table, version)
	}
	return schemaParser.parser, nil
}

// setSchemaParser sets the schema parser for a given schema and table version
func (c *Consumer) setSchemaParser(schemaPathKey cloudstorage.SchemaPathKey, filePath string, parser *parser.TableParser) {
	c.schemaParserMapMu.Lock()
	c.schemaParserMap[schemaPathKey] = schemaParser{
		path:   filePath,
		parser: parser,
	}
	c.schemaParserMapMu.Unlock()
}

// downloadSchemaFiles downloads schema files concurrently for given schema path keys
func (c *Consumer) downloadSchemaFiles(
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

			parser, err := parser.NewTableParser(schemaPathKey.GetKey(), content)
			if err != nil {
				return errors.Annotatef(err, "failed to create table parser: %s", schemaPathKey.GetKey())
			}

			c.setSchemaParser(schemaPathKey, filePath, parser)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Consumer) discoverAndDownloadNewTableVersions(
	ctx context.Context,
	schema, table string,
) ([]uint64, error) {
	currentVersion := c.getCurrentTableVersion(schema, table)
	metaSubDir := fmt.Sprintf("%s/%s/meta/", schema, table)
	opt := &storage.WalkOption{
		SubDir:    metaSubDir,
		ObjPrefix: "schema_",
	}

	var scanVersions []uint64
	newVersionPaths := make(map[cloudstorage.SchemaPathKey]string)
	versionSet := make(map[uint64]struct{})
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
		if version > currentVersion {
			if _, exists := versionSet[version]; !exists {
				versionSet[version] = struct{}{}
				scanVersions = append(scanVersions, version)
			}
		}

		newVersionPaths[schemaKey] = filePath
		return nil
	}); err != nil {
		return nil, errors.Trace(err)
	}

	// download new version schema files concurrently
	if err := c.downloadSchemaFiles(ctx, newVersionPaths); err != nil {
		return nil, errors.Trace(err)
	}

	if currentVersion > 0 {
		scanVersions = append(scanVersions, currentVersion)
	}
	return scanVersions, nil
}

func (c *Consumer) diffNewTableDMLIdxMap(
	newTableDMLIdxMap map[cloudstorage.DmlPathKey]fileIndexKeyMap,
) map[cloudstorage.DmlPathKey]fileIndexRange {
	resMap := make(map[cloudstorage.DmlPathKey]fileIndexRange)
	c.tableDMLIdxMapMu.Lock()
	defer c.tableDMLIdxMapMu.Unlock()
	for newDMLPathKey, newFileIndexKeyMap := range newTableDMLIdxMap {
		origFileIndexKeyMap, ok := c.tableDMLIdxMap[newDMLPathKey]
		if !ok {
			c.tableDMLIdxMap[newDMLPathKey] = newFileIndexKeyMap
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

func (c *Consumer) getNewFilesForSchemaPathKey(
	ctx context.Context,
	schema, table string,
	version uint64,
) (map[cloudstorage.DmlPathKey]fileIndexRange, error) {
	schemaPrefix := path.Join(schema, table, fmt.Sprintf("%d", version))
	opt := &storage.WalkOption{SubDir: schemaPrefix}

	// Save a snapshot of current tableDMLIdxMap
	origDMLIdxMap := make(map[cloudstorage.DmlPathKey]fileIndexKeyMap, len(c.tableDMLIdxMap))
	for k, v := range c.tableDMLIdxMap {
		m := make(fileIndexKeyMap)
		maps.Copy(m, v)
		origDMLIdxMap[k] = m
	}

	// Walk through all files in S3 storage
	newTableDMLIdxMap := make(map[cloudstorage.DmlPathKey]fileIndexKeyMap)
	if err := c.s3Storage.WalkDir(ctx, opt, func(path string, size int64) error {
		// Try to parse DML file path if it matches the expected extension
		if strings.HasSuffix(path, c.fileExtension) {
			var dmlkey cloudstorage.DmlPathKey
			fileIdx, err := dmlkey.ParseDMLFilePath(c.dateSeparator, path)
			if err != nil {
				log.Error("failed to parse dml file path, skipping",
					zap.String("path", path),
					zap.Error(err))
				return nil
			}
			updateTableDMLIdxMap(newTableDMLIdxMap, dmlkey, fileIdx)
		}
		return nil
	}); err != nil {
		return nil, errors.Trace(err)
	}

	// Calculate the difference to find new files
	return c.diffNewTableDMLIdxMap(newTableDMLIdxMap), nil
}

func (c *Consumer) downloadDMLFiles(
	ctx context.Context,
	schema, table string,
	version uint64,
) (map[cloudstorage.DmlPathKey]map[cloudstorage.FileIndexKey][][]byte, error) {
	newFiles, err := c.getNewFilesForSchemaPathKey(ctx, schema, table, version)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(newFiles) == 0 {
		return nil, nil
	}

	result := make(map[cloudstorage.DmlPathKey]map[cloudstorage.FileIndexKey][][]byte)

	// Prepare all file download tasks
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

func (c *Consumer) ConsumeNewFiles(
	ctx context.Context,
) (map[cloudstorage.DmlPathKey]IncrementalData, error) {
	var mu sync.Mutex
	// Combine DML data and schema data into result
	result := make(map[cloudstorage.DmlPathKey]IncrementalData)
	eg, egCtx := errgroup.WithContext(ctx)
	for schema, tables := range c.tables {
		for table := range tables {
			eg.Go(func() error {
				newVersions, err := c.discoverAndDownloadNewTableVersions(egCtx, schema, table)
				if err != nil {
					return errors.Trace(err)
				}
				maxVersion := uint64(0)
				for _, version := range newVersions {
					maxVersion = max(maxVersion, version)
					eg.Go(func() error {
						dmlData, err := c.downloadDMLFiles(egCtx, schema, table, version)
						if err != nil {
							return errors.Trace(err)
						}
						parser, err := c.getSchemaParser(schema, table, version)
						if err != nil {
							return errors.Trace(err)
						}
						for dmlPathKey, dmlSlices := range dmlData {
							mu.Lock()
							result[dmlPathKey] = IncrementalData{
								DataContentSlices: dmlSlices,
								Parser:            parser,
							}
							mu.Unlock()
						}
						return nil
					})
				}
				c.updateCurrentTableVersion(schema, table, maxVersion)
				return nil
			})
		}
	}

	if err := eg.Wait(); err != nil {
		return nil, errors.Trace(err)
	}
	return result, nil
}
