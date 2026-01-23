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
	"maps"
	"sort"
	"strings"
	"sync"

	"github.com/pingcap/log"
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

// indexRange defines a range of files. eg. CDC000002.csv ~ CDC000005.csv
type indexRange struct {
	start uint64
	end   uint64
}

type schemaFilePathWithChecksum struct {
	filepath string
	checksum uint32
}

type S3Consumer struct {
	s3Storage         storage.ExternalStorage
	fileExtension     string
	dateSeparator     string
	tableDMLIdxMap    map[cloudstorage.DmlPathKey]fileIndexKeyMap
	schemaChecksumMap map[cloudstorage.SchemaPathKey]schemaFilePathWithChecksum
	fileIndexWidth    int
}

func NewS3Consumer(s3Storage storage.ExternalStorage) *S3Consumer {
	// Use default values for file extension and date separator
	// File extension will be detected from files, date separator defaults to day
	return &S3Consumer{
		s3Storage:         s3Storage,
		fileExtension:     ".csv",
		dateSeparator:     config.DateSeparatorDay.String(),
		tableDMLIdxMap:    make(map[cloudstorage.DmlPathKey]fileIndexKeyMap),
		schemaChecksumMap: make(map[cloudstorage.SchemaPathKey]schemaFilePathWithChecksum),
		fileIndexWidth:    config.DefaultFileIndexWidth,
	}
}

// diffDMLMaps returns the difference between two DML index maps (map1 - map2)
func diffDMLMaps(
	map1, map2 map[cloudstorage.DmlPathKey]fileIndexKeyMap,
) map[cloudstorage.DmlPathKey]fileIndexRange {
	resMap := make(map[cloudstorage.DmlPathKey]fileIndexRange)
	for dmlPathKey1, fileIndexKeyMap1 := range map1 {
		dmlPathKey2, ok := map2[dmlPathKey1]
		if !ok {
			resMap[dmlPathKey1] = make(fileIndexRange)
			for indexKey, val1 := range fileIndexKeyMap1 {
				resMap[dmlPathKey1][indexKey] = indexRange{
					start: 1,
					end:   val1,
				}
			}
			continue
		}
		for fileIndexKey, val1 := range fileIndexKeyMap1 {
			val2 := dmlPathKey2[fileIndexKey]
			if val1 > val2 {
				if _, ok := resMap[dmlPathKey1]; !ok {
					resMap[dmlPathKey1] = make(fileIndexRange)
				}
				resMap[dmlPathKey1][fileIndexKey] = indexRange{
					start: val2 + 1,
					end:   val1,
				}
			}
		}
	}
	return resMap
}

// parseDMLFilePath parses a DML file path and updates the tableDMLIdxMap
func (c *S3Consumer) parseDMLFilePath(_ context.Context, path string) error {
	var dmlkey cloudstorage.DmlPathKey
	fileIdx, err := dmlkey.ParseDMLFilePath(c.dateSeparator, path)
	if err != nil {
		return err
	}

	m, ok := c.tableDMLIdxMap[dmlkey]
	if !ok {
		c.tableDMLIdxMap[dmlkey] = fileIndexKeyMap{
			fileIdx.FileIndexKey: fileIdx.Idx,
		}
	} else if fileIdx.Idx >= m[fileIdx.FileIndexKey] {
		c.tableDMLIdxMap[dmlkey][fileIdx.FileIndexKey] = fileIdx.Idx
	}
	return nil
}

// getNewFiles returns newly created dml files in specific ranges
func (c *S3Consumer) getNewFiles(
	ctx context.Context,
) (map[cloudstorage.DmlPathKey]fileIndexRange, error) {
	tableDMLMap := make(map[cloudstorage.DmlPathKey]fileIndexRange)
	opt := &storage.WalkOption{SubDir: ""}

	// Save a snapshot of current tableDMLIdxMap
	origDMLIdxMap := make(map[cloudstorage.DmlPathKey]fileIndexKeyMap, len(c.tableDMLIdxMap))
	for k, v := range c.tableDMLIdxMap {
		m := make(fileIndexKeyMap)
		maps.Copy(m, v)
		origDMLIdxMap[k] = m
	}

	// Walk through all files in S3 storage
	if err := c.s3Storage.WalkDir(ctx, opt, func(path string, size int64) error {
		if cloudstorage.IsSchemaFile(path) {
			var schemaKey cloudstorage.SchemaPathKey
			checksumInFile, err := schemaKey.ParseSchemaFilePath(path)
			if err != nil {
				log.Error("failed to parse schema file path, skipping",
					zap.String("path", path),
					zap.Error(err))
				return nil
			}
			c.schemaChecksumMap[schemaKey] = schemaFilePathWithChecksum{
				filepath: path,
				checksum: checksumInFile,
			}
			return nil
		}

		// Try to parse DML file path if it matches the expected extension
		if strings.HasSuffix(path, c.fileExtension) {
			err := c.parseDMLFilePath(ctx, path)
			if err != nil {
				log.Error("failed to parse dml file path, skipping",
					zap.String("path", path),
					zap.Error(err))
				return nil
			}
		}
		return nil
	}); err != nil {
		return tableDMLMap, errors.Trace(err)
	}

	// Calculate the difference to find new files
	tableDMLMap = diffDMLMaps(c.tableDMLIdxMap, origDMLIdxMap)
	return tableDMLMap, nil
}

type IncrementalData struct {
	DataContentSlices map[cloudstorage.FileIndexKey][][]byte
	SchemaContent     []byte
}

// downloadDMLFiles downloads DML files concurrently and returns their content
func (c *S3Consumer) downloadDMLFiles(
	ctx context.Context,
	newFiles map[cloudstorage.DmlPathKey]fileIndexRange,
) (map[cloudstorage.DmlPathKey]map[cloudstorage.FileIndexKey][][]byte, error) {
	result := make(map[cloudstorage.DmlPathKey]map[cloudstorage.FileIndexKey][][]byte)

	// Prepare all file download tasks
	type downloadTask struct {
		dmlPathKey cloudstorage.DmlPathKey
		fileIndex  *cloudstorage.FileIndex
	}

	var tasks []downloadTask
	for dmlPathKey, fileRange := range newFiles {
		for indexKey, indexRange := range fileRange {
			for i := indexRange.start; i <= indexRange.end; i++ {
				fileIdx := &cloudstorage.FileIndex{
					FileIndexKey: indexKey,
					Idx:          i,
				}
				tasks = append(tasks, downloadTask{
					dmlPathKey: dmlPathKey,
					fileIndex:  fileIdx,
				})
			}
		}
	}

	log.Info("starting concurrent DML file download", zap.Int("totalFiles", len(tasks)))

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
				task.fileIndex,
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
	close(fileContents)

	// Group and merge file contents by DmlPathKey and FileIndexKey
	type contentWithIdx struct {
		idx     uint64
		content []byte
	}
	tempResult := make(map[cloudstorage.DmlPathKey]map[cloudstorage.FileIndexKey][]contentWithIdx)

	for fc := range fileContents {
		if tempResult[fc.dmlPathKey] == nil {
			tempResult[fc.dmlPathKey] = make(map[cloudstorage.FileIndexKey][]contentWithIdx)
		}
		tempResult[fc.dmlPathKey][fc.indexKey] = append(
			tempResult[fc.dmlPathKey][fc.indexKey],
			contentWithIdx{idx: fc.idx, content: fc.content},
		)
	}

	// Merge contents in order (by idx) for each FileIndexKey
	for dmlPathKey, indexKeyMap := range tempResult {
		if result[dmlPathKey] == nil {
			result[dmlPathKey] = make(map[cloudstorage.FileIndexKey][][]byte)
		}
		for indexKey, contentWithIdxs := range indexKeyMap {
			// Sort by idx to maintain file order
			sort.Slice(contentWithIdxs, func(i, j int) bool {
				return contentWithIdxs[i].idx < contentWithIdxs[j].idx
			})
			contents := make([][]byte, len(contentWithIdxs))
			for i, contentWithIdx := range contentWithIdxs {
				contents[i] = contentWithIdx.content
			}
			result[dmlPathKey][indexKey] = contents
		}
	}

	return result, nil
}

// downloadSchemaFiles downloads schema files concurrently for the given SchemaPathKeys
func (c *S3Consumer) downloadSchemaFiles(
	ctx context.Context,
	schemaPathKeys map[cloudstorage.SchemaPathKey]struct{},
) (map[cloudstorage.SchemaPathKey][]byte, error) {
	schemaContents := make(map[cloudstorage.SchemaPathKey][]byte)
	schemaMutex := &sync.Mutex{}
	eg, egCtx := errgroup.WithContext(ctx)

	log.Info("starting concurrent schema file download", zap.Int("totalSchemas", len(schemaPathKeys)))

	for schemaPathKey := range schemaPathKeys {
		eg.Go(func() error {
			checksumInFile, exists := c.schemaChecksumMap[schemaPathKey]
			if !exists {
				return errors.Annotatef(errors.ErrInternalCheckFailed, "schema file not found: %s", schemaPathKey.GetKey())
			}
			schemaPath := checksumInFile.filepath
			content, err := c.s3Storage.ReadFile(egCtx, schemaPath)
			if err != nil {
				return errors.Annotatef(err, "failed to read schema file: %s", schemaPath)
			}

			schemaMutex.Lock()
			schemaContents[schemaPathKey] = content
			schemaMutex.Unlock()

			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, errors.Trace(err)
	}

	return schemaContents, nil
}

// ConsumeNewFiles downloads new files concurrently and returns their content
func (c *S3Consumer) ConsumeNewFiles(ctx context.Context) (map[cloudstorage.DmlPathKey]IncrementalData, error) {
	newFiles, err := c.getNewFiles(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(newFiles) == 0 {
		log.Info("no new dml files found")
		return nil, nil
	}

	// Download DML files
	dmlData, err := c.downloadDMLFiles(ctx, newFiles)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Collect unique SchemaPathKeys
	schemaPathKeys := make(map[cloudstorage.SchemaPathKey]struct{})
	for dmlPathKey := range newFiles {
		schemaPathKey := dmlPathKey.SchemaPathKey
		schemaPathKeys[schemaPathKey] = struct{}{}
	}

	// Download schema files
	schemaContents, err := c.downloadSchemaFiles(ctx, schemaPathKeys)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Combine DML data and schema data into result
	result := make(map[cloudstorage.DmlPathKey]IncrementalData)
	for dmlPathKey, dataSlices := range dmlData {
		incrementalData := IncrementalData{
			DataContentSlices: dataSlices,
		}
		// Assign schema content if available
		schemaPathKey := dmlPathKey.SchemaPathKey
		if schemaContent, ok := schemaContents[schemaPathKey]; ok {
			incrementalData.SchemaContent = schemaContent
		}
		result[dmlPathKey] = incrementalData
	}

	// Log the new files found
	for dmlPathKey, fileRange := range newFiles {
		for indexKey, indexRange := range fileRange {
			log.Info("found and downloaded new dml files",
				zap.String("schema", dmlPathKey.Schema),
				zap.String("table", dmlPathKey.Table),
				zap.Uint64("tableVersion", dmlPathKey.TableVersion),
				zap.Int64("partitionNum", dmlPathKey.PartitionNum),
				zap.String("date", dmlPathKey.Date),
				zap.String("dispatcherID", indexKey.DispatcherID),
				zap.Bool("enableTableAcrossNodes", indexKey.EnableTableAcrossNodes),
				zap.Uint64("startIndex", indexRange.start),
				zap.Uint64("endIndex", indexRange.end),
				zap.Int("fileCount", int(indexRange.end-indexRange.start+1)))
		}
	}

	return result, nil
}
