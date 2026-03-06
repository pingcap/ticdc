// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
	"go.uber.org/zap"
)

// map1 - map2
func diffDMLMaps(
	map1, map2 map[DmlPathKey]fileIndexKeyMap,
) map[DmlPathKey]fileIndexRange {
	resMap := make(map[DmlPathKey]fileIndexRange) // DmlPathKey -> FileIndexKey -> indexRange
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

// getNewFiles returns newly created dml files in specific ranges
func (c *consumer) getNewFiles(
	ctx context.Context,
) (map[DmlPathKey]fileIndexRange, error) {
	tableDMLMap := make(map[DmlPathKey]fileIndexRange)
	opt := &storage.WalkOption{SubDir: ""}

	origDMLIdxMap := make(map[DmlPathKey]fileIndexKeyMap, len(c.tableDMLIdxMap))
	for k, v := range c.tableDMLIdxMap {
		m := make(fileIndexKeyMap)
		for fileIndexKey, val := range v {
			m[fileIndexKey] = val
		}
		origDMLIdxMap[k] = m
	}

	err := c.externalStorage.WalkDir(ctx, opt, func(path string, size int64) error {
		if IsSchemaFile(path) {
			err := c.parseSchemaFilePath(ctx, path)
			if err != nil {
				log.Error("failed to parse schema file path", zap.Error(err))
				// skip handling this file
				return nil
			}
		} else if strings.HasSuffix(path, ".index") {
			err := c.parseDMLFilePath(ctx, path)
			if err != nil {
				log.Error("failed to parse dml file path", zap.Error(err))
				// skip handling this file
				return nil
			}
		} else {
			log.Debug("ignore handling file", zap.String("path", path))
		}
		return nil
	})
	if err != nil {
		return tableDMLMap, err
	}

	tableDMLMap = diffDMLMaps(c.tableDMLIdxMap, origDMLIdxMap)
	return tableDMLMap, err
}

func (c *consumer) parseDMLFilePath(ctx context.Context, path string) error {
	var dmlkey DmlPathKey
	dispatcherID, err := dmlkey.ParseIndexFilePath(
		putil.GetOrZero(c.replicationCfg.Sink.DateSeparator),
		path,
	)
	if err != nil {
		return errors.Trace(err)
	}
	data, err := c.externalStorage.ReadFile(ctx, path)
	if err != nil {
		return errors.Trace(err)
	}
	fileName := strings.TrimSuffix(string(data), "\n")
	fileIdx, err := FetchIndexFromFileName(fileName, c.fileExtension)
	if err != nil {
		return err
	}
	fileIndex := &FileIndex{
		FileIndexKey: FileIndexKey{
			DispatcherID:           dispatcherID,
			EnableTableAcrossNodes: dispatcherID != "",
		},
		Idx: fileIdx,
	}

	m, ok := c.tableDMLIdxMap[dmlkey]
	if !ok {
		c.tableDMLIdxMap[dmlkey] = fileIndexKeyMap{
			fileIndex.FileIndexKey: fileIndex.Idx,
		}
	} else if fileIndex.Idx >= m[fileIndex.FileIndexKey] {
		c.tableDMLIdxMap[dmlkey][fileIndex.FileIndexKey] = fileIndex.Idx
	}
	return nil
}

func (c *consumer) parseSchemaFilePath(ctx context.Context, path string) error {
	var schemaKey SchemaPathKey
	checksumInFile, err := schemaKey.ParseSchemaFilePath(path)
	if err != nil {
		return errors.Trace(err)
	}
	key := schemaKey.GetKey()
	if tableDefs, ok := c.tableDefMap[key]; ok {
		if _, ok := tableDefs[schemaKey.TableVersion]; ok {
			// Skip if tableDef already exists.
			return nil
		}
	} else {
		c.tableDefMap[key] = make(map[uint64]*cloudstorage.TableDefinition)
	}

	// Read tableDef from schema file and check checksum.
	var tableDef cloudstorage.TableDefinition
	schemaContent, err := c.externalStorage.ReadFile(ctx, path)
	if err != nil {
		return errors.Trace(err)
	}
	err = json.Unmarshal(schemaContent, &tableDef)
	if err != nil {
		return errors.Trace(err)
	}
	checksumInMem, err := tableDef.Sum32(nil)
	if err != nil {
		return errors.Trace(err)
	}
	if checksumInMem != checksumInFile || schemaKey.TableVersion != tableDef.TableVersion {
		log.Panic("checksum mismatch",
			zap.Uint32("checksumInMem", checksumInMem),
			zap.Uint32("checksumInFile", checksumInFile),
			zap.Uint64("tableversionInMem", schemaKey.TableVersion),
			zap.Uint64("tableversionInFile", tableDef.TableVersion),
			zap.String("path", path))
	}

	// Update tableDefMap.
	c.tableDefMap[key][tableDef.TableVersion] = &tableDef

	// Fake a dml key for schema.json file, which is useful for putting DDL
	// in front of the DML files when sorting.
	// e.g, for the partitioned table:
	//
	// test/test1/439972354120482843/schema.json					(partitionNum = -1)
	// test/test1/439972354120482843/55/2023-03-09/CDC000001.csv	(partitionNum = 55)
	// test/test1/439972354120482843/66/2023-03-09/CDC000001.csv	(partitionNum = 66)
	//
	// and for the non-partitioned table:
	// test/test2/439972354120482843/schema.json				(partitionNum = -1)
	// test/test2/439972354120482843/2023-03-09/CDC000001.csv	(partitionNum = 0)
	// test/test2/439972354120482843/2023-03-09/CDC000002.csv	(partitionNum = 0)
	//
	// the DDL event recorded in schema.json should be executed first, then the DML events
	// in csv files can be executed.
	dmlkey := DmlPathKey{
		SchemaPathKey: schemaKey,
		PartitionNum:  fakePartitionNumForSchemaFile,
		Date:          "",
	}
	if _, ok := c.tableDMLIdxMap[dmlkey]; !ok {
		c.tableDMLIdxMap[dmlkey] = fileIndexKeyMap{}
	} else {
		// duplicate table schema file found, this should not happen.
		log.Panic("duplicate schema file found",
			zap.String("path", path), zap.Any("tableDef", tableDef),
			zap.Any("schemaKey", schemaKey), zap.Any("dmlkey", dmlkey))
	}
	return nil
}
