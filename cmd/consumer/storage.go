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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	putil "github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

const storageScanInterval = 2 * time.Second

type storageMetadata struct {
	CheckpointTs uint64 `json:"checkpoint-ts"`
}

type storageIndexRange struct {
	start uint64
	end   uint64
}

type storageIndex struct {
	path  string
	key   cloudstorage.DMLPathKey
	index cloudstorage.FileIndex
}

type storageInput struct {
	key      cloudstorage.DMLPathKey
	index    cloudstorage.FileIndex
	end      uint64 // The pending range is expanded one file at a time.
	groupEnd bool
	sort     bool
}
type storageTableKey struct {
	schema    string
	table     string
	partition int64
}

type storageReader struct {
	tables         map[cloudstorage.SchemaPathKey]*common.TableInfo
	tableIDs       map[storageTableKey]int64
	nextTableID    int64
	group          *readGroup
	groupKey       cloudstorage.DMLPathKey
	storage        storeapi.Storage
	memory         *memoryUsage
	dateSeparator  config.DateSeparator
	fileExtension  string
	fileIndexWidth int
	checkpoint     uint64
	readCheckpoint uint64
	schemas        map[cloudstorage.SchemaPathKey]*cloudstorage.SchemaFile
	schemaBytes    map[cloudstorage.SchemaPathKey]int64
	fileIndices    map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64
	ddlWatermarks  map[string]uint64
	mu             sync.Mutex
	records        []*ack
	inputs         []storageInput
	scanned        bool
}

func newStorageReader(ctx context.Context, upstreamURI *url.URL, replicaConfig *config.ReplicaConfig, memory *memoryUsage) (*storageReader, error) {
	if upstreamURI.Scheme == config.FileScheme && upstreamURI.Path == "" {
		return nil, errors.ErrInvalidReplicaConfig.FastGenByArgs("file upstream-uri must include a path")
	}
	if upstreamURI.Scheme != config.FileScheme && upstreamURI.Host == "" {
		return nil, errors.ErrInvalidReplicaConfig.FastGenByArgs("object storage upstream-uri must include a bucket")
	}
	if err := replicaConfig.ValidateAndAdjust(upstreamURI); err != nil {
		return nil, err
	}
	protocol, err := config.ParseSinkProtocolFromString(putil.GetOrZero(replicaConfig.Sink.Protocol))
	if err != nil {
		return nil, err
	}
	if protocol != config.ProtocolCsv && protocol != config.ProtocolCanalJSON {
		return nil, errors.ErrStorageSinkInvalidConfig.FastGenByArgs("Storage consumer requires csv or canal-json")
	}
	storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, upstreamURI.String())
	if err != nil {
		return nil, err
	}

	c := &storageReader{
		storage: storage, memory: memory,
		dateSeparator: putil.GetOrZero(replicaConfig.Sink.DateSeparator), fileExtension: helper.GetFileExtension(protocol),
		fileIndexWidth: putil.GetOrZero(replicaConfig.Sink.FileIndexWidth),
		schemas:        make(map[cloudstorage.SchemaPathKey]*cloudstorage.SchemaFile), fileIndices: make(map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64),
		schemaBytes:   make(map[cloudstorage.SchemaPathKey]int64),
		ddlWatermarks: make(map[string]uint64),
		tables:        make(map[cloudstorage.SchemaPathKey]*common.TableInfo),
		tableIDs:      make(map[storageTableKey]int64),
	}
	log.Info("Storage reader initialized", zap.String("protocol", protocol.String()))
	return c, nil
}

func (c *storageReader) readFile(ctx context.Context, path string, limit int64) (data []byte, err error) {
	if limit <= 0 {
		return nil, errors.ErrInternalCheckFailed.FastGenByArgs("consumer has no buffer capacity for a Storage file")
	}
	reader, err := c.storage.Open(ctx, path, nil)
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "open Storage file")
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil && err == nil {
			err = errors.WrapError(errors.ErrExternalStorageAPI, closeErr, "close Storage file")
		}
	}()
	data, err = io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "read Storage file")
	}
	if int64(len(data)) > limit {
		return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Storage file exceeds the consumer buffer limit")
	}
	return data, nil
}

func (c *storageReader) scanFiles(ctx context.Context) (map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]storageIndexRange, error) {
	// Read already rejects older schema versions after observing their DDL.
	// Retire those caches at the next scan, after its captured inputs are drained.
	for key, indices := range c.fileIndices {
		if key.TableVersion < c.ddlWatermarks[key.GetKey()] {
			bytes := int64(len(key.Schema) + len(key.Table) + len(key.Date) + 256)
			for index := range indices {
				bytes += int64(len(index.DispatcherID) + 128)
			}
			delete(c.fileIndices, key)
			c.memory.release(bytes)
		}
	}
	for key := range c.schemas {
		if key.TableVersion < c.ddlWatermarks[key.GetKey()] {
			delete(c.schemas, key)
			c.memory.release(c.schemaBytes[key])
			delete(c.schemaBytes, key)
			if table := c.tables[key]; table != nil {
				c.memory.releaseSchema(table)
				delete(c.tables, key)
			}
		}
	}
	files := make(map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]storageIndexRange)
	var paths []*storageIndex
	err := c.storage.WalkDir(ctx, &storeapi.WalkOption{}, func(path string, _ int64) error {
		if cloudstorage.IsSchemaFile(path) {
			key, added, err := c.readSchema(ctx, path)
			if err != nil {
				return err
			}
			if added {
				files[cloudstorage.NewSchemaFileDMLPathKey(key)] = nil
			}
			return nil
		}
		if !strings.HasSuffix(path, ".index") {
			return nil
		}
		var key cloudstorage.DMLPathKey
		if err := key.ParseIndexFilePath(c.dateSeparator, path); err != nil {
			return nil //nolint:nilerr // Ignore unsupported index paths in a shared Storage directory.
		}
		if (c.checkpoint > 0 && key.TableVersion > c.checkpoint) || key.TableVersion < c.ddlWatermarks[key.GetKey()] {
			return nil
		}
		paths = append(paths, &storageIndex{path: path, key: key})
		return nil
	})
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "scan Storage files")
	}
	if err := c.readIndices(ctx, paths); err != nil {
		return nil, err
	}
	for _, path := range paths {
		key, index := path.key, path.index
		indices := c.fileIndices[key]
		if indices == nil {
			if len(c.fileIndices) >= maxRecords {
				return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Storage path cache exceeds its count limit")
			}
			indices = make(map[cloudstorage.FileIndexKey]uint64)
			c.fileIndices[key] = indices
			bytes := int64(len(key.Schema) + len(key.Table) + len(key.Date) + 256)
			if err := c.memory.reserve(ctx, bytes); err != nil {
				return nil, err
			}
		}
		if _, known := indices[index.FileIndexKey]; !known {
			if len(indices) >= maxRecords {
				return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Storage index cache exceeds its resource limit")
			}
			indices[index.FileIndexKey] = 0
			bytes := int64(len(index.DispatcherID) + 128)
			if err := c.memory.reserve(ctx, bytes); err != nil {
				return nil, err
			}
		}

		previous := indices[index.FileIndexKey]
		if index.Idx > previous {
			if files[key] == nil {
				files[key] = make(map[cloudstorage.FileIndexKey]storageIndexRange)
			}
			files[key][index.FileIndexKey] = storageIndexRange{start: previous + 1, end: index.Idx}
		}
	}
	return files, nil
}

func (c *storageReader) readIndices(ctx context.Context, paths []*storageIndex) error {
	ctx, cancel := context.WithCancelCause(ctx)
	var wg sync.WaitGroup
	defer cancel(nil)
	workers := min(8, len(paths))
	for worker := range workers {
		wg.Go(func() {
			for index := worker; index < len(paths) && ctx.Err() == nil; index += workers {
				path := paths[index]
				data, err := c.readFile(ctx, path.path, maxRecordBytes)
				if err != nil {
					cancel(err)
					return
				}
				path.index, err = cloudstorage.ParseFileIndexFromFileName(strings.TrimSuffix(string(data), "\n"), c.fileExtension)
				if err != nil {
					cancel(err)
					return
				}
			}
		})
	}
	wg.Wait()
	return context.Cause(ctx)
}

func (c *storageReader) readSchema(ctx context.Context, path string) (cloudstorage.SchemaPathKey, bool, error) {
	var key cloudstorage.SchemaPathKey
	key.Parse(path)
	if (c.checkpoint > 0 && key.TableVersion > c.checkpoint) || c.schemas[key] != nil || key.TableVersion < c.ddlWatermarks[key.GetKey()] {
		return key, false, nil
	}
	data, err := c.readFile(ctx, path, maxRecordBytes)
	if err != nil {
		return key, false, err
	}
	var file cloudstorage.SchemaFile
	if err := json.Unmarshal(data, &file); err != nil {
		return key, false, errors.WrapError(errors.ErrCodecDecode, err, "decode Storage schema")
	}
	checksumText := strings.TrimSuffix(path[strings.LastIndex(path, "_")+1:], ".json")
	checksum, err := strconv.ParseUint(checksumText, 10, 32)
	if err != nil {
		return key, false, errors.WrapError(errors.ErrStorageSinkInvalidFileName, err, "parse Storage schema checksum")
	}
	if file.Checksum() != uint32(checksum) || key.TableVersion != file.TableVersion {
		return key, false, errors.ErrCodecDecode.FastGenByArgs("Storage schema checksum or table version does not match its path")
	}
	bytes := int64(len(data))*4 + 1024
	if err := c.memory.reserve(ctx, bytes); err != nil {
		return key, false, err
	}
	c.schemas[key] = &file
	if c.schemaBytes == nil {
		c.schemaBytes = make(map[cloudstorage.SchemaPathKey]int64)
	}
	c.schemaBytes[key] = bytes
	return key, true, nil
}

func (c *storageReader) Read(ctx context.Context) (*readData, error) {
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		if len(c.inputs) == 0 {
			if c.scanned && c.checkpoint > c.readCheckpoint {
				// The metadata checkpoint covers every file in the completed scan.
				return &readData{control: &readControl{watermark: c.checkpoint}}, nil
			}
			if c.scanned {
				select {
				case <-ctx.Done():
					return nil, context.Cause(ctx)
				case <-time.After(storageScanInterval):
				}
			}
			c.scanned = true
			exists, err := c.storage.FileExists(ctx, "metadata")
			if err != nil {
				return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "check Storage metadata")
			}
			if exists {
				data, err := c.readFile(ctx, "metadata", maxRecordBytes)
				if err != nil {
					return nil, err
				}
				var metadata storageMetadata
				if err := json.Unmarshal(data, &metadata); err != nil {
					return nil, errors.WrapError(errors.ErrCodecDecode, err, "decode Storage metadata")
				}
				c.checkpoint = max(c.checkpoint, metadata.CheckpointTs)
			}
			files, err := c.scanFiles(ctx)
			if err != nil {
				return nil, err
			}
			keys := slices.SortedFunc(maps.Keys(files), cloudstorage.CompareDMLPathKey)
			for _, key := range keys {
				if key.IsSchemaFileDMLPathKey() {
					if err := c.memory.reserve(ctx, 256); err != nil {
						return nil, err
					}
					c.inputs = append(c.inputs, storageInput{key: key})
					continue
				}
				sortBeforeWrite := false
				for indexKey := range files[key] {
					sortBeforeWrite = sortBeforeWrite || indexKey.EnableTableAcrossNodes
				}
				for indexKey, span := range files[key] {
					if err := c.memory.reserve(ctx, 256); err != nil {
						return nil, err
					}
					c.inputs = append(c.inputs, storageInput{key: key, index: cloudstorage.FileIndex{FileIndexKey: indexKey, Idx: span.start}, end: span.end, sort: sortBeforeWrite})
				}
				if err := c.memory.reserve(ctx, 256); err != nil {
					return nil, err
				}
				c.inputs = append(c.inputs, storageInput{key: key, groupEnd: true})
			}
			continue
		}
		input := c.inputs[0]
		key := input.key
		tableKey := key.GetKey()
		if input.index.Idx < input.end && key.TableVersion >= c.ddlWatermarks[tableKey] {
			c.inputs[0].index.Idx++
		} else {
			c.inputs[0] = storageInput{}
			c.inputs = c.inputs[1:]
			c.memory.release(256)
		}
		schema := c.schemas[input.key.SchemaPathKey]
		if schema == nil {
			return nil, errors.ErrCodecDecode.FastGenByArgs("Storage DML file has no matching schema")
		}
		if key.IsSchemaFileDMLPathKey() {
			if schema.Query == "" || key.TableVersion <= c.ddlWatermarks[tableKey] {
				continue
			}
			size := int64(len(schema.Query) + len(key.Schema) + len(key.Table) + 1024)
			record, err := c.memory.newAck(ctx, size)
			if err != nil {
				return nil, err
			}
			c.mu.Lock()
			c.records = append(c.records, record)
			c.mu.Unlock()
			table := c.tables[key.SchemaPathKey]
			if table == nil {
				table = schema.TableInfo()
				table.UpdateTS = schema.TableVersion
				if err := c.memory.retainSchema(ctx, table); err != nil {
					return nil, err
				}
				c.tables[key.SchemaPathKey] = table
			}
			ddl := schema.DDLEvent()
			ddl.TableInfo = table
			return &readData{table: table, ddl: ddl, record: record, retainedBytes: 256, ddlBoundary: &readBoundary{reached: true}}, nil
		}
		if key.TableVersion < c.ddlWatermarks[tableKey] {
			if !input.groupEnd {
				c.fileIndices[key][input.index.FileIndexKey] = max(input.index.Idx, input.end)
			}
			continue
		}
		table := c.tables[key.SchemaPathKey]
		if table == nil {
			table = schema.TableInfo()
			table.UpdateTS = schema.TableVersion
			if err := c.memory.retainSchema(ctx, table); err != nil {
				return nil, err
			}
			c.tables[key.SchemaPathKey] = table
		}
		idKey := storageTableKey{schema: key.Schema, table: key.Table, partition: key.PartitionNum}
		tableID := c.tableIDs[idKey]
		if tableID == 0 {
			if len(c.tableIDs) >= maxRecords {
				return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Storage table identity cache exceeds its count limit")
			}
			if err := c.memory.reserve(ctx, int64(len(key.Schema)+len(key.Table)+128)); err != nil {
				return nil, err
			}
			c.nextTableID++
			tableID = c.nextTableID
			c.tableIDs[idKey] = tableID
		}
		if c.group == nil || c.groupKey != key {
			order := inputOrder
			if input.sort {
				order = commitOrder
			}
			c.group = &readGroup{tableID: tableID, order: order, boundary: &readBoundary{reached: !input.sort}}
			c.groupKey = key
		}
		if input.groupEnd {
			return &readData{table: table, group: c.group, groupEnd: true}, nil
		}
		path := key.GenerateDMLFilePath(&input.index, c.fileExtension, c.fileIndexWidth)
		file, err := c.storage.Open(ctx, path, nil)
		if err != nil {
			return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "open Storage DML file")
		}
		size, err := file.GetFileSize()
		if err != nil {
			_ = file.Close()
			return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "get Storage DML file size")
		}
		if size < 0 {
			_ = file.Close()
			return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Storage DML file has a negative size")
		}
		record, err := c.memory.newAck(ctx, size+256)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		data := make([]byte, size)
		_, readErr := io.ReadFull(file, data)
		var extra [1]byte
		if readErr == nil {
			_, extraErr := io.ReadFull(file, extra[:])
			if extraErr != io.EOF {
				if extraErr != nil {
					readErr = extraErr
				} else {
					readErr = errors.ErrExternalStorageAPI.FastGenByArgs("Storage DML file size changed while reading")
				}
			}
		}
		closeErr := file.Close()
		if readErr != nil {
			return nil, errors.WrapError(errors.ErrExternalStorageAPI, readErr, "read Storage DML file")
		}
		if closeErr != nil {
			return nil, errors.WrapError(errors.ErrExternalStorageAPI, closeErr, "close Storage DML file")
		}
		c.mu.Lock()
		c.records = append(c.records, record)
		c.mu.Unlock()
		c.fileIndices[key][input.index.FileIndexKey] = input.index.Idx
		return &readData{format: rowFormat, value: data, table: table, group: c.group, record: record, retainedBytes: 256, dmlBoundary: c.group.boundary}, nil
	}
}

func (c *storageReader) Advance(ctx context.Context, feedback readFeedback) (readProgress, error) {
	if err := context.Cause(ctx); err != nil {
		return readProgress{}, err
	}
	data := feedback.data
	if data == nil {
		return readProgress{}, nil
	}
	if data.control != nil {
		c.readCheckpoint = max(c.readCheckpoint, data.control.watermark)
		return readProgress{control: data.control}, nil
	}
	if feedback.ddl != nil {
		ddl := feedback.ddl
		key := common.QuoteSchema(data.table.GetSchemaName(), data.table.GetTableName())
		c.ddlWatermarks[key] = max(c.ddlWatermarks[key], data.table.UpdateTS)
		if ddl.Type == byte(timodel.ActionRenameTable) {
			if len(ddl.BlockedTableNames) == 0 {
				return readProgress{}, errors.ErrCodecDecode.FastGenByArgs("Storage rename DDL has no old table")
			}
			old := ddl.BlockedTableNames[0]
			oldKey := common.QuoteSchema(old.SchemaName, old.TableName)
			c.ddlWatermarks[oldKey] = max(c.ddlWatermarks[oldKey], data.table.UpdateTS)
		}
	}
	if feedback.dml != nil {
		if feedback.dml.GetCommitTs() < c.readCheckpoint {
			return readProgress{skip: true}, nil
		}
	}
	if data.groupEnd {
		data.group.boundary.reached = true
		c.group = nil
	}
	return readProgress{}, nil
}

func (c *storageReader) Confirm(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, record := range c.records {
		refs := record.refs.Load()
		if refs < 0 {
			return errors.ErrInternalCheckFailed.FastGenByArgs("Storage input completed more than once")
		}
		if refs != 0 {
			break
		}
		c.memory.confirm(record)
		count++
	}
	clear(c.records[:count])
	c.records = c.records[count:]
	return nil
}

func (c *storageReader) Close() error {
	c.storage.Close()
	return nil
}
