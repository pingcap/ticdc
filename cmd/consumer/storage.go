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
	"cmp"
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
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	putil "github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

type storageSchema struct {
	file  *cloudstorage.SchemaFile
	table *common.TableInfo
	bytes int64
}

type storageReader struct {
	tableIDs       map[storageTableKey]int64
	nextTableID    int64
	group          *readGroup
	groupKey       cloudstorage.DMLPathKey
	storage        storeapi.Storage
	memory         *memoryUsage
	progress       *inputProgress
	schemaUsage    *schemaUsage
	confirmable    chan<- struct{}
	dateSeparator  config.DateSeparator
	fileExtension  string
	fileIndexWidth int
	sortCSVRows    bool
	checkpoint     uint64
	readCheckpoint uint64
	schemas        map[cloudstorage.SchemaPathKey]*storageSchema
	fileIndices    map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64
	ddlWatermarks  map[string]uint64
	mu             sync.Mutex
	records        []*ack
	inputs         []storageInput
	scanned        bool
}

func newStorageReader(ctx context.Context, upstreamURI *url.URL, replicaConfig *config.ReplicaConfig, progress *inputProgress, schemas *schemaUsage, confirmable chan<- struct{}) *storageReader {
	if upstreamURI.Scheme == config.FileScheme && upstreamURI.Path == "" {
		log.Panic("file upstream URI must include a path")
	}
	if upstreamURI.Scheme != config.FileScheme && upstreamURI.Host == "" {
		log.Panic("object storage upstream URI must include a bucket")
	}
	if err := replicaConfig.ValidateAndAdjust(upstreamURI); err != nil {
		log.Panic("Storage configuration is invalid", zap.Error(err))
	}
	protocol, err := config.ParseSinkProtocolFromString(putil.GetOrZero(replicaConfig.Sink.Protocol))
	if err != nil {
		log.Panic("Storage protocol is invalid", zap.Error(err))
	}
	if protocol != config.ProtocolCsv && protocol != config.ProtocolCanalJSON {
		log.Panic("Storage consumer requires CSV or Canal JSON", zap.String("protocol", protocol.String()))
	}
	storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, upstreamURI.String())
	if err != nil {
		log.Panic("Storage client initialization failed", zap.Error(err))
	}

	c := &storageReader{
		storage:        storage,
		memory:         progress.memory,
		progress:       progress,
		schemaUsage:    schemas,
		confirmable:    confirmable,
		dateSeparator:  putil.GetOrZero(replicaConfig.Sink.DateSeparator),
		fileExtension:  helper.GetFileExtension(protocol),
		fileIndexWidth: putil.GetOrZero(replicaConfig.Sink.FileIndexWidth),
		sortCSVRows:    protocol == config.ProtocolCsv && replicaConfig.Sink.CSVConfig.OutputOldValue && replicaConfig.Sink.CSVConfig.IncludeCommitTs,
		schemas:        make(map[cloudstorage.SchemaPathKey]*storageSchema),
		fileIndices:    make(map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64),
		ddlWatermarks:  make(map[string]uint64),
		tableIDs:       make(map[storageTableKey]int64),
	}
	log.Info("Storage reader initialized", zap.String("protocol", protocol.String()))
	return c
}

func (c *storageReader) readFile(ctx context.Context, path string) ([]byte, error) {
	reader, err := c.storage.Open(ctx, path, nil)
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "open Storage file")
	}
	defer func() {
		if err := reader.Close(); err != nil {
			log.Warn("Storage input file close failed", zap.String("path", path), zap.Error(err))
		}
	}()
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "read Storage file")
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
	for key, schema := range c.schemas {
		if key.TableVersion < c.ddlWatermarks[key.GetKey()] {
			delete(c.schemas, key)
			c.memory.release(schema.bytes)
			c.schemaUsage.release(schema.table)
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
		paths = append(paths, &storageIndex{
			path: path,
			key:  key,
		})
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
			indices = make(map[cloudstorage.FileIndexKey]uint64)
			c.fileIndices[key] = indices
			bytes := int64(len(key.Schema) + len(key.Table) + len(key.Date) + 256)
			c.memory.reserve(bytes)
		}
		if _, known := indices[index.FileIndexKey]; !known {
			indices[index.FileIndexKey] = 0
			bytes := int64(len(index.DispatcherID) + 128)
			c.memory.reserve(bytes)
		}

		previous := indices[index.FileIndexKey]
		if index.Idx > previous {
			if files[key] == nil {
				files[key] = make(map[cloudstorage.FileIndexKey]storageIndexRange)
			}
			files[key][index.FileIndexKey] = storageIndexRange{
				start: previous + 1,
				end:   index.Idx,
			}
		}
	}
	return files, nil
}

func (c *storageReader) readIndices(ctx context.Context, paths []*storageIndex) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	g, ctx := errgroup.WithContext(ctx)
	workers := min(8, len(paths))
	for worker := range workers {
		g.Go(func() error {
			for index := worker; index < len(paths) && ctx.Err() == nil; index += workers {
				path := paths[index]
				data, err := c.readFile(ctx, path.path)
				if err != nil {
					return err
				}
				path.index, err = cloudstorage.ParseFileIndexFromFileName(strings.TrimSuffix(string(data), "\n"), c.fileExtension)
				if err != nil {
					return err
				}
			}
			return context.Cause(ctx)
		})
	}
	return g.Wait()
}

func (c *storageReader) readSchema(ctx context.Context, path string) (cloudstorage.SchemaPathKey, bool, error) {
	var key cloudstorage.SchemaPathKey
	key.Parse(path)
	if (c.checkpoint > 0 && key.TableVersion > c.checkpoint) || c.schemas[key] != nil || key.TableVersion < c.ddlWatermarks[key.GetKey()] {
		return key, false, nil
	}
	data, err := c.readFile(ctx, path)
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
	c.memory.reserve(bytes)
	c.schemas[key] = &storageSchema{
		file:  &file,
		bytes: bytes,
	}
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
			if err := c.scanInputs(ctx); err != nil {
				return nil, err
			}
			continue
		}
		input := c.inputs[0]
		key := input.key
		tableKey := key.GetKey()
		switch {
		case input.index.Idx < input.end && key.TableVersion >= c.ddlWatermarks[tableKey]:
			c.inputs[0].index.Idx++
		default:
			c.inputs[0] = storageInput{}
			c.inputs = c.inputs[1:]
			c.memory.release(256)
		}
		schema := c.schemas[input.key.SchemaPathKey]
		if schema == nil {
			log.Panic("Storage DML file has no matching schema", zap.String("schema", key.Schema), zap.String("table", key.Table), zap.Uint64("tableVersion", key.TableVersion))
		}
		if key.IsSchemaFileDMLPathKey() && (schema.file.Query == "" || key.TableVersion <= c.ddlWatermarks[tableKey]) {
			continue
		}
		if key.TableVersion < c.ddlWatermarks[tableKey] {
			if !input.groupEnd {
				c.fileIndices[key][input.index.FileIndexKey] = max(input.index.Idx, input.end)
			}
			continue
		}
		table := schema.table
		if table == nil {
			table = schema.file.TableInfo()
			table.UpdateTS = schema.file.TableVersion
			c.schemaUsage.retain(table)
			schema.table = table
		}
		if key.IsSchemaFileDMLPathKey() {
			return c.readDDL(key, schema), nil
		}
		idKey := storageTableKey{
			schema:    key.Schema,
			table:     key.Table,
			partition: key.PartitionNum,
		}
		tableID := c.tableIDs[idKey]
		if tableID == 0 {
			c.memory.reserve(int64(len(key.Schema) + len(key.Table) + 128))
			c.nextTableID++
			tableID = c.nextTableID
			c.tableIDs[idKey] = tableID
		}
		if c.group == nil || c.groupKey != key {
			var compare func(first, second *event.DMLEvent) int
			if input.sort {
				compare = c.compareRows
			}
			c.group = &readGroup{
				tableID:  tableID,
				compare:  compare,
				boundary: &readBoundary{reached: !input.sort},
			}
			c.groupKey = key
		}
		if input.groupEnd {
			return &readData{
				table:    table,
				group:    c.group,
				groupEnd: true,
			}, nil
		}
		return c.readDMLFile(ctx, input, table)
	}
}

// File sequences from different dispatchers overlap in commitTs. Storage owns
// their merge order; CSV old-value rows additionally delete before inserting.
func (c *storageReader) compareRows(first, second *event.DMLEvent) int {
	order := cmp.Compare(first.CommitTs, second.CommitTs)
	if order != 0 || !c.sortCSVRows {
		return order
	}
	return cmp.Compare(first.RowTypes[0], second.RowTypes[0])
}

func (c *storageReader) scanInputs(ctx context.Context) error {
	if c.scanned {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(storageScanInterval):
		}
	}
	c.scanned = true
	exists, err := c.storage.FileExists(ctx, "metadata")
	if err != nil {
		return errors.WrapError(errors.ErrExternalStorageAPI, err, "check Storage metadata")
	}
	if exists {
		data, err := c.readFile(ctx, "metadata")
		if err != nil {
			return err
		}
		var metadata storageMetadata
		if err := json.Unmarshal(data, &metadata); err != nil {
			return errors.WrapError(errors.ErrCodecDecode, err, "decode Storage metadata")
		}
		c.checkpoint = max(c.checkpoint, metadata.CheckpointTs)
	}
	files, err := c.scanFiles(ctx)
	if err != nil {
		return err
	}
	keys := slices.SortedFunc(maps.Keys(files), cloudstorage.CompareDMLPathKey)
	for _, key := range keys {
		if key.IsSchemaFileDMLPathKey() {
			c.memory.reserve(256)
			c.inputs = append(c.inputs, storageInput{key: key})
			continue
		}
		var sortBeforeWrite bool
		for indexKey := range files[key] {
			sortBeforeWrite = sortBeforeWrite || indexKey.EnableTableAcrossNodes
		}
		for indexKey, span := range files[key] {
			c.memory.reserve(256)
			c.inputs = append(c.inputs, storageInput{
				key: key,
				index: cloudstorage.FileIndex{
					FileIndexKey: indexKey,
					Idx:          span.start,
				},
				end:  span.end,
				sort: sortBeforeWrite,
			})
		}
		c.memory.reserve(256)
		c.inputs = append(c.inputs, storageInput{
			key:      key,
			groupEnd: true,
		})
	}
	return nil
}

func (c *storageReader) readDDL(key cloudstorage.DMLPathKey, schema *storageSchema) *readData {
	size := int64(len(schema.file.Query) + len(key.Schema) + len(key.Table) + 1024)
	record := c.progress.newAck(size, c.confirmable)
	c.mu.Lock()
	c.records = append(c.records, record)
	c.mu.Unlock()
	scope := event.InfluenceTypeNormal
	switch {
	case schema.file.Schema == "" || timodel.ActionType(schema.file.Type) == timodel.ActionCreateView:
		scope = event.InfluenceTypeAll
	case schema.file.Table == "":
		scope = event.InfluenceTypeDB
	}
	// Reuse the cached table instead of reconstructing its columns again.
	ddl := &event.DDLEvent{
		TableInfo:     schema.table,
		FinishedTs:    schema.file.TableVersion,
		Type:          schema.file.Type,
		Query:         schema.file.Query,
		SchemaName:    schema.file.Schema,
		TableName:     schema.file.Table,
		BlockedTables: &event.InfluencedTables{InfluenceType: scope},
	}
	return &readData{
		table:         schema.table,
		ddl:           ddl,
		record:        record,
		retainedBytes: 256,
		ddlBoundary:   &readBoundary{reached: true},
	}
}

func (c *storageReader) readDMLFile(ctx context.Context, input storageInput, table *common.TableInfo) (*readData, error) {
	path := input.key.GenerateDMLFilePath(&input.index, c.fileExtension, c.fileIndexWidth)
	file, err := c.storage.Open(ctx, path, nil)
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "open Storage DML file")
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Warn("Storage input file close failed", zap.String("path", path), zap.Error(err))
		}
	}()
	size, err := file.GetFileSize()
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "get Storage DML file size")
	}
	record := c.progress.newAck(size+256, c.confirmable)
	data := make([]byte, size)
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "read Storage DML file")
	}
	c.mu.Lock()
	c.records = append(c.records, record)
	c.mu.Unlock()
	c.fileIndices[input.key][input.index.FileIndexKey] = input.index.Idx
	return &readData{
		format:        rowFormat,
		value:         data,
		table:         table,
		group:         c.group,
		record:        record,
		retainedBytes: 256,
		dmlBoundary:   c.group.boundary,
	}, nil
}

func (c *storageReader) Advance(_ context.Context, feedback readFeedback) (readProgress, error) {
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

func (c *storageReader) Confirm(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var count int
	for _, record := range c.records {
		if record.refs.Load() != 0 {
			break
		}
		record.confirm()
		count++
	}
	clear(c.records[:count])
	c.records = c.records[count:]
	return nil
}

func (c *storageReader) Close() {
	c.storage.Close()
}
