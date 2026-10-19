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
	"iter"
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
	"github.com/pingcap/ticdc/pkg/sink/codec/canal"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/csv"
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
	key   cloudstorage.DMLPathKey
	index cloudstorage.FileIndex
	end   uint64 // The pending range is expanded one file at a time.
}
type storageTableKey struct {
	schema    string
	table     string
	partition int64
}

type storageReader struct {
	*readLimit
	tableIDs       map[storageTableKey]int64
	nextTableID    int64
	storage        storeapi.Storage
	memory         *memoryUsage
	confirmable    chan<- struct{}
	dateSeparator  config.DateSeparator
	fileExtension  string
	fileIndexWidth int
	scanInterval   time.Duration
	checkpoint     uint64
	readCheckpoint uint64
	schemas        map[cloudstorage.SchemaPathKey]*readSchema
	fileIndices    map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64
	ddlWatermarks  map[string]uint64
	pendingSchema  chan struct{} // Header decoding determines the next file groups.
	mu             sync.Mutex
	records        []*ack
	inputs         []storageInput
	scanned        bool
}

func newStorageReader(ctx context.Context, upstreamURI *url.URL, replicaConfig *config.ReplicaConfig, memory *memoryUsage, confirmable chan<- struct{}) *storageReader {
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
		readLimit:      newReadLimit(memory),
		storage:        storage,
		memory:         memory,
		confirmable:    confirmable,
		dateSeparator:  putil.GetOrZero(replicaConfig.Sink.DateSeparator),
		fileExtension:  helper.GetFileExtension(protocol),
		fileIndexWidth: putil.GetOrZero(replicaConfig.Sink.FileIndexWidth),
		scanInterval:   storageScanInterval,
		schemas:        make(map[cloudstorage.SchemaPathKey]*readSchema),
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
	for key := range c.fileIndices {
		if key.TableVersion < c.ddlWatermarks[key.GetKey()] {
			delete(c.fileIndices, key)
		}
	}
	for key := range c.schemas {
		if key.TableVersion < c.ddlWatermarks[key.GetKey()] {
			delete(c.schemas, key)
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
		}
		if _, known := indices[index.FileIndexKey]; !known {
			indices[index.FileIndexKey] = 0
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
	c.schemas[key] = &readSchema{
		file: &file,
	}
	return key, true, nil
}

type (
	storageRead interface{ storageRead() }
	storageFile struct {
		contents []byte
		schema   *readSchema
		tableID  int64
		record   *ack
	}
)

type storageDDL struct {
	schema     *readSchema
	ddl        *event.DDLEvent
	record     *ack
	registered chan struct{}
}
type storageCheckpoint uint64

func (*storageFile) storageRead()      {}
func (*storageDDL) storageRead()       {}
func (storageCheckpoint) storageRead() {}

func (c *storageReader) Run(ctx context.Context, parser *inputParser, tasks chan<- []task) error {
	return readAndParse(ctx, c.Read, func(ctx context.Context, input storageRead) iter.Seq2[task, error] {
		return c.parse(ctx, parser, input)
	}, tasks)
}

func (c *storageReader) parse(ctx context.Context, parser *inputParser, input storageRead) iter.Seq2[task, error] {
	if file, ok := input.(*storageFile); ok {
		return c.parseFile(ctx, parser, file)
	}
	return func(yield func(task, error) bool) {
		switch input := input.(type) {
		case *storageDDL:
			input.ddl.TableInfo = input.schema.tableInfo()
			input.record.retain()
			if !yield(&inputDDL{ddl: input.ddl, scope: inputScope{record: input.record, ddlBoundary: &readBoundary{reached: true}}, registered: func() { c.registerDDL(input) }}, nil) {
				return
			}
			input.record.release()
		case storageCheckpoint:
			yield(&checkpoint{watermark: uint64(input)}, nil)
		}
	}
}

func (c *storageReader) Read(ctx context.Context) ([]storageRead, error) {
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		if c.pendingSchema != nil {
			select {
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			case <-c.pendingSchema:
				c.pendingSchema = nil
			}
		}
		if len(c.inputs) == 0 {
			if c.scanned && c.checkpoint > c.readCheckpoint {
				c.readCheckpoint = c.checkpoint
				// The metadata checkpoint covers every file in the completed scan.
				return []storageRead{storageCheckpoint(c.checkpoint)}, nil
			}
			if err := c.scanInputs(ctx); err != nil {
				return nil, err
			}
			continue
		}
		input := c.inputs[0]
		key := input.key
		tableKey := key.GetKey()
		c.mu.Lock()
		ddlWatermark := c.ddlWatermarks[tableKey]
		c.mu.Unlock()
		switch {
		case input.index.Idx < input.end && key.TableVersion >= ddlWatermark:
			c.inputs[0].index.Idx++
		default:
			c.inputs[0] = storageInput{}
			c.inputs = c.inputs[1:]
		}
		schema := c.schemas[input.key.SchemaPathKey]
		if schema == nil {
			log.Panic("Storage DML file has no matching schema", zap.String("schema", key.Schema), zap.String("table", key.Table), zap.Uint64("tableVersion", key.TableVersion))
		}
		if key.IsSchemaFileDMLPathKey() && (schema.file.Query == "" || key.TableVersion <= ddlWatermark) {
			continue
		}
		if key.TableVersion < ddlWatermark {
			c.fileIndices[key][input.index.FileIndexKey] = max(input.index.Idx, input.end)
			continue
		}
		if key.IsSchemaFileDMLPathKey() {
			if err := c.wait(ctx); err != nil {
				return nil, err
			}
			return []storageRead{c.readDDL(key, schema)}, nil
		}
		idKey := storageTableKey{
			schema:    key.Schema,
			table:     key.Table,
			partition: key.PartitionNum,
		}
		tableID := c.tableIDs[idKey]
		if tableID == 0 {
			c.nextTableID++
			tableID = c.nextTableID
			c.tableIDs[idKey] = tableID
		}
		if err := c.wait(ctx); err != nil {
			return nil, err
		}
		data, err := c.readDMLFile(ctx, input, schema, tableID)
		if err != nil {
			return nil, err
		}
		return []storageRead{data}, nil
	}
}

func (c *storageReader) scanInputs(ctx context.Context) error {
	if c.scanned {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(c.scanInterval):
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
			c.inputs = append(c.inputs, storageInput{key: key})
			continue
		}
		for indexKey, span := range files[key] {
			c.inputs = append(c.inputs, storageInput{
				key: key,
				index: cloudstorage.FileIndex{
					FileIndexKey: indexKey,
					Idx:          span.start,
				},
				end: span.end,
			})
		}
	}
	return nil
}

func (c *storageReader) readDDL(key cloudstorage.DMLPathKey, schema *readSchema) *storageDDL {
	c.pendingSchema = make(chan struct{})
	size := int64(len(schema.file.Query) + len(key.Schema) + len(key.Table))
	record := newAck(c.memory, size, c.confirmable)
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
	ddl := &event.DDLEvent{
		FinishedTs:    schema.file.TableVersion,
		Type:          schema.file.Type,
		Query:         schema.file.Query,
		SchemaName:    schema.file.Schema,
		TableName:     schema.file.Table,
		BlockedTables: &event.InfluencedTables{InfluenceType: scope},
	}
	return &storageDDL{schema: schema, ddl: ddl, record: record, registered: c.pendingSchema}
}

func (c *storageReader) readDMLFile(ctx context.Context, input storageInput, schema *readSchema, tableID int64) (*storageFile, error) {
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
	record := newAck(c.memory, size, c.confirmable)
	data := make([]byte, size)
	if _, err := io.ReadFull(file, data); err != nil {
		record.release()
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "read Storage DML file")
	}
	c.mu.Lock()
	c.records = append(c.records, record)
	c.mu.Unlock()
	c.fileIndices[input.key][input.index.FileIndexKey] = input.index.Idx
	return &storageFile{contents: data, schema: schema, tableID: tableID, record: record}, nil
}

func (c *storageReader) Advance(_ []readUpdate) {}

// The source can read the next schema only after the conductor registers this DDL.
func (c *storageReader) registerDDL(input *storageDDL) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ddl, file := input.ddl, input.schema.file
	key := common.QuoteSchema(file.Schema, file.Table)
	c.ddlWatermarks[key] = max(c.ddlWatermarks[key], file.TableVersion)
	if ddl.Type == byte(timodel.ActionRenameTable) {
		old := ddl.BlockedTableNames[0]
		oldKey := common.QuoteSchema(old.SchemaName, old.TableName)
		c.ddlWatermarks[oldKey] = max(c.ddlWatermarks[oldKey], file.TableVersion)
	}
	close(input.registered)
}

func (c *storageReader) Boundary(_ []event.SchemaTableName, _ uint64) *readBoundary {
	// File rows are ready for writing without waiting for other files.
	return nil
}

func (c *storageReader) Confirm(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var count int
	for _, record := range c.records {
		if !record.ready() {
			break
		}
		count++
	}
	clear(c.records[:count])
	c.records = c.records[count:]
	return nil
}

func (c *storageReader) Close() {
	c.storage.Close()
}

// The reader supplies schema contents. Its parser materializes TableInfo;
// conversion workers receive that captured immutable metadata.
type readSchema struct {
	file  *cloudstorage.SchemaFile
	table *common.TableInfo
}

func (s *readSchema) tableInfo() *common.TableInfo {
	if s.table == nil {
		s.table = s.file.TableInfo()
		s.table.UpdateTS = s.file.TableVersion
	}
	return s.table
}

func (c *storageReader) parseFile(ctx context.Context, p *inputParser, file *storageFile) iter.Seq2[task, error] {
	return func(yield func(task, error) bool) {
		table := file.schema.tableInfo()
		var decoder codecCommon.Decoder
		switch p.codecConfig.Protocol {
		case config.ProtocolCsv:
			var err error
			decoder, err = csv.NewDecoderWithColumnSelector(ctx, p.codecConfig, table, file.contents, p.selectors.GetForTableInfo(table))
			if err != nil {
				yield(nil, errors.WrapError(errors.ErrCodecDecode, err, "create CSV decoder"))
				return
			}
		case config.ProtocolCanalJSON:
			decoder = canal.NewTxnDecoder(p.codecConfig)
			decoder.AddKeyValue(nil, file.contents)
		}
		scope := inputScope{record: file.record, dmlBoundary: &readBoundary{reached: true}}
		for {
			if err := context.Cause(ctx); err != nil {
				yield(nil, err)
				return
			}
			kind, more := decoder.HasNext()
			if !more {
				break
			}
			if kind != codecCommon.MessageTypeRow {
				continue
			}
			task := p.messageTask(scope, decoder.NextDMLMessage())
			task.table, task.tableID = table, file.tableID
			if !yield(task, nil) {
				return
			}
		}
		file.record.release()
	}
}
