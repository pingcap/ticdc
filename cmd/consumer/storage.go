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
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/canal"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/csv"
	putil "github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
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

type storageSchema struct {
	file      cloudstorage.SchemaFile
	tableInfo *common.TableInfo
}

type storageTableKey struct {
	schema    string
	table     string
	partition int64
}

type storageInput struct {
	key      cloudstorage.DMLPathKey
	index    cloudstorage.FileIndex
	groupEnd bool
	tableID  int64
	sort     bool
}
type storagePosition struct {
	key   cloudstorage.DMLPathKey
	index cloudstorage.FileIndex
}
type storageReader struct {
	storage          storeapi.Storage
	buffer           *readBuffer
	codecConfig      *codecCommon.Config
	columnSelectors  *columnselector.ColumnSelectors
	dateSeparator    config.DateSeparator
	fileExtension    string
	fileIndexWidth   int
	checkpoint       uint64
	schemas          map[cloudstorage.SchemaPathKey]*storageSchema
	fileIndices      map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64
	confirmedIndices map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64
	ddlWatermarks    map[string]uint64
	tableIDs         map[storageTableKey]int64
	tableWatermarks  map[int64]uint64
	nextTableID      int64
	mu               sync.Mutex
	records          []*inputRecord
	positions        map[*inputRecord]storagePosition
	inputs           []storageInput
	current          storageInput
	decoder          codecCommon.Decoder
	record           *inputRecord
	sortBeforeWrite  bool
	groupReady       bool
	scanned          bool
}

func newStorageReader(ctx context.Context, upstreamURI *url.URL, timezone string, replicaConfig *config.ReplicaConfig, memory *bufferUsage) (*storageReader, error) {
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
	codecConfig := codecCommon.NewConfig(protocol)
	if err := codecConfig.Apply(upstreamURI, replicaConfig.Sink); err != nil {
		return nil, err
	}
	codecConfig.TimeZone, err = putil.GetTimezone(timezone)
	if err != nil {
		return nil, err
	}
	if protocol == config.ProtocolCanalJSON {
		codecConfig.EnableTiDBExtension = true
	}
	selectors, err := columnselector.New(replicaConfig.Sink, putil.GetOrZero(replicaConfig.CaseSensitive))
	if err != nil {
		return nil, err
	}
	storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, upstreamURI.String())
	if err != nil {
		return nil, err
	}

	c := &storageReader{
		storage: storage, buffer: &readBuffer{memory: memory}, codecConfig: codecConfig, columnSelectors: selectors,
		dateSeparator: putil.GetOrZero(replicaConfig.Sink.DateSeparator), fileExtension: helper.GetFileExtension(protocol),
		fileIndexWidth: putil.GetOrZero(replicaConfig.Sink.FileIndexWidth),
		schemas:        make(map[cloudstorage.SchemaPathKey]*storageSchema), fileIndices: make(map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64),
		confirmedIndices: make(map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64),
		positions:        make(map[*inputRecord]storagePosition),
		ddlWatermarks:    make(map[string]uint64), tableIDs: make(map[storageTableKey]int64), tableWatermarks: make(map[int64]uint64),
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
	files := make(map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]storageIndexRange)
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
		if c.checkpoint > 0 && key.TableVersion > c.checkpoint {
			return nil
		}
		data, err := c.readFile(ctx, path, maxRecordBytes)
		if err != nil {
			return err
		}
		index, err := cloudstorage.ParseFileIndexFromFileName(strings.TrimSuffix(string(data), "\n"), c.fileExtension)
		if err != nil {
			return err
		}
		indices := c.fileIndices[key]
		if indices == nil {
			if len(c.fileIndices) >= maxRecords {
				return errors.ErrInternalCheckFailed.FastGenByArgs("Storage path cache exceeds its count limit")
			}
			indices = make(map[cloudstorage.FileIndexKey]uint64)
			c.fileIndices[key] = indices
			bytes := int64(len(key.Schema) + len(key.Table) + len(key.Date) + 256)
			if err := c.buffer.memory.reserve(ctx, bytes); err != nil {
				return err
			}
			c.buffer.memory.readBytes.Add(bytes)
		}
		if _, known := indices[index.FileIndexKey]; !known {
			if len(indices) >= maxRecords {
				return errors.ErrInternalCheckFailed.FastGenByArgs("Storage index cache exceeds its resource limit")
			}
			indices[index.FileIndexKey] = 0
			bytes := int64(len(index.DispatcherID) + 128)
			if err := c.buffer.memory.reserve(ctx, bytes); err != nil {
				return err
			}
			c.buffer.memory.readBytes.Add(bytes)
		}

		previous := indices[index.FileIndexKey]
		if index.Idx > previous {
			if files[key] == nil {
				files[key] = make(map[cloudstorage.FileIndexKey]storageIndexRange)
			}
			files[key][index.FileIndexKey] = storageIndexRange{start: previous + 1, end: index.Idx}
		}
		return nil
	})
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "scan Storage files")
	}
	return files, nil
}

func (c *storageReader) readSchema(ctx context.Context, path string) (cloudstorage.SchemaPathKey, bool, error) {
	var key cloudstorage.SchemaPathKey
	key.Parse(path)
	if (c.checkpoint > 0 && key.TableVersion > c.checkpoint) || c.schemas[key] != nil {
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
	if err := c.buffer.memory.reserve(ctx, bytes); err != nil {
		return key, false, err
	}
	c.buffer.memory.readBytes.Add(bytes)
	table := file.TableInfo()
	table.UpdateTS = file.TableVersion
	c.schemas[key] = &storageSchema{file: file, tableInfo: table}
	return key, true, nil
}

func (c *storageReader) Read(ctx context.Context) (*readResult, error) {
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		if !c.sortBeforeWrite || c.groupReady {
			if result := c.buffer.nextReady(^uint64(0)); result != nil {
				return result, nil
			}
		}
		if c.groupReady {
			c.groupReady = false
			return &readResult{tableID: c.current.tableID, watermark: c.tableWatermarks[c.current.tableID], hasWatermark: true}, nil
		}
		if c.decoder != nil {
			messageType, hasNext := c.decoder.HasNext()
			if hasNext {
				if messageType != codecCommon.MessageTypeRow {
					continue
				}
				message := c.decoder.NextDMLMessage()
				if message == nil {
					return nil, errors.ErrCodecDecode.FastGenByArgs("Storage decoder returned an empty DML message")
				}
				tableID := c.current.tableID
				if !c.current.index.EnableTableAcrossNodes && message.GetCommitTs() < c.tableWatermarks[tableID] {
					continue
				}
				c.tableWatermarks[tableID] = max(c.tableWatermarks[tableID], message.GetCommitTs())
				dml := message.ToDMLEvent()
				if dml == nil || dml.TableInfo == nil {
					return nil, errors.ErrCodecDecode.FastGenByArgs("Storage DML message has no table metadata")
				}
				dml.PhysicalTableID = tableID
				if c.codecConfig.Protocol == config.ProtocolCanalJSON {
					dml.TableInfo.UpdateTS = c.current.key.TableVersion
				}
				if err := c.buffer.queueDML(ctx, dml, []*inputRecord{c.record}, nil); err != nil {
					return nil, err
				}
				continue
			}
			// All rows have their own write references before decoding is released.
			size := c.record.bytes.Swap(256)
			c.buffer.memory.release(size - 256)
			c.buffer.memory.readBytes.Add(256 - size)
			c.record.pending.Add(-1)
			c.buffer.memory.effects.Add(-1)
			select {
			case c.buffer.memory.completed <- struct{}{}:
			default:
			}
			c.fileIndices[c.current.key][c.current.index.FileIndexKey] = c.current.index.Idx
			c.decoder = nil
			c.record = nil
			continue
		}
		if len(c.inputs) == 0 {
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
					if err := c.buffer.memory.reserve(ctx, 256); err != nil {
						return nil, err
					}
					c.buffer.memory.readBytes.Add(256)
					c.inputs = append(c.inputs, storageInput{key: key})
					continue
				}
				sortBeforeWrite := false
				for indexKey := range files[key] {
					sortBeforeWrite = sortBeforeWrite || indexKey.EnableTableAcrossNodes
				}
				for indexKey, span := range files[key] {
					for index := span.start; ; index++ {
						if err := c.buffer.memory.reserve(ctx, 256); err != nil {
							return nil, err
						}
						c.buffer.memory.readBytes.Add(256)
						c.inputs = append(c.inputs, storageInput{key: key, index: cloudstorage.FileIndex{FileIndexKey: indexKey, Idx: index}, sort: sortBeforeWrite})
						if len(c.inputs) > maxRecords {
							return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Storage scan exceeds its input limit")
						}
						if index == span.end {
							break
						}
					}
				}
				if err := c.buffer.memory.reserve(ctx, 256); err != nil {
					return nil, err
				}
				c.buffer.memory.readBytes.Add(256)
				c.inputs = append(c.inputs, storageInput{key: key, groupEnd: true})
			}
			continue
		}
		input := c.inputs[0]
		c.inputs[0] = storageInput{}
		c.inputs = c.inputs[1:]
		c.buffer.memory.release(256)
		c.buffer.memory.readBytes.Add(-256)
		schema := c.schemas[input.key.SchemaPathKey]
		if schema == nil {
			return nil, errors.ErrCodecDecode.FastGenByArgs("Storage DML file has no matching schema")
		}
		key := input.key
		tableKey := key.GetKey()
		if key.IsSchemaFileDMLPathKey() {
			if schema.file.Query == "" || key.TableVersion <= c.ddlWatermarks[tableKey] {
				continue
			}
			oldKey := ""
			if schema.file.Type == byte(timodel.ActionRenameTable) {
				statement, err := parser.New().ParseOneStmt(schema.file.Query, "", "")
				if err != nil {
					return nil, errors.WrapError(errors.ErrCodecDecode, err, "parse Storage rename DDL")
				}
				rename, ok := statement.(*ast.RenameTableStmt)
				if !ok || len(rename.TableToTables) == 0 {
					return nil, errors.ErrCodecDecode.FastGenByArgs("Storage rename DDL has no old table")
				}
				old := rename.TableToTables[0].OldTable
				oldKey = common.QuoteSchema(cmp.Or(old.Schema.O, schema.file.Schema), old.Name.O)
			}
			ddl := schema.file.DDLEvent()
			ddl.TableInfo = schema.tableInfo
			size := int64(len(ddl.Query) + len(ddl.SchemaName) + len(ddl.TableName) + 1024)
			record, err := c.buffer.newRecord(ctx, size)
			if err != nil {
				return nil, err
			}
			c.mu.Lock()
			c.records = append(c.records, record)
			c.mu.Unlock()
			c.ddlWatermarks[tableKey] = max(c.ddlWatermarks[tableKey], key.TableVersion)
			if oldKey != "" {
				c.ddlWatermarks[oldKey] = max(c.ddlWatermarks[oldKey], key.TableVersion)
			}
			// This record's initial reference is the DDL write itself.
			return &readResult{ddl: ddl, onFlush: func() {
				record.pending.Add(-1)
				c.buffer.memory.effects.Add(-1)
			}}, nil
		}
		if key.TableVersion < c.ddlWatermarks[tableKey] {
			if !input.groupEnd {
				c.fileIndices[key][input.index.FileIndexKey] = input.index.Idx
			}
			continue
		}
		idKey := storageTableKey{schema: key.Schema, table: key.Table, partition: key.PartitionNum}
		tableID := c.tableIDs[idKey]
		if tableID == 0 {
			if len(c.tableIDs) >= maxRecords {
				return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Storage table identity cache exceeds its count limit")
			}
			size := int64(len(key.Schema) + len(key.Table) + 128)
			if err := c.buffer.memory.reserve(ctx, size); err != nil {
				return nil, err
			}
			c.buffer.memory.readBytes.Add(size)
			c.nextTableID++
			tableID = c.nextTableID
			c.tableIDs[idKey] = tableID
		}
		input.tableID = tableID
		c.current = input
		if input.groupEnd {
			c.groupReady = true
			continue
		}
		// Cross-node groups must be decoded completely before sorting their rows.
		c.sortBeforeWrite = input.sort
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
		if size < 0 || size > maxBufferedBytes-2*maxInFlightBytes-256 {
			_ = file.Close()
			return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Storage DML file exceeds the consumer memory budget")
		}
		record, err := c.buffer.newRecord(ctx, size+256)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		data, readErr := io.ReadAll(io.LimitReader(file, size+1))
		closeErr := file.Close()
		if readErr != nil {
			return nil, errors.WrapError(errors.ErrExternalStorageAPI, readErr, "read Storage DML file")
		}
		if closeErr != nil {
			return nil, errors.WrapError(errors.ErrExternalStorageAPI, closeErr, "close Storage DML file")
		}
		if int64(len(data)) != size {
			return nil, errors.ErrExternalStorageAPI.FastGenByArgs("Storage DML file size changed while reading")
		}
		c.mu.Lock()
		c.records = append(c.records, record)
		c.positions[record] = storagePosition{key: key, index: input.index}
		c.mu.Unlock()
		c.record = record
		if c.codecConfig.Protocol == config.ProtocolCsv {
			c.decoder, err = csv.NewDecoderWithColumnSelector(ctx, c.codecConfig, schema.tableInfo, data, c.columnSelectors.GetForTableInfo(schema.tableInfo))
			if err != nil {
				return nil, errors.WrapError(errors.ErrCodecDecode, err, "create Storage CSV decoder")
			}
		} else {
			c.decoder = canal.NewTxnDecoder(c.codecConfig)
			c.decoder.AddKeyValue(nil, data)
		}
	}
}

func (c *storageReader) Confirm(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, record := range c.records {
		pending := record.pending.Load()
		if pending < 0 {
			return errors.ErrInternalCheckFailed.FastGenByArgs("Storage input completed more than once")
		}
		if pending != 0 {
			break
		}
		if position, ok := c.positions[record]; ok {
			indices := c.confirmedIndices[position.key]
			if indices == nil {
				indices = make(map[cloudstorage.FileIndexKey]uint64)
				c.confirmedIndices[position.key] = indices
			}
			indices[position.index.FileIndexKey] = position.index.Idx
			delete(c.positions, record)
		}
		size := record.bytes.Load()
		c.buffer.memory.release(size)
		c.buffer.memory.readBytes.Add(-size)
		c.buffer.memory.records.Add(-1)
		count++
	}
	copy(c.records, c.records[count:])
	clear(c.records[len(c.records)-count:])
	c.records = c.records[:len(c.records)-count]
	c.buffer.memory.confirmed.Add(int64(count))
	return nil
}

func (c *storageReader) BufferedBytes() int64 { return c.buffer.memory.readBytes.Load() }

func (c *storageReader) Close() error {
	c.storage.Close()
	return nil
}
