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
	"math"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/canal"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/csv"
	putil "github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
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

type storageConsumer struct {
	storage         storeapi.Storage
	writer          *eventWriter
	codecConfig     *codeccommon.Config
	columnSelectors *columnselector.ColumnSelectors
	dateSeparator   config.DateSeparator
	fileExtension   string
	fileIndexWidth  int
	checkpoint      uint64
	schemas         map[cloudstorage.SchemaPathKey]*storageSchema
	fileIndices     map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64
	ddlWatermarks   map[string]uint64
	tableIDs        map[storageTableKey]int64
	tableWatermarks map[int64]uint64
	nextTableID     int64
}

func runStorageConsumer(ctx context.Context, wg *sync.WaitGroup, upstreamURI *url.URL, downstreamURI, timezone string, replicaConfig *config.ReplicaConfig) error {
	parentCtx := ctx
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	writeCtx, cancelWrite := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancelWrite(nil)
	wg.Go(func() {
		select {
		case <-parentCtx.Done():
			log.Info("consumer stopping", zap.Duration("shutdownTimeout", shutdownTimeout))
		case <-writeCtx.Done():
			return
		}
		shutdownCtx, cancelShutdown := context.WithTimeout(writeCtx, shutdownTimeout)
		defer cancelShutdown()
		<-shutdownCtx.Done()
		if shutdownCtx.Err() == context.DeadlineExceeded {
			cancelWrite(errors.ErrInternalCheckFailed.FastGenByArgs("consumer shutdown drain timed out"))
		}
	})
	c, err := newStorageConsumer(ctx, writeCtx, upstreamURI, downstreamURI, timezone, replicaConfig)
	if err != nil {
		return err
	}
	defer c.storage.Close()
	defer c.writer.downstream.Close()
	sinkDone := make(chan bool)
	defer func() {
		cancelWrite(nil)
		<-sinkDone
	}()
	wg.Go(func() {
		defer close(sinkDone)
		if err := c.writer.downstream.Run(writeCtx); err != nil {
			cancelWrite(err)
			cancel(err)
			return
		}
		if writeCtx.Err() == nil {
			err := errors.ErrInternalCheckFailed.FastGenByArgs("downstream sink stopped unexpectedly")
			cancelWrite(err)
			cancel(err)
		}
	})
	err = c.run(ctx, writeCtx)
	if writeErr := context.Cause(writeCtx); writeErr != nil {
		return writeErr
	}
	if parentCtx.Err() == nil || !errors.Is(err, context.Canceled) {
		return err
	}
	drainCtx, cancelDrain := context.WithTimeout(writeCtx, shutdownTimeout)
	defer cancelDrain()
	// A partially decoded file or cross-node group is replayed on restart.
	// Only batches already submitted to the sink are drained here.
	for len(c.writer.inFlight) != 0 {
		if err := c.writer.waitBatch(drainCtx, c.writer.inFlight[0]); err != nil {
			return err
		}
	}
	return context.Cause(writeCtx)
}

func newStorageConsumer(ctx, writeCtx context.Context, upstreamURI *url.URL, downstreamURI, timezone string, replicaConfig *config.ReplicaConfig) (*storageConsumer, error) {
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
	codecConfig := codeccommon.NewConfig(protocol)
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
	if err := codecConfig.Validate(); err != nil {
		return nil, err
	}
	selectors, err := columnselector.New(replicaConfig.Sink, putil.GetOrZero(replicaConfig.CaseSensitive))
	if err != nil {
		return nil, err
	}
	storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, upstreamURI.String())
	if err != nil {
		return nil, err
	}
	replicaConfig.Sink.TiDBSourceID = 1
	changefeedID := common.NewChangeFeedIDWithName("consumer", common.DefaultKeyspaceName)
	downstream, err := sink.New(writeCtx, &config.ChangefeedConfig{
		ChangefeedID: changefeedID, SinkURI: downstreamURI, SinkConfig: replicaConfig.Sink,
		CaseSensitive: putil.GetOrZero(replicaConfig.CaseSensitive), EnableTableAcrossNodes: putil.GetOrZero(replicaConfig.Scheduler.EnableTableAcrossNodes),
	}, changefeedID, common.DefaultKeyspaceID)
	if err != nil {
		storage.Close()
		return nil, err
	}
	partition := &messagePartition{schemas: make(map[schemaKey]bool), schemaPointers: make(map[*common.TableInfo]bool)}
	writer := &eventWriter{downstream: downstream, protocol: protocol, partitions: map[int32]*messagePartition{0: partition}, mutations: make(map[mutationKey]*mutation)}
	c := &storageConsumer{
		storage: storage, writer: writer, codecConfig: codecConfig, columnSelectors: selectors,
		dateSeparator: putil.GetOrZero(replicaConfig.Sink.DateSeparator), fileExtension: helper.GetFileExtension(protocol),
		fileIndexWidth: putil.GetOrZero(replicaConfig.Sink.FileIndexWidth),
		schemas:        make(map[cloudstorage.SchemaPathKey]*storageSchema), fileIndices: make(map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]uint64),
		ddlWatermarks: make(map[string]uint64), tableIDs: make(map[storageTableKey]int64), tableWatermarks: make(map[int64]uint64),
	}
	writer.confirm = c.confirmCompleted
	log.Info("Storage consumer initialized", zap.String("protocol", protocol.String()))
	return c, nil
}

func (c *storageConsumer) run(ctx, writeCtx context.Context) error {
	tick := time.Tick(storageScanInterval)
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		exists, err := c.storage.FileExists(ctx, "metadata")
		if err != nil {
			return errors.WrapError(errors.ErrExternalStorageAPI, err, "check Storage metadata")
		}
		if exists {
			data, err := c.readFile(ctx, "metadata", maxRecordBytes)
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
		if err := c.handleFiles(ctx, writeCtx, files); err != nil {
			return err
		}
		if time.Since(c.writer.lastProgressLog) >= progressLogInterval {
			c.writer.lastProgressLog = time.Now()
			log.Info("consumer progress", zap.Uint64("checkpoint", c.checkpoint),
				zap.Int64("receivedInputs", c.writer.receivedInputs), zap.Int64("decodedRows", c.writer.decodedRows),
				zap.Int64("writtenRows", c.writer.writtenRows), zap.Int64("completedInputs", c.writer.completedInputs),
				zap.Int("pendingDMLCount", len(c.writer.pendingDML)), zap.Int("pendingDDLCount", len(c.writer.pendingDDL)),
				zap.Int("inFlightBatches", len(c.writer.inFlight)), zap.Int64("inFlightBytes", c.writer.inFlightBytes),
				zap.Int("uncompletedInputs", c.writer.recordCount), zap.Int64("bufferedBytes", c.writer.bufferedBytes()))
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-tick:
		}
	}
}

func (c *storageConsumer) readFile(ctx context.Context, path string, limit int64) ([]byte, error) {
	if limit <= 0 {
		return nil, errors.ErrInternalCheckFailed.FastGenByArgs("consumer has no buffer capacity for a Storage file")
	}
	reader, err := c.storage.Open(ctx, path, nil)
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "open Storage file")
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, errors.WrapError(errors.ErrExternalStorageAPI, err, "read Storage file")
	}
	if int64(len(data)) > limit {
		return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Storage file exceeds the consumer buffer limit")
	}
	return data, nil
}

func (c *storageConsumer) scanFiles(ctx context.Context) (map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]storageIndexRange, error) {
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
			return nil
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
			c.writer.controlBytes += int64(len(key.Schema) + len(key.Table) + len(key.Date) + 256)
		}
		if _, known := indices[index.FileIndexKey]; !known {
			if len(indices) >= maxRecords {
				return errors.ErrInternalCheckFailed.FastGenByArgs("Storage index cache exceeds its resource limit")
			}
			indices[index.FileIndexKey] = 0
			c.writer.controlBytes += int64(len(index.DispatcherID) + 128)
		}
		if c.writer.bufferedBytes() > maxBufferedBytes {
			return errors.ErrInternalCheckFailed.FastGenByArgs("Storage index cache exceeds its byte limit")
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

func (c *storageConsumer) readSchema(ctx context.Context, path string) (cloudstorage.SchemaPathKey, bool, error) {
	var key cloudstorage.SchemaPathKey
	key.Parse(path)
	if (c.checkpoint > 0 && key.TableVersion > c.checkpoint) || c.schemas[key] != nil {
		return key, false, nil
	}
	if len(c.schemas) >= maxSchemas {
		return key, false, errors.ErrInternalCheckFailed.FastGenByArgs("Storage schema cache exceeds its count limit")
	}
	data, err := c.readFile(ctx, path, min(maxRecordBytes, maxSchemaBytes-c.writer.schemaBytes))
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
	if c.writer.schemaBytes+bytes > maxSchemaBytes || c.writer.bufferedBytes()+bytes > maxBufferedBytes {
		return key, false, errors.ErrInternalCheckFailed.FastGenByArgs("Storage schema cache exceeds its byte limit")
	}
	// Schema files carry primary-key flags. Rebuild offsets and an explicit
	// primary index, including composite keys, for the default batch DML path.
	table := file.TableInfo().ToTiDBTableInfo()
	table.UpdateTS = file.TableVersion
	table.PKIsHandle = false
	primary := &timodel.IndexInfo{ID: 1, Name: ast.NewCIStr("PRIMARY"), Primary: true, Unique: true, State: timodel.StatePublic}
	for offset, column := range table.Columns {
		column.Offset = offset
		column.State = timodel.StatePublic
		if mysql.HasPriKeyFlag(column.GetFlag()) {
			primary.Columns = append(primary.Columns, &timodel.IndexColumn{Name: column.Name, Offset: offset})
		}
	}
	if len(primary.Columns) != 0 {
		table.Indices = []*timodel.IndexInfo{primary}
	}
	c.schemas[key] = &storageSchema{file: file, tableInfo: common.NewTableInfo4Decoder(file.Schema, table)}
	c.writer.schemaBytes += bytes
	return key, true, nil
}

func (c *storageConsumer) handleFiles(ctx, writeCtx context.Context, files map[cloudstorage.DMLPathKey]map[cloudstorage.FileIndexKey]storageIndexRange) error {
	keys := slices.SortedFunc(maps.Keys(files), cloudstorage.CompareDMLPathKey)
	for _, key := range keys {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		schema := c.schemas[key.SchemaPathKey]
		if schema == nil {
			return errors.ErrCodecDecode.FastGenByArgs("Storage DML file has no matching schema")
		}
		tableKey := key.GetKey()
		if key.IsSchemaFileDMLPathKey() {
			if schema.file.Query == "" || key.TableVersion <= c.ddlWatermarks[tableKey] {
				continue
			}
			oldKey := ""
			if schema.file.Type == byte(timodel.ActionRenameTable) {
				statement, err := parser.New().ParseOneStmt(schema.file.Query, "", "")
				if err != nil {
					return errors.WrapError(errors.ErrCodecDecode, err, "parse Storage rename DDL")
				}
				rename, ok := statement.(*ast.RenameTableStmt)
				if !ok || len(rename.TableToTables) == 0 {
					return errors.ErrCodecDecode.FastGenByArgs("Storage rename DDL has no old table")
				}
				old := rename.TableToTables[0].OldTable
				oldKey = common.QuoteSchema(cmp.Or(old.Schema.O, schema.file.Schema), old.Name.O)
			}
			ddl := schema.file.DDLEvent()
			ddl.TableInfo = schema.tableInfo
			// The preceding path group has drained before advancing to this DDL.
			if err := c.writer.downstream.FlushDMLBeforeBlock(ddl); err != nil {
				return err
			}
			if err := c.writer.downstream.WriteBlockEvent(ddl); err != nil {
				return err
			}
			c.ddlWatermarks[tableKey] = max(c.ddlWatermarks[tableKey], key.TableVersion)
			if oldKey != "" {
				c.ddlWatermarks[oldKey] = max(c.ddlWatermarks[oldKey], key.TableVersion)
			}
			continue
		}
		if key.TableVersion < c.ddlWatermarks[tableKey] {
			for indexKey, span := range files[key] {
				c.fileIndices[key][indexKey] = span.end
			}
			continue
		}
		idKey := storageTableKey{schema: key.Schema, table: key.Table, partition: key.PartitionNum}
		tableID := c.tableIDs[idKey]
		if tableID == 0 {
			if len(c.tableIDs) >= maxRecords {
				return errors.ErrInternalCheckFailed.FastGenByArgs("Storage table identity cache exceeds its count limit")
			}
			c.nextTableID++
			tableID = c.nextTableID
			c.tableIDs[idKey] = tableID
			c.writer.controlBytes += int64(len(key.Schema) + len(key.Table) + 128)
		}
		sortBeforeWrite := false
		for indexKey := range files[key] {
			sortBeforeWrite = sortBeforeWrite || indexKey.EnableTableAcrossNodes
		}
		for indexKey, span := range files[key] {
			for index := span.start; ; index++ {
				fileIndex := &cloudstorage.FileIndex{FileIndexKey: indexKey, Idx: index}
				if err := c.readDMLFile(ctx, writeCtx, key, fileIndex, schema, tableID, sortBeforeWrite); err != nil {
					return err
				}
				if index == span.end {
					break
				}
			}
		}
		if err := c.writer.flushDML(ctx, writeCtx, math.MaxUint64, true, true); err != nil {
			return err
		}
		for len(c.writer.inFlight) != 0 {
			if err := c.writer.waitBatch(ctx, c.writer.inFlight[0]); err != nil {
				return err
			}
		}
		if err := c.confirmCompleted(ctx); err != nil {
			return err
		}
		for indexKey, span := range files[key] {
			c.fileIndices[key][indexKey] = span.end
		}
	}
	return nil
}

func (c *storageConsumer) readDMLFile(ctx, writeCtx context.Context, key cloudstorage.DMLPathKey, index *cloudstorage.FileIndex, schema *storageSchema, tableID int64, sortBeforeWrite bool) error {
	path := key.GenerateDMLFilePath(index, c.fileExtension, c.fileIndexWidth)
	// Reserve capacity for decoded rows while a complete input file is held.
	data, err := c.readFile(ctx, path, maxBufferedBytes-c.writer.bufferedBytes()-maxInFlightBytes)
	if err != nil {
		return err
	}
	if c.writer.recordCount >= maxRecords {
		return errors.ErrInternalCheckFailed.FastGenByArgs("Storage file completion queue exceeds its count limit")
	}
	// The decoding effect keeps the file's payload charged during intermediate
	// flushes. Rows are materialized before that effect is released.
	record := &messageRecord{remaining: 1, bytes: int64(len(data)) + 256}
	c.writer.effectCount++
	c.writer.recordCount++
	c.writer.receivedInputs++
	c.writer.inputBytes += record.bytes
	c.writer.partitions[0].records = append(c.writer.partitions[0].records, record)
	var decoder codeccommon.Decoder
	if c.codecConfig.Protocol == config.ProtocolCsv {
		decoder, err = csv.NewDecoderWithColumnSelector(ctx, c.codecConfig, schema.tableInfo, data, c.columnSelectors.GetForTableInfo(schema.tableInfo))
		if err != nil {
			return errors.WrapError(errors.ErrCodecDecode, err, "create Storage CSV decoder")
		}
	} else {
		decoder = canal.NewTxnDecoder(c.codecConfig)
		decoder.AddKeyValue(nil, data)
	}
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		messageType, hasNext := decoder.HasNext()
		if !hasNext {
			break
		}
		if messageType != codeccommon.MessageTypeRow {
			continue
		}
		message := decoder.NextDMLMessage()
		if message == nil {
			return errors.ErrCodecDecode.FastGenByArgs("Storage decoder returned an empty DML message")
		}
		if !index.EnableTableAcrossNodes && message.GetCommitTs() < c.tableWatermarks[tableID] {
			continue
		}
		c.tableWatermarks[tableID] = max(c.tableWatermarks[tableID], message.GetCommitTs())
		dml := message.ToDMLEvent()
		if dml == nil || dml.TableInfo == nil {
			return errors.ErrCodecDecode.FastGenByArgs("Storage DML message has no table metadata")
		}
		dml.PhysicalTableID = tableID
		if c.codecConfig.Protocol == config.ProtocolCanalJSON {
			dml.TableInfo.UpdateTS = key.TableVersion
		}
		record.remaining++
		c.writer.effectCount++
		if err := c.writer.queueDML(codeccommon.NewDMLMessageFromEvent(dml), record, nil); err != nil {
			return err
		}
		if c.writer.effectCount > maxEffects {
			return errors.ErrInternalCheckFailed.FastGenByArgs("Storage decoded input exceeds its effect limit")
		}
		if !sortBeforeWrite {
			if err := c.writer.flushDML(ctx, writeCtx, math.MaxUint64, true, c.writer.bufferedBytes() >= memoryHighWater); err != nil {
				return err
			}
		}
	}
	c.writer.inputBytes -= record.bytes - 256
	record.bytes = 256
	if err := c.writer.finishRecordEffect(record); err != nil {
		return err
	}
	return c.confirmCompleted(ctx)
}

func (c *storageConsumer) confirmCompleted(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	partition := c.writer.partitions[0]
	count := 0
	for _, record := range partition.records {
		if !record.complete {
			break
		}
		c.writer.inputBytes -= record.bytes
		count++
	}
	c.writer.recordCount -= count
	c.writer.completedInputs += int64(count)
	copy(partition.records, partition.records[count:])
	clear(partition.records[len(partition.records)-count:])
	partition.records = partition.records[:len(partition.records)-count]
	// Storage has independent table progress. A global checkpoint must not
	// discard replay identities belonging to a table whose files are unread.
	before := maps.Clone(c.tableWatermarks)
	for _, item := range c.writer.pendingDML {
		before[item.event.PhysicalTableID] = min(before[item.event.PhysicalTableID], item.event.CommitTs)
	}
	for _, batch := range c.writer.inFlight {
		for _, item := range batch.items {
			before[item.event.PhysicalTableID] = min(before[item.event.PhysicalTableID], item.event.CommitTs)
		}
	}
	for key, mutation := range c.writer.mutations {
		if mutation.batch == nil && key.commitTs < before[key.tableID] {
			delete(c.writer.mutations, key)
			c.writer.mutationBytes -= int64(len(key.handle) + 192)
		}
	}
	return nil
}
