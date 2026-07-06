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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/canal"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/csv"
	putil "github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultChangefeedName = "storage-consumer"
	defaultLogInterval    = 5 * time.Second
	metadataFileName      = "metadata"
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

type storageMetadata struct {
	CheckpointTs uint64 `json:"checkpoint-ts"`
}

type consumer struct {
	replicationCfg  *config.ReplicaConfig
	codecCfg        *common.Config
	externalStorage storeapi.Storage
	fileExtension   string
	sink            sink.Sink
	// tableDMLIdxMap maintains a map of <dmlPathKey, fileIndexKeyMap>
	tableDMLIdxMap map[cloudstorage.DMLPathKey]fileIndexKeyMap
	eventsGroup    map[int64]*util.EventsGroup
	// tableDDLWatermark maintains a map of <`schema`.`table`, max executed DDL table version>.
	// DML files with smaller table versions are considered stale replays and should be ignored.
	tableDDLWatermark map[string]uint64
	// schemaFileMap maintains a map of <`schema`.`table`, schema files by TableVersion>
	schemaFileMap    map[string]map[uint64]*cloudstorage.SchemaFile
	tableIDGenerator *fakeTableIDGenerator
	errCh            chan error

	dmlCount atomic.Int64
	readSeq  atomic.Uint64

	globalCheckpointTs uint64
}

func newConsumer(ctx context.Context) (*consumer, error) {
	_, err := putil.GetTimezone(timezone)
	if err != nil {
		return nil, errors.Annotate(err, "can not load timezone")
	}
	serverCfg := config.GetGlobalServerConfig().Clone()
	serverCfg.TZ = timezone
	config.StoreGlobalServerConfig(serverCfg)
	replicaConfig := config.GetDefaultReplicaConfig()
	if len(configFile) > 0 {
		err = util.StrictDecodeFile(configFile, "storage consumer", replicaConfig)
		if err != nil {
			log.Error("failed to decode config file", zap.Error(err))
			return nil, err
		}
	}
	// the TiDB source ID should never be set to 0
	replicaConfig.Sink.TiDBSourceID = 1
	err = replicaConfig.ValidateAndAdjust(upstreamURI)
	if err != nil {
		log.Error("failed to validate replica config", zap.Error(err))
		return nil, err
	}

	switch putil.GetOrZero(replicaConfig.Sink.Protocol) {
	case config.ProtocolCsv.String():
	case config.ProtocolCanalJSON.String():
	default:
		return nil, fmt.Errorf(
			"data encoded in protocol %s is not supported yet",
			putil.GetOrZero(replicaConfig.Sink.Protocol),
		)
	}

	protocol, err := config.ParseSinkProtocolFromString(putil.GetOrZero(replicaConfig.Sink.Protocol))
	if err != nil {
		return nil, err
	}

	codecConfig := common.NewConfig(protocol)
	err = codecConfig.Apply(upstreamURI, replicaConfig.Sink)
	if err != nil {
		return nil, err
	}

	extension := helper.GetFileExtension(protocol)

	storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, upstreamURIStr)
	if err != nil {
		log.Error("failed to create external storage", zap.Error(err))
		return nil, err
	}

	errCh := make(chan error, 1)
	stdCtx := ctx

	cfg := &config.ChangefeedConfig{
		SinkURI:    downstreamURIStr,
		SinkConfig: replicaConfig.Sink,
	}
	sink, err := sink.New(stdCtx, cfg, commonType.NewChangeFeedIDWithName(defaultChangefeedName, commonType.DefaultKeyspaceName))
	if err != nil {
		log.Error("failed to create sink", zap.Error(err))
		return nil, err
	}

	return &consumer{
		replicationCfg:    replicaConfig,
		codecCfg:          codecConfig,
		externalStorage:   storage,
		fileExtension:     extension,
		sink:              sink,
		errCh:             errCh,
		tableDMLIdxMap:    make(map[cloudstorage.DMLPathKey]fileIndexKeyMap),
		eventsGroup:       make(map[int64]*util.EventsGroup),
		tableDDLWatermark: make(map[string]uint64),
		schemaFileMap:     make(map[string]map[uint64]*cloudstorage.SchemaFile),
		tableIDGenerator: &fakeTableIDGenerator{
			tableIDs: make(map[string]int64),
		},
	}, nil
}

// map1 - map2
func diffDMLMaps(
	map1, map2 map[cloudstorage.DMLPathKey]fileIndexKeyMap,
) map[cloudstorage.DMLPathKey]fileIndexRange {
	resMap := make(map[cloudstorage.DMLPathKey]fileIndexRange) // DmlPathKey -> FileIndexKey -> indexRange
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

func (c *consumer) getGlobalCheckpointTs(ctx context.Context) error {
	exists, err := c.externalStorage.FileExists(ctx, metadataFileName)
	if err != nil {
		return errors.Trace(err)
	}
	if !exists {
		return nil
	}

	data, err := c.externalStorage.ReadFile(ctx, metadataFileName)
	if err != nil {
		return errors.Trace(err)
	}
	var metadata storageMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return errors.Trace(err)
	}
	if metadata.CheckpointTs > c.globalCheckpointTs {
		c.globalCheckpointTs = metadata.CheckpointTs
	}
	return nil
}

// getNewFiles returns newly created dml files in specific ranges that are visible under checkpointTs.
func (c *consumer) getNewFiles(
	ctx context.Context,
) (map[cloudstorage.DMLPathKey]fileIndexRange, error) {
	tableDMLMap := make(map[cloudstorage.DMLPathKey]fileIndexRange)
	opt := &storeapi.WalkOption{SubDir: ""}

	origDMLIdxMap := make(map[cloudstorage.DMLPathKey]fileIndexKeyMap, len(c.tableDMLIdxMap))
	for k, v := range c.tableDMLIdxMap {
		m := make(fileIndexKeyMap)
		for fileIndexKey, val := range v {
			m[fileIndexKey] = val
		}
		origDMLIdxMap[k] = m
	}

	dateSeparator := putil.GetOrZero(c.replicationCfg.Sink.DateSeparator)
	err := c.externalStorage.WalkDir(ctx, opt, func(path string, _ int64) error {
		if cloudstorage.IsSchemaFile(path) {
			c.parseSchemaFilePath(ctx, path)
			return nil
		}
		if strings.HasSuffix(path, ".index") {
			var dmlkey cloudstorage.DMLPathKey
			if err := dmlkey.ParseIndexFilePath(dateSeparator, path); err != nil {
				log.Debug("ignore handling unsupported dml index file", zap.String("path", path))
				return nil
			}
			c.parseDMLIndexFile(ctx, path, dmlkey)
			return nil
		}
		log.Debug("ignore handling file", zap.String("path", path))
		return nil
	})
	if err != nil {
		return tableDMLMap, err
	}

	tableDMLMap = diffDMLMaps(c.tableDMLIdxMap, origDMLIdxMap)
	return tableDMLMap, err
}

func (c *consumer) appendMessage2Group(message *common.DMLMessage, enableTableAcrossNodes bool) {
	var (
		tableID  = message.TableID
		schema   = message.Schema
		table    = message.Table
		commitTs = message.GetCommitTs()
	)
	group := c.eventsGroup[tableID]
	if group == nil {
		group = util.NewEventsGroup(0, tableID)
		c.eventsGroup[tableID] = group
	}
	if commitTs >= group.HighWatermark {
		group.AppendMessage(message, false)
		log.Debug("DML event append to the group",
			zap.Uint64("commitTs", commitTs), zap.Uint64("highWatermark", group.HighWatermark),
			zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
			zap.Stringer("eventType", message.RowType))
		return
	}
	if enableTableAcrossNodes {
		log.Warn("DML events fallback, but enableTableAcrossNodes is true, still append it",
			zap.Uint64("commitTs", commitTs), zap.Uint64("highWatermark", group.HighWatermark),
			zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
			zap.Stringer("eventType", message.RowType))
		group.AppendMessage(message, true)
		return
	}
	log.Warn("dml event commit ts fallback, ignore",
		zap.Uint64("commitTs", commitTs),
		zap.Any("highWatermark", group.HighWatermark),
		zap.String("schema", schema),
		zap.String("table", table),
	)
}

// appendDMLEvents decodes RowChangedEvents from file content and append them to event group.
func (c *consumer) appendDMLEvents(
	ctx context.Context,
	tableID int64,
	schemaFile cloudstorage.SchemaFile,
	pathKey cloudstorage.DMLPathKey,
	fileIdx *cloudstorage.FileIndex,
) error {
	filePath := pathKey.GenerateDMLFilePath(fileIdx, c.fileExtension, fileIndexWidth)
	log.Debug("read from dml file path", zap.String("path", filePath))
	content, err := c.externalStorage.ReadFile(ctx, filePath)
	if err != nil {
		return errors.Trace(err)
	}
	var decoder common.Decoder
	switch c.codecCfg.Protocol {
	case config.ProtocolCsv:
		decoder, err = csv.NewDecoder(ctx, c.codecCfg, schemaFile.TableInfo(), content)
		if err != nil {
			return errors.Trace(err)
		}
	case config.ProtocolCanalJSON:
		// Always enable tidb extension for canal-json protocol
		// because we need to get the commit ts from the extension field.
		c.codecCfg.EnableTiDBExtension = true
		decoder = canal.NewTxnDecoder(c.codecCfg)
		decoder.AddKeyValue(nil, content)
	}

	cnt := 0
	filteredCnt := 0
	for {
		tp, hasNext := decoder.HasNext()
		if err != nil {
			log.Error("failed to decode message", zap.Error(err))
			return err
		}
		if !hasNext {
			break
		}
		cnt++

		if tp == common.MessageTypeRow {
			c.dmlCount.Add(1)

			row := decoder.NextDMLMessage().ToDMLEvent()
			row.PhysicalTableID = tableID
			c.appendMessage2Group(common.NewDMLMessageFromEvent(row), fileIdx.EnableTableAcrossNodes)
			filteredCnt++
		}
	}
	log.Info("decode success", zap.String("schema", pathKey.Schema),
		zap.String("table", pathKey.Table),
		zap.Uint64("version", pathKey.TableVersion),
		zap.Int("decodeRowsCnt", cnt),
		zap.Int("filteredRowsCnt", filteredCnt))

	return err
}

func (c *consumer) flushDMLEvents(ctx context.Context, tableID int64) error {
	group := c.eventsGroup[tableID]
	if group == nil {
		return nil
	}
	messages := group.GetAllMessages()
	if len(messages) == 0 {
		return nil
	}
	events := make([]*event.DMLEvent, 0, len(messages))
	for _, message := range messages {
		events = util.AppendOrMergeDMLEvent(events, message.ToDMLEvent())
	}
	total := len(events)
	if total == 0 {
		return nil
	}
	var (
		schema string
		table  string
	)
	if events[0].TableInfo != nil {
		schema = events[0].TableInfo.GetSchemaName()
		table = events[0].TableInfo.GetTableName()
	}
	var flushed atomic.Int64
	done := make(chan struct{})
	for _, e := range events {
		e.AddPostFlushFunc(func() {
			if flushed.Inc() == int64(total) {
				close(done)
			}
		})
		c.sink.AddDMLEvent(e)
	}

	// Make sure all events are flushed to downstream.
	start := time.Now()
	ticker := time.NewTicker(defaultLogInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-done:
			log.Info("flush DML events done",
				zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
				zap.Int("total", total), zap.Duration("duration", time.Since(start)))
			return nil
		case <-ticker.C:
			log.Warn("DML events cannot be flushed in time",
				zap.Int("total", total), zap.Int64("flushed", flushed.Load()))
		}
	}
}

func (c *consumer) parseDMLIndexFile(ctx context.Context, path string, dmlkey cloudstorage.DMLPathKey) {
	if c.globalCheckpointTs > 0 && dmlkey.TableVersion > c.globalCheckpointTs {
		log.Debug("skip dml index file by checkpoint",
			zap.String("path", path),
			zap.Uint64("tableVersion", dmlkey.TableVersion),
			zap.Uint64("checkpointTs", c.globalCheckpointTs))
		return
	}
	data, err := c.externalStorage.ReadFile(ctx, path)
	if err != nil {
		log.Panic("read dml index file failed",
			zap.String("path", path), zap.Error(err))
	}
	fileName := strings.TrimSuffix(string(data), "\n")
	fileIndex, err := cloudstorage.ParseFileIndexFromFileName(fileName, c.fileExtension)
	if err != nil {
		log.Panic("parse file index from file name failed",
			zap.String("path", path), zap.String("fileName", fileName), zap.Error(err))
	}

	m, ok := c.tableDMLIdxMap[dmlkey]
	if !ok {
		c.tableDMLIdxMap[dmlkey] = fileIndexKeyMap{
			fileIndex.FileIndexKey: fileIndex.Idx,
		}
		return
	}
	if fileIndex.Idx >= m[fileIndex.FileIndexKey] {
		c.tableDMLIdxMap[dmlkey][fileIndex.FileIndexKey] = fileIndex.Idx
	}
}

func (c *consumer) parseSchemaFilePath(ctx context.Context, path string) {
	var schemaKey cloudstorage.SchemaPathKey
	schemaKey.Parse(path)
	if c.globalCheckpointTs > 0 && schemaKey.TableVersion > c.globalCheckpointTs {
		log.Debug("skip schema file by checkpoint",
			zap.String("path", path),
			zap.Uint64("tableVersion", schemaKey.TableVersion),
			zap.Uint64("checkpointTs", c.globalCheckpointTs))
		return
	}
	key := schemaKey.GetKey()
	if schemaFiles, ok := c.schemaFileMap[key]; ok {
		if _, ok := schemaFiles[schemaKey.TableVersion]; ok {
			// Skip if schema file already exists.
			return
		}
	}
	if _, ok := c.schemaFileMap[key]; !ok {
		c.schemaFileMap[key] = make(map[uint64]*cloudstorage.SchemaFile)
	}

	// Read schema file.
	data, err := c.externalStorage.ReadFile(ctx, path)
	if err != nil {
		log.Panic("read schema file failed",
			zap.String("path", path), zap.Error(err))
	}
	var schemaFile cloudstorage.SchemaFile
	if err := json.Unmarshal(data, &schemaFile); err != nil {
		log.Panic("unmarshal schema file failed, this should not happen",
			zap.ByteString("content", data), zap.Error(err))
	}
	schemaFileName := path[strings.LastIndex(path, "/")+1:]
	checksumText := strings.TrimSuffix(schemaFileName[strings.LastIndex(schemaFileName, "_")+1:], ".json")
	checksum, err := strconv.ParseUint(checksumText, 10, 32)
	if err != nil {
		log.Panic("parse schema file checksum failed, this should not happen",
			zap.String("path", path), zap.Error(err))
	}
	checksumInMem := schemaFile.Checksum()
	if checksumInMem != uint32(checksum) || schemaKey.TableVersion != schemaFile.TableVersion {
		log.Panic("checksum mismatch in the schema file",
			zap.String("path", path),
			zap.Uint32("checksum", uint32(checksum)),
			zap.Uint32("checksumInMem", checksumInMem),
			zap.Uint64("tableVersion", schemaFile.TableVersion),
			zap.Uint64("schemaKeyTableVersion", schemaKey.TableVersion))
	}
	// Update schemaFileMap.
	c.schemaFileMap[key][schemaKey.TableVersion] = &schemaFile

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
	dmlkey := cloudstorage.NewSchemaFileDMLPathKey(schemaKey)
	if _, ok := c.tableDMLIdxMap[dmlkey]; ok {
		// duplicate schema file found, this should not happen.
		log.Panic("duplicate schema file found",
			zap.String("path", path), zap.Any("schemaFile", schemaFile),
			zap.Any("schemaKey", schemaKey), zap.Any("dmlkey", dmlkey))
	}
	c.tableDMLIdxMap[dmlkey] = fileIndexKeyMap{}
}

func (c *consumer) mustGetSchemaFile(key cloudstorage.SchemaPathKey) cloudstorage.SchemaFile {
	var schemaFile *cloudstorage.SchemaFile
	if schemaFiles, ok := c.schemaFileMap[key.GetKey()]; ok {
		schemaFile = schemaFiles[key.TableVersion]
	}
	if schemaFile == nil {
		log.Panic("schema file not found", zap.Any("key", key), zap.Any("schemaFileMap", c.schemaFileMap))
	}
	return *schemaFile
}

func getRenameTableOldTableKey(schemaFile cloudstorage.SchemaFile) (string, bool) {
	if schemaFile.Type != byte(timodel.ActionRenameTable) {
		return "", false
	}
	schemaName := schemaFile.Schema
	stmt, err := parser.New().ParseOneStmt(schemaFile.Query, "", "")
	if err != nil {
		log.Panic("parse statement failed", zap.Any("DDL", schemaFile.Query), zap.Error(err))
	}
	// The query in job maybe "RENAME TABLE table1 to table2"
	renameStmt, ok := stmt.(*ast.RenameTableStmt)
	if !ok || len(renameStmt.TableToTables) == 0 {
		log.Panic("invalid rename table statement", zap.Any("DDL", schemaFile.Query))
	}
	oldTable := renameStmt.TableToTables[0].OldTable
	if oldTable.Schema.O != "" {
		schemaName = oldTable.Schema.O
	}
	tableName := oldTable.Name.O
	return commonType.QuoteSchema(schemaName, tableName), true
}

func (c *consumer) updateTableDDLWatermark(schemaFile cloudstorage.SchemaFile) string {
	key := commonType.QuoteSchema(schemaFile.Schema, schemaFile.Table)
	if c.tableDDLWatermark[key] < schemaFile.TableVersion {
		c.tableDDLWatermark[key] = schemaFile.TableVersion
	}
	if oldTableKey, ok := getRenameTableOldTableKey(schemaFile); ok {
		if c.tableDDLWatermark[oldTableKey] < schemaFile.TableVersion {
			c.tableDDLWatermark[oldTableKey] = schemaFile.TableVersion
		}
	}
	return key
}

func (c *consumer) handleNewFiles(
	ctx context.Context,
	dmlFileMap map[cloudstorage.DMLPathKey]fileIndexRange,
	round uint64,
) error {
	if len(dmlFileMap) == 0 {
		log.Info("no new dml files found since last round", zap.Uint64("round", round))
		return nil
	}
	keys := make([]cloudstorage.DMLPathKey, 0, len(dmlFileMap))
	for k := range dmlFileMap {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return cloudstorage.CompareDMLPathKey(keys[i], keys[j]) < 0
	})

	for order, key := range keys {
		schemaFile := c.mustGetSchemaFile(key.SchemaPathKey)
		tableKey := key.GetKey()
		ddlWatermark := c.tableDDLWatermark[tableKey]
		log.Info("storage consumer handle file key",
			zap.Uint64("round", round),
			zap.Int("order", order),
			zap.String("schema", key.Schema),
			zap.String("table", key.Table),
			zap.Uint64("tableVersion", key.TableVersion),
			zap.Int64("partition", key.PartitionNum),
			zap.String("date", key.Date),
			zap.Int("rangeCount", len(dmlFileMap[key])))

		// if the key is a fake dml path key which is mainly used for
		// sorting schema.json file before the dml files, then execute the ddl query.
		if key.IsSchemaFileDMLPathKey() && len(schemaFile.Query) > 0 {
			if key.TableVersion <= ddlWatermark {
				log.Warn("DDL event replayed with stale table version, ignore it",
					zap.String("schema", key.Schema), zap.String("table", key.Table),
					zap.Uint64("tableVersion", key.TableVersion), zap.Uint64("ddlWatermark", ddlWatermark),
					zap.String("query", schemaFile.Query))
				continue
			}

			seq := c.readSeq.Inc()
			log.Info("storage consumer read ddl event",
				zap.Uint64("seq", seq),
				zap.Uint64("round", round),
				zap.Int("order", order),
				zap.String("schema", key.Schema),
				zap.String("table", key.Table),
				zap.Uint64("tableVersion", key.TableVersion),
				zap.Uint64("ddlWatermark", ddlWatermark),
				zap.String("query", schemaFile.Query))

			ddlEvent := schemaFile.DDLEvent()
			if err := c.sink.WriteBlockEvent(ddlEvent); err != nil {
				return errors.Trace(err)
			}
			watermarkKey := c.updateTableDDLWatermark(schemaFile)
			// TODO: need to cleanup schemaFileMap in the future.
			log.Info("execute ddl event successfully",
				zap.String("query", schemaFile.Query),
				zap.String("schema", key.Schema), zap.String("table", key.Table),
				zap.Uint64("ddlWatermark", c.tableDDLWatermark[tableKey]),
				zap.String("watermarkKey", watermarkKey))
			continue
		}

		// The downstream table has already moved to a newer DDL version.
		// DML files produced with an older table version should be ignored.
		if key.TableVersion < ddlWatermark {
			log.Warn("DML files replayed with stale table version, ignore them",
				zap.String("schema", key.Schema), zap.String("table", key.Table),
				zap.Uint64("tableVersion", key.TableVersion), zap.Uint64("ddlWatermark", ddlWatermark),
				zap.Int64("partition", key.PartitionNum), zap.String("date", key.Date))
			continue
		}

		tableID := c.tableIDGenerator.generateFakeTableID(
			key.Schema, key.Table, key.PartitionNum)
		fileRange := dmlFileMap[key]
		for indexKey, indexRange := range fileRange {
			for i := indexRange.start; i <= indexRange.end; i++ {
				fileIndex := &cloudstorage.FileIndex{
					FileIndexKey: indexKey,
					Idx:          i,
				}
				filePath := key.GenerateDMLFilePath(fileIndex, c.fileExtension, fileIndexWidth)
				seq := c.readSeq.Inc()
				log.Info("storage consumer read dml file",
					zap.Uint64("seq", seq),
					zap.Uint64("round", round),
					zap.Int("order", order),
					zap.String("schema", key.Schema),
					zap.String("table", key.Table),
					zap.Uint64("tableVersion", key.TableVersion),
					zap.Int64("partition", key.PartitionNum),
					zap.String("date", key.Date),
					zap.String("dispatcher", indexKey.DispatcherID),
					zap.Bool("enableTableAcrossNodes", indexKey.EnableTableAcrossNodes),
					zap.Uint64("fileIndex", i),
					zap.String("path", filePath))
				if err := c.appendDMLEvents(ctx, tableID, schemaFile, key, fileIndex); err != nil {
					return err
				}
			}
		}
		if err := c.flushDMLEvents(ctx, tableID); err != nil {
			return err
		}
	}

	return nil
}

func (c *consumer) handle(ctx context.Context) error {
	ticker := time.NewTicker(flushInterval)
	logTicker := time.NewTicker(defaultLogInterval)
	defer func() {
		ticker.Stop()
		logTicker.Stop()
	}()

	var (
		lastDMLCount int64
		round        uint64
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-c.errCh:
			return err
		case <-logTicker.C:
			dmlDelta := c.dmlCount.Load() - lastDMLCount
			flushSpeed := dmlDelta / int64(defaultLogInterval.Seconds())
			lastDMLCount = c.dmlCount.Load()
			logString := fmt.Sprintf("total flush dml count: %d, flush row per second: %d", c.dmlCount.Load(), flushSpeed)
			log.Info(logString)

		case <-ticker.C:
		}

		round++
		err := c.getGlobalCheckpointTs(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		dmlFileMap, err := c.getNewFiles(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("storage consumer scan done",
			zap.Uint64("round", round),
			zap.Uint64("checkpointTs", c.globalCheckpointTs),
			zap.Int("dmlPathKeyCount", len(dmlFileMap)))

		err = c.handleNewFiles(ctx, dmlFileMap, round)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

func (c *consumer) run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return c.sink.Run(ctx)
	})
	g.Go(func() error {
		return c.handle(ctx)
	})
	return g.Wait()
}
