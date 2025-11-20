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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec/canal"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/csv"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultChangefeedName         = "storage-consumer"
	defaultLogInterval            = 5 * time.Second
	fakePartitionNumForSchemaFile = -1
)

// fileIndexRange defines a range of files. eg. CDC000002.csv ~ CDC000005.csv
type fileIndexRange struct {
	start uint64
	end   uint64
}

type consumer struct {
	replicationCfg  *config.ReplicaConfig
	codecCfg        *common.Config
	externalStorage storage.ExternalStorage
	fileExtension   string
	sink            sink.Sink
	// tableDMLIdxMap maintains a map of <dmlPathKey, max file index>
	tableDMLIdxMap map[cloudstorage.DmlPathKey]uint64
	eventsGroup    map[int64]*util.EventsGroup
	// tableDefMap maintains a map of <`schema`.`table`, tableDef slice sorted by TableVersion>
	tableDefMap      map[string]map[uint64]*cloudstorage.TableDefinition
	tableIDGenerator *fakeTableIDGenerator
	errCh            chan error

	dmlCount               atomic.Int64
	enableTableAcrossNodes bool
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
	sink, err := sink.New(stdCtx, cfg, commonType.NewChangeFeedIDWithName(defaultChangefeedName, commonType.DefaultKeyspaceNamme))
	if err != nil {
		log.Error("failed to create sink", zap.Error(err))
		return nil, err
	}

	return &consumer{
		replicationCfg:  replicaConfig,
		codecCfg:        codecConfig,
		externalStorage: storage,
		fileExtension:   extension,
		sink:            sink,
		errCh:           errCh,
		tableDMLIdxMap:  make(map[cloudstorage.DmlPathKey]uint64),
		eventsGroup:     make(map[int64]*util.EventsGroup),
		tableDefMap:     make(map[string]map[uint64]*cloudstorage.TableDefinition),
		tableIDGenerator: &fakeTableIDGenerator{
			tableIDs: make(map[string]int64),
		},
		enableTableAcrossNodes: replicaConfig.Scheduler.EnableTableAcrossNodes,
	}, nil
}

// map1 - map2
func diffDMLMaps(
	map1, map2 map[cloudstorage.DmlPathKey]uint64,
) map[cloudstorage.DmlPathKey]fileIndexRange {
	resMap := make(map[cloudstorage.DmlPathKey]fileIndexRange)
	for k, v := range map1 {
		if _, ok := map2[k]; !ok {
			resMap[k] = fileIndexRange{
				start: 1,
				end:   v,
			}
		} else if v > map2[k] {
			resMap[k] = fileIndexRange{
				start: map2[k] + 1,
				end:   v,
			}
		}
	}

	return resMap
}

// getNewFiles returns newly created dml files in specific ranges
func (c *consumer) getNewFiles(
	ctx context.Context,
) (map[cloudstorage.DmlPathKey]fileIndexRange, error) {
	tableDMLMap := make(map[cloudstorage.DmlPathKey]fileIndexRange)
	opt := &storage.WalkOption{SubDir: ""}

	origDMLIdxMap := make(map[cloudstorage.DmlPathKey]uint64, len(c.tableDMLIdxMap))
	for k, v := range c.tableDMLIdxMap {
		origDMLIdxMap[k] = v
	}

	err := c.externalStorage.WalkDir(ctx, opt, func(path string, size int64) error {
		if cloudstorage.IsSchemaFile(path) {
			err := c.parseSchemaFilePath(ctx, path)
			if err != nil {
				log.Error("failed to parse schema file path", zap.Error(err))
				// skip handling this file
				return nil
			}
		} else if strings.HasSuffix(path, c.fileExtension) {
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

func (c *consumer) appendRow2Group(dml *event.DMLEvent) {
	var (
		tableID  = dml.GetTableID()
		schema   = dml.TableInfo.GetSchemaName()
		table    = dml.TableInfo.GetTableName()
		commitTs = dml.GetCommitTs()
	)
	group := c.eventsGroup[tableID]
	if group == nil {
		group = util.NewEventsGroup(0, tableID)
		c.eventsGroup[tableID] = group
	}
	if commitTs >= group.HighWatermark {
		group.Append(dml, false)
		log.Info("DML event append to the group",
			zap.Uint64("commitTs", commitTs), zap.Uint64("highWatermark", group.HighWatermark),
			zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
			zap.Stringer("eventType", dml.RowTypes[0]))
		return
	}
	if c.enableTableAcrossNodes {
		log.Warn("DML events fallback, but enableTableAcrossNodes is true, still append it",
			zap.Uint64("commitTs", commitTs), zap.Uint64("highWatermark", group.HighWatermark),
			zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
			zap.Stringer("eventType", dml.RowTypes[0]))
		group.Append(dml, true)
		return
	}
	log.Warn("dml event commit ts fallback, ignore",
		zap.Uint64("commitTs", dml.CommitTs),
		zap.Any("highWatermark", group.HighWatermark),
		zap.Any("row", dml),
	)
}

// emitDMLEvents decodes RowChangedEvents from file content and emit them.
func (c *consumer) emitDMLEvents(
	ctx context.Context, tableID int64,
	tableDetail cloudstorage.TableDefinition,
	pathKey cloudstorage.DmlPathKey,
	content []byte,
) error {
	var (
		decoder common.Decoder
		err     error
	)

	tableInfo, err := tableDetail.ToTableInfo()
	if err != nil {
		return errors.Trace(err)
	}

	switch c.codecCfg.Protocol {
	case config.ProtocolCsv:
		decoder, err = csv.NewDecoder(ctx, c.codecCfg, tableInfo, content)
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

			row := decoder.NextDMLEvent()

			log.Debug("next dml event", zap.Any("commitTs", row.CommitTs), zap.Any("tableName", tableInfo.TableName.String()), zap.Any("tableID", tableID))

			row.PhysicalTableID = tableID
			c.appendRow2Group(row)
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

func (c *consumer) syncExecDMLEvents(
	ctx context.Context,
	tableDef cloudstorage.TableDefinition,
	key cloudstorage.DmlPathKey,
	fileIdx uint64,
) error {
	filePath := key.GenerateDMLFilePath(fileIdx, c.fileExtension, fileIndexWidth)
	log.Debug("read from dml file path", zap.String("path", filePath))
	content, err := c.externalStorage.ReadFile(ctx, filePath)
	if err != nil {
		return errors.Trace(err)
	}
	tableID := c.tableIDGenerator.generateFakeTableID(
		key.Schema, key.Table, key.PartitionNum)
	err = c.emitDMLEvents(ctx, tableID, tableDef, key, content)
	if err != nil {
		return errors.Trace(err)
	}

	events := c.eventsGroup[tableID].GetAllEvents()
	total := len(events)
	if total == 0 {
		return nil
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
				zap.Int("total", total), zap.Duration("duration", time.Since(start)))
			return nil
		case <-ticker.C:
			log.Warn("DML events cannot be flushed in time",
				zap.Int("total", total), zap.Int64("flushed", flushed.Load()))
		}
	}
}

func (c *consumer) parseDMLFilePath(_ context.Context, path string) error {
	var dmlkey cloudstorage.DmlPathKey
	fileIdx, err := dmlkey.ParseDMLFilePath(
		putil.GetOrZero(c.replicationCfg.Sink.DateSeparator),
		path,
	)
	if err != nil {
		return errors.Trace(err)
	}

	if _, ok := c.tableDMLIdxMap[dmlkey]; !ok || fileIdx >= c.tableDMLIdxMap[dmlkey] {
		c.tableDMLIdxMap[dmlkey] = fileIdx
	}
	return nil
}

func (c *consumer) parseSchemaFilePath(ctx context.Context, path string) error {
	var schemaKey cloudstorage.SchemaPathKey
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
	dmlkey := cloudstorage.DmlPathKey{
		SchemaPathKey: schemaKey,
		PartitionNum:  fakePartitionNumForSchemaFile,
		Date:          "",
	}
	if _, ok := c.tableDMLIdxMap[dmlkey]; !ok {
		c.tableDMLIdxMap[dmlkey] = 0
	} else {
		// duplicate table schema file found, this should not happen.
		log.Panic("duplicate schema file found",
			zap.String("path", path), zap.Any("tableDef", tableDef),
			zap.Any("schemaKey", schemaKey), zap.Any("dmlkey", dmlkey))
	}
	return nil
}

func (c *consumer) mustGetTableDef(key cloudstorage.SchemaPathKey) cloudstorage.TableDefinition {
	var tableDef *cloudstorage.TableDefinition
	if tableDefs, ok := c.tableDefMap[key.GetKey()]; ok {
		tableDef = tableDefs[key.TableVersion]
	}
	if tableDef == nil {
		log.Panic("tableDef not found", zap.Any("key", key), zap.Any("tableDefMap", c.tableDefMap))
	}
	return *tableDef
}

func (c *consumer) handleNewFiles(
	ctx context.Context,
	dmlFileMap map[cloudstorage.DmlPathKey]fileIndexRange,
) error {
	keys := make([]cloudstorage.DmlPathKey, 0, len(dmlFileMap))
	for k := range dmlFileMap {
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		log.Info("no new dml files found since last round")
		return nil
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].TableVersion != keys[j].TableVersion {
			return keys[i].TableVersion < keys[j].TableVersion
		}
		if keys[i].PartitionNum != keys[j].PartitionNum {
			return keys[i].PartitionNum < keys[j].PartitionNum
		}
		if keys[i].Date != keys[j].Date {
			return keys[i].Date < keys[j].Date
		}
		if keys[i].Schema != keys[j].Schema {
			return keys[i].Schema < keys[j].Schema
		}
		return keys[i].Table < keys[j].Table
	})

	for _, key := range keys {
		tableDef := c.mustGetTableDef(key.SchemaPathKey)
		// if the key is a fake dml path key which is mainly used for
		// sorting schema.json file before the dml files, then execute the ddl query.
		if key.PartitionNum == fakePartitionNumForSchemaFile &&
			len(key.Date) == 0 && len(tableDef.Query) > 0 {
			ddlEvent, err := tableDef.ToDDLEvent()
			if err != nil {
				return err
			}
			if err := c.sink.WriteBlockEvent(ddlEvent); err != nil {
				return errors.Trace(err)
			}
			// TODO: need to cleanup tableDefMap in the future.
			log.Info("execute ddl event successfully", zap.String("query", tableDef.Query))
			continue
		}

		fileRange := dmlFileMap[key]
		for i := fileRange.start; i <= fileRange.end; i++ {
			if err := c.syncExecDMLEvents(ctx, tableDef, key, i); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *consumer) handle(ctx context.Context) error {
	ticker := time.NewTicker(flushInterval)
	logTicker := time.NewTicker(defaultLogInterval)
	lastDMLCount := int64(0)
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

		dmlFileMap, err := c.getNewFiles(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		err = c.handleNewFiles(ctx, dmlFileMap)
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
