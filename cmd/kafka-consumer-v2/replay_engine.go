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

package main

import (
	"context"
	"database/sql"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/simple"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

type partitionState struct {
	partition       int32
	watermark       uint64
	watermarkOffset kafka.Offset
	decoder         codeccommon.Decoder
	groups          map[int64]*dmlGroup
}

type queuedDDL struct {
	ddl    *commonEvent.DDLEvent
	source *messageSource
}

type replayEngine struct {
	partitions []*partitionState

	applyMu     sync.Mutex
	watermarkMu sync.RWMutex
	consumerID  string
	topic       string

	ddlQueue           []queuedDDL
	ddlWithMaxCommitTs map[int64]uint64
	seenDDLs           map[string]struct{}

	eventRouter            *eventrouter.EventRouter
	protocol               config.Protocol
	maxMessageBytes        int
	maxBatchSize           int
	enableTableAcrossNodes bool

	mysqlSink sink.Sink
	offsets   *offsetTracker
	inflight  *inflightTracker
	syncpoint *syncpointManager
}

type decodedMessage struct {
	messageType codeccommon.MessageType
	resolvedTs  uint64
	ddl         *commonEvent.DDLEvent
	rows        []*commonEvent.DMLEvent
}

func newReplayEngine(ctx context.Context, o *option) (*replayEngine, error) {
	engine := &replayEngine{
		partitions:             make([]*partitionState, o.partitionNum),
		ddlQueue:               make([]queuedDDL, 0),
		ddlWithMaxCommitTs:     make(map[int64]uint64),
		seenDDLs:               make(map[string]struct{}),
		protocol:               o.protocol,
		maxMessageBytes:        o.maxMessageBytes,
		maxBatchSize:           o.maxBatchSize,
		enableTableAcrossNodes: o.enableTableAcrossNodes,
		consumerID:             o.groupID,
		topic:                  o.topic,
		offsets:                newOffsetTracker(),
		inflight:               newInflightTracker(),
	}

	var db *sql.DB
	var err error
	if o.upstreamTiDBDSN != "" {
		db, err = openDB(ctx, o.upstreamTiDBDSN)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	for i := 0; i < int(o.partitionNum); i++ {
		decoder, err := codec.NewEventDecoder(ctx, i, o.codecConfig, o.topic, db)
		if err != nil {
			return nil, errors.Trace(err)
		}
		engine.partitions[i] = &partitionState{
			partition: int32(i),
			decoder:   decoder,
			groups:    make(map[int64]*dmlGroup),
		}
	}

	eventRouter, err := eventrouter.NewEventRouter(o.sinkConfig, o.topic, false, o.protocol == config.ProtocolAvro)
	if err != nil {
		return nil, errors.Trace(err)
	}
	engine.eventRouter = eventRouter

	changefeedID := commonType.NewChangeFeedIDWithName("kafka-consumer-v2", commonType.DefaultKeyspaceName)
	cfg := &config.ChangefeedConfig{
		ChangefeedID: changefeedID,
		SinkURI:      o.downstreamURI,
		SinkConfig:   o.sinkConfig,
	}
	engine.mysqlSink, err = sink.New(ctx, cfg, changefeedID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	engine.syncpoint, err = newSyncpointManager(ctx, o)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return engine, nil
}

func (e *replayEngine) Run(ctx context.Context) error {
	if e.syncpoint != nil {
		defer e.syncpoint.Close()
	}
	return e.mysqlSink.Run(ctx)
}

func (e *replayEngine) HandleMessage(ctx context.Context, message *kafka.Message) ([]kafka.TopicPartition, error) {
	if message == nil || message.TopicPartition.Topic == nil {
		return nil, errors.New("received nil kafka message or topic")
	}
	partition := message.TopicPartition.Partition
	if partition < 0 || int(partition) >= len(e.partitions) {
		return nil, errors.Errorf("received message from unexpected partition %d, configured partitions %d",
			partition, len(e.partitions))
	}
	source := e.offsets.NewSource(*message.TopicPartition.Topic, partition, message.TopicPartition.Offset)
	decoded, err := e.decodeMessage(message)
	if err == nil {
		e.applyMu.Lock()
		err = e.applyDecodedMessage(ctx, message, source, decoded)
		e.applyMu.Unlock()
	}
	source.Close()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return e.offsets.DrainCommittable(), nil
}

func (e *replayEngine) DrainCommittableOffsets() []kafka.TopicPartition {
	return e.offsets.DrainCommittable()
}

func (e *replayEngine) decodeMessage(message *kafka.Message) (*decodedMessage, error) {
	partition := message.TopicPartition.Partition
	offset := message.TopicPartition.Offset
	progress := e.partitions[partition]

	progress.decoder.AddKeyValue(message.Key, message.Value)
	messageType, hasNext := progress.decoder.HasNext()
	if !hasNext {
		return nil, errors.Errorf("decoder has no event after receiving kafka message, partition=%d offset=%v", partition, offset)
	}

	switch messageType {
	case codeccommon.MessageTypeResolved:
		newWatermark := progress.decoder.NextResolvedEvent()
		return &decodedMessage{messageType: messageType, resolvedTs: newWatermark}, nil
	case codeccommon.MessageTypeDDL:
		ddl := progress.decoder.NextDDLEvent()
		rows := make([]*commonEvent.DMLEvent, 0)
		if dec, ok := progress.decoder.(*simple.Decoder); ok {
			for _, row := range dec.GetCachedEvents() {
				rows = append(rows, row)
			}
		}
		return &decodedMessage{messageType: messageType, ddl: ddl, rows: rows}, nil
	case codeccommon.MessageTypeRow:
		counter := 0
		row := progress.decoder.NextDMLEvent()
		if row == nil {
			if e.protocol != config.ProtocolSimple {
				return nil, errors.Errorf("decoded nil DML event, partition=%d offset=%v", partition, offset)
			}
			return &decodedMessage{messageType: messageType}, nil
		}
		rows := []*commonEvent.DMLEvent{row}
		counter++
		for {
			_, hasNext = progress.decoder.HasNext()
			if !hasNext {
				break
			}
			row = progress.decoder.NextDMLEvent()
			if row == nil {
				return nil, errors.Errorf("decoded nil DML event in batch, partition=%d offset=%v", partition, offset)
			}
			rows = append(rows, row)
			counter++
		}
		if len(message.Key)+len(message.Value) > e.maxMessageBytes && counter > 1 {
			return nil, errors.Errorf("kafka max-message-bytes exceeded, partition=%d offset=%v max=%d received=%d",
				partition, offset, e.maxMessageBytes, len(message.Key)+len(message.Value))
		}
		if counter > e.maxBatchSize {
			return nil, errors.Errorf("open protocol max-batch-size exceeded, partition=%d offset=%v max=%d actual=%d",
				partition, offset, e.maxBatchSize, counter)
		}
		return &decodedMessage{messageType: messageType, rows: rows}, nil
	default:
		return nil, errors.Errorf("unknown kafka message type %v, partition=%d offset=%v", messageType, partition, offset)
	}
}

func (e *replayEngine) applyDecodedMessage(
	ctx context.Context, message *kafka.Message, source *messageSource, decoded *decodedMessage,
) error {
	partition := message.TopicPartition.Partition
	offset := message.TopicPartition.Offset
	progress := e.partitions[partition]
	if decoded == nil {
		return nil
	}

	switch decoded.messageType {
	case codeccommon.MessageTypeResolved:
		source.AddWork()
		e.updateWatermark(progress, decoded.resolvedTs, offset)
		if err := e.processReadyDDLs(ctx); err != nil {
			return errors.Trace(err)
		}
		if err := e.processReadySyncpoints(ctx); err != nil {
			return errors.Trace(err)
		}
		e.dispatchBufferedDMLs()
		source.Done()
	case codeccommon.MessageTypeDDL:
		for _, row := range decoded.rows {
			if err := e.appendDML(row, progress, source, offset); err != nil {
				return errors.Trace(err)
			}
		}
		if decoded.ddl != nil && decoded.ddl.Query != "" {
			source.AddWork()
			accepted, err := e.enqueueDDL(decoded.ddl, source)
			if err != nil {
				source.Done()
				return errors.Trace(err)
			}
			if !accepted {
				source.Done()
			}
			if err = e.processReadyDDLs(ctx); err != nil {
				return errors.Trace(err)
			}
			e.dispatchBufferedDMLs()
		}
	case codeccommon.MessageTypeRow:
		for _, row := range decoded.rows {
			if err := e.appendDML(row, progress, source, offset); err != nil {
				return errors.Trace(err)
			}
		}
	default:
		return errors.Errorf("unknown decoded kafka message type %v, partition=%d offset=%v",
			decoded.messageType, partition, offset)
	}
	return nil
}

func (e *replayEngine) updateWatermark(progress *partitionState, newWatermark uint64, offset kafka.Offset) {
	e.watermarkMu.Lock()
	currentWatermark := progress.watermark
	currentWatermarkOffset := progress.watermarkOffset
	if newWatermark >= currentWatermark {
		progress.watermark = newWatermark
		progress.watermarkOffset = offset
		e.watermarkMu.Unlock()
		log.Debug("watermark received",
			zap.Int32("partition", progress.partition),
			zap.Any("offset", offset),
			zap.Uint64("watermark", newWatermark))
		return
	}
	e.watermarkMu.Unlock()
	log.Warn("partition resolved ts fall back, ignore it",
		zap.Int32("partition", progress.partition),
		zap.Uint64("newWatermark", newWatermark),
		zap.Any("offset", offset),
		zap.Uint64("watermark", currentWatermark),
		zap.Any("watermarkOffset", currentWatermarkOffset))
}

func (e *replayEngine) appendDML(
	dml *commonEvent.DMLEvent, progress *partitionState, source *messageSource, offset kafka.Offset,
) error {
	if err := e.checkPartition(dml, progress.partition, offset); err != nil {
		return errors.Trace(err)
	}
	tableID := dml.GetTableID()
	group := progress.groups[tableID]
	if group == nil {
		group = newDMLGroup(progress.partition, tableID)
		progress.groups[tableID] = group
	}

	commitTs := dml.GetCommitTs()
	if e.syncpoint != nil && e.syncpoint.enabled {
		e.syncpoint.EnsureNextTs(commitTs)
	}
	groupAppliedWatermark := group.AppliedWatermark()
	syncedWatermark := uint64(0)
	if e.syncpoint != nil && e.syncpoint.enabled {
		syncedWatermark = e.syncpoint.lastSyncedTs
	}
	skipReason := ""
	if syncedWatermark != 0 && commitTs <= syncedWatermark {
		skipReason = "synced-syncpoint"
	} else if groupAppliedWatermark != 0 && commitTs < groupAppliedWatermark {
		skipReason = "table-applied-watermark"
	}
	if skipReason != "" {
		log.Debug("DML event replayed after applied, ignore it",
			zap.String("reason", skipReason),
			zap.Int64("tableID", tableID),
			zap.Int32("partition", group.partition),
			zap.Uint64("commitTs", commitTs),
			zap.Any("offset", offset),
			zap.Uint64("groupAppliedWatermark", groupAppliedWatermark),
			zap.Uint64("syncedWatermark", syncedWatermark),
			zap.Uint64("highWatermark", group.HighWatermark()),
			zap.Uint64("partitionWatermark", progress.watermark))
		return nil
	}

	highWatermark := group.HighWatermark()
	source.AddWork()
	if e.shouldBufferDML(commitTs) {
		forceInsert := commitTs < highWatermark || commitTs < progress.watermark || e.enableTableAcrossNodes
		queued, err := group.Append(dml, source, forceInsert)
		if err != nil {
			source.Done()
			return errors.Trace(err)
		}
		if !queued {
			source.Done()
			return nil
		}
		log.Debug("DML event buffered by replay barrier",
			zap.Int32("partition", group.partition),
			zap.Any("offset", offset),
			zap.Uint64("commitTs", commitTs),
			zap.Uint64("dispatchThreshold", e.dispatchThreshold()))
		return nil
	}
	group.Observe(commitTs)
	dml.AddPostFlushFunc(source.Done)
	e.dispatchDML(dml, group)

	eventType := "<empty>"
	if len(dml.RowTypes) > 0 {
		eventType = dml.RowTypes[0].String()
	}
	log.Debug("DML event dispatched to downstream sink",
		zap.Int32("partition", group.partition),
		zap.Any("offset", offset),
		zap.Uint64("commitTs", commitTs),
		zap.Uint64("highWatermark", group.HighWatermark()),
		zap.Uint64("appliedWatermark", group.AppliedWatermark()),
		zap.String("schema", dml.TableInfo.GetSchemaName()),
		zap.String("table", dml.TableInfo.GetTableName()),
		zap.Int64("tableID", tableID),
		zap.String("eventType", eventType))
	return nil
}

func (e *replayEngine) dispatchDML(dml *commonEvent.DMLEvent, group *dmlGroup) {
	commitTs := dml.GetCommitTs()
	e.inflight.Add(commitTs)
	dml.AddPostFlushFunc(func() {
		group.MarkApplied(commitTs)
		e.inflight.Done(commitTs)
	})
	e.mysqlSink.AddDMLEvent(dml)
}

func (e *replayEngine) dispatchBufferedDMLs() {
	threshold := e.dispatchThreshold()
	for _, progress := range e.partitions {
		for _, group := range progress.groups {
			for _, dml := range group.PopDispatchable(threshold) {
				e.dispatchDML(dml, group)
			}
		}
	}
}

func (e *replayEngine) shouldBufferDML(commitTs uint64) bool {
	return commitTs > e.dispatchThreshold()
}

func (e *replayEngine) dispatchThreshold() uint64 {
	threshold := uint64(math.MaxUint64)
	if e.syncpoint != nil && e.syncpoint.enabled && e.syncpoint.nextTs != 0 && e.syncpoint.nextTs < threshold {
		threshold = e.syncpoint.nextTs
	}
	for _, item := range e.ddlQueue {
		if commitTs := item.ddl.GetCommitTs(); commitTs < threshold {
			threshold = commitTs
		}
	}
	return threshold
}

func (e *replayEngine) checkPartition(row *commonEvent.DMLEvent, partition int32, offset kafka.Offset) error {
	partitioner := e.eventRouter.GetPartitionGenerator(row.TableInfo.GetSchemaName(), row.TableInfo.GetTableName())
	partitionNum := int32(len(e.partitions))
	for {
		change, ok := row.GetNextRow()
		if !ok {
			row.Rewind()
			return nil
		}
		target, _, err := partitioner.GeneratePartitionIndexAndKey(&change, partitionNum, row.TableInfo, row.GetCommitTs())
		if err != nil {
			row.Rewind()
			return errors.Trace(err)
		}
		if partition != target {
			row.Rewind()
			return errors.Errorf("dml event dispatched to wrong partition, actual=%d expected=%d partitionNum=%d offset=%v tableID=%d",
				partition, target, len(e.partitions), offset, row.GetTableID())
		}
	}
}

func (e *replayEngine) globalWatermark() uint64 {
	e.watermarkMu.RLock()
	defer e.watermarkMu.RUnlock()

	watermark := uint64(math.MaxUint64)
	for _, progress := range e.partitions {
		if progress.watermark < watermark {
			watermark = progress.watermark
		}
	}
	return watermark
}

func (e *replayEngine) waitInflightDML(ctx context.Context, commitTs uint64, fields ...zap.Field) error {
	logFields := append([]zap.Field{zap.Uint64("barrierTs", commitTs)}, fields...)
	log.Info("wait DML barrier", logFields...)
	start := time.Now()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	done := make(chan error, 1)
	go func() {
		done <- e.inflight.WaitLTE(ctx, commitTs)
	}()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case err := <-done:
			if err != nil {
				return errors.Trace(err)
			}
			log.Info("wait DML barrier done",
				append(logFields, zap.Duration("duration", time.Since(start)))...)
			return nil
		case <-ticker.C:
			log.Warn("DML barrier cannot be flushed in time", logFields...)
		}
	}
}

func (e *replayEngine) enqueueDDL(ddl *commonEvent.DDLEvent, source *messageSource) (bool, error) {
	dedupeKey := strconv.FormatUint(ddl.GetCommitTs(), 10) + ":" + ddl.Query
	if _, exists := e.seenDDLs[dedupeKey]; exists {
		return false, nil
	}
	tableIDs := e.getBlockTableIDs(ddl)
	for tableID := range tableIDs {
		maxCommitTs, ok := e.ddlWithMaxCommitTs[tableID]
		if ok && ddl.GetCommitTs() < maxCommitTs {
			log.Warn("ignore DDL commitTs fallback",
				zap.Uint64("commitTs", ddl.GetCommitTs()),
				zap.Uint64("maxCommitTs", maxCommitTs),
				zap.String("DDL", ddl.Query))
			return false, nil
		}
	}

	e.ddlQueue = append(e.ddlQueue, queuedDDL{
		ddl:    ddl,
		source: source,
	})
	e.seenDDLs[dedupeKey] = struct{}{}
	for tableID := range tableIDs {
		e.ddlWithMaxCommitTs[tableID] = ddl.GetCommitTs()
	}
	return true, nil
}

func (e *replayEngine) processReadyDDLs(ctx context.Context) error {
	if len(e.ddlQueue) > 1 {
		sort.SliceStable(e.ddlQueue, func(i, j int) bool {
			return e.ddlQueue[i].ddl.GetCommitTs() < e.ddlQueue[j].ddl.GetCommitTs()
		})
	}

	watermark := e.globalWatermark()
	remaining := make([]queuedDDL, 0)
	for i, item := range e.ddlQueue {
		if !e.shouldBypassWatermark(item.ddl) && item.ddl.GetCommitTs() > watermark {
			remaining = append(remaining, e.ddlQueue[i:]...)
			break
		}
		if err := e.flushDDLEvent(ctx, item.ddl); err != nil {
			return errors.Trace(err)
		}
		item.source.Done()
		log.Info("DDL event applied",
			zap.Uint64("commitTs", item.ddl.GetCommitTs()),
			zap.String("schema", item.ddl.GetSchemaName()),
			zap.String("table", item.ddl.GetTableName()),
			zap.String("query", item.ddl.Query))
	}
	e.ddlQueue = remaining
	return nil
}

func (e *replayEngine) processReadySyncpoints(ctx context.Context) error {
	if e.syncpoint == nil || !e.syncpoint.enabled {
		return nil
	}
	watermark := e.globalWatermark()
	e.syncpoint.EnsureNextTs(watermark)
	for e.syncpoint.nextTs != 0 && e.syncpoint.nextTs <= watermark {
		syncpointTs := e.syncpoint.nextTs
		start := time.Now()
		e.dispatchBufferedDMLs()
		if err := e.waitInflightDML(ctx, syncpointTs,
			zap.String("barrierType", "syncpoint"),
			zap.Uint64("globalWatermark", watermark)); err != nil {
			return errors.Trace(err)
		}
		secondaryTs, err := e.syncpoint.Write(ctx, syncpointTs)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("consumer syncpoint written",
			zap.String("consumerID", e.syncpoint.consumer),
			zap.String("topic", e.syncpoint.topic),
			zap.Uint64("primaryTs", syncpointTs),
			zap.Uint64("secondaryTs", secondaryTs),
			zap.Duration("duration", time.Since(start)))
		e.syncpoint.AdvanceNextTs()
		e.dispatchBufferedDMLs()
	}
	return nil
}

func (e *replayEngine) shouldBypassWatermark(ddl *commonEvent.DDLEvent) bool {
	action := timodel.ActionType(ddl.Type)
	switch action {
	case timodel.ActionCreateSchema:
		return true
	case timodel.ActionCreateTable:
		blockedTables := ddl.GetBlockedTables()
		return blockedTables != nil &&
			blockedTables.InfluenceType == commonEvent.InfluenceTypeNormal &&
			len(blockedTables.TableIDs) == 1 &&
			blockedTables.TableIDs[0] == commonType.DDLSpanTableID &&
			len(ddl.GetBlockedTableNames()) == 0
	default:
		return false
	}
}

func (e *replayEngine) flushDDLEvent(ctx context.Context, ddl *commonEvent.DDLEvent) error {
	commitTs := ddl.GetCommitTs()
	e.dispatchBufferedDMLs()
	if err := e.waitInflightDML(ctx, commitTs,
		zap.String("barrierType", "DDL"),
		zap.String("query", ddl.Query)); err != nil {
		return errors.Trace(err)
	}
	return e.mysqlSink.WriteBlockEvent(ddl)
}

func (e *replayEngine) getBlockTableIDs(ddl *commonEvent.DDLEvent) map[int64]struct{} {
	tableIDs := make(map[int64]struct{})
	blockedTables := ddl.GetBlockedTables()
	if blockedTables == nil {
		for _, progress := range e.partitions {
			for tableID := range progress.groups {
				tableIDs[tableID] = struct{}{}
			}
		}
		return tableIDs
	}
	switch blockedTables.InfluenceType {
	case commonEvent.InfluenceTypeDB, commonEvent.InfluenceTypeAll:
		for _, progress := range e.partitions {
			for tableID := range progress.groups {
				tableIDs[tableID] = struct{}{}
			}
		}
	case commonEvent.InfluenceTypeNormal:
		for _, item := range blockedTables.TableIDs {
			tableIDs[item] = struct{}{}
		}
	default:
		log.Warn("unsupported DDL influence type, block all buffered tables",
			zap.Any("influenceType", blockedTables.InfluenceType),
			zap.String("query", ddl.Query))
		for _, progress := range e.partitions {
			for tableID := range progress.groups {
				tableIDs[tableID] = struct{}{}
			}
		}
	}
	return tableIDs
}

func openDB(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(10 * time.Minute)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("open db success", zap.String("dsn", dsn))
	return db, nil
}
