// Copyright 2024 PingCAP, Inc.
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
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/simple"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type partitionProgress struct {
	partition       int32
	watermark       uint64
	watermarkOffset kafka.Offset

	eventsGroup map[int64]*util.EventsGroup
	decoder     common.Decoder
}

func newPartitionProgress(partition int32, decoder common.Decoder) *partitionProgress {
	return &partitionProgress{
		partition:   partition,
		eventsGroup: make(map[int64]*util.EventsGroup),
		decoder:     decoder,
	}
}

func (p *partitionProgress) updateWatermark(newWatermark uint64, offset kafka.Offset) {
	if newWatermark >= p.watermark {
		p.watermark = newWatermark
		p.watermarkOffset = offset
		log.Debug("watermark received", zap.Int32("partition", p.partition), zap.Any("offset", offset),
			zap.Uint64("watermark", newWatermark))
		return
	}
	readOldOffset := true
	if offset > p.watermarkOffset {
		readOldOffset = false
	}
	log.Warn("partition resolved ts fall back, ignore it",
		zap.Bool("readOldOffset", readOldOffset),
		zap.Int32("partition", p.partition),
		zap.Uint64("newWatermark", newWatermark), zap.Any("offset", offset),
		zap.Uint64("watermark", p.watermark), zap.Any("watermarkOffset", p.watermarkOffset))
}

type writer struct {
	progresses         []*partitionProgress
	ddlList            []*commonEvent.DDLEvent
	ddlWithMaxCommitTs map[int64]uint64
	mu                 sync.Mutex
	pendingDMLFlushes  []*dmlFlushTracker

	// this should be used by the canal-json, avro and open protocol
	partitionTableAccessor *common.PartitionTableAccessor

	eventRouter            *eventrouter.EventRouter
	protocol               config.Protocol
	maxMessageBytes        int
	maxBatchSize           int
	mysqlSink              sink.Sink
	enableTableAcrossNodes bool

	syncpointEnabled       bool
	syncpointInterval      time.Duration
	nextSyncpointTs        uint64
	lastSyncedSyncpointTs  uint64
	consumerSyncpointStore consumerSyncpointStore
}

type dmlFlushTracker struct {
	tableID     int64
	group       *util.EventsGroup
	maxCommitTs uint64
	total       int64
	done        chan struct{}
	flushed     atomic.Int64
}

func newWriter(ctx context.Context, o *option) *writer {
	w := &writer{
		protocol:               o.protocol,
		maxMessageBytes:        o.maxMessageBytes,
		maxBatchSize:           o.maxBatchSize,
		progresses:             make([]*partitionProgress, o.partitionNum),
		partitionTableAccessor: common.NewPartitionTableAccessor(),
		ddlList:                make([]*commonEvent.DDLEvent, 0),
		ddlWithMaxCommitTs:     make(map[int64]uint64),
		enableTableAcrossNodes: o.enableTableAcrossNodes,
		syncpointEnabled:       o.enableSyncpoint,
		syncpointInterval:      o.syncpointInterval,
	}
	var (
		db  *sql.DB
		err error
	)
	if o.upstreamTiDBDSN != "" {
		db, err = openDB(ctx, o.upstreamTiDBDSN)
		if err != nil {
			log.Panic("cannot open the upstream TiDB, handle key only enabled",
				zap.String("dsn", o.upstreamTiDBDSN))
		}
	}
	for i := 0; i < int(o.partitionNum); i++ {
		decoder, err := codec.NewEventDecoder(ctx, i, o.codecConfig, o.topic, db)
		if err != nil {
			log.Panic("cannot create the decoder", zap.Error(err))
		}
		w.progresses[i] = newPartitionProgress(int32(i), decoder)
	}

	eventRouter, err := eventrouter.NewEventRouter(o.sinkConfig, o.topic, false, o.protocol == config.ProtocolAvro)
	if err != nil {
		log.Panic("initialize the event router failed",
			zap.Any("protocol", o.protocol), zap.Any("topic", o.topic),
			zap.Any("dispatcherRules", o.sinkConfig.DispatchRules), zap.Error(err))
	}
	w.eventRouter = eventRouter
	log.Info("event router created", zap.Any("protocol", o.protocol),
		zap.Any("topic", o.topic), zap.Any("dispatcherRules", o.sinkConfig.DispatchRules))

	changefeedID := commonType.NewChangeFeedIDWithName("kafka-consumer", commonType.DefaultKeyspaceName)
	cfg := &config.ChangefeedConfig{
		ChangefeedID: changefeedID,
		SinkURI:      o.downstreamURI,
		SinkConfig:   o.sinkConfig,
	}
	w.mysqlSink, err = sink.New(ctx, cfg, changefeedID)
	if err != nil {
		log.Panic("cannot create the mysql sink", zap.Error(err))
	}
	if o.enableSyncpoint {
		store, err := newMySQLConsumerSyncpointStore(ctx, consumerSyncpointStoreConfig{
			downstreamURI: o.downstreamURI,
			consumerID:    o.groupID,
			topic:         o.topic,
			retention:     o.syncpointRetention,
		})
		if err != nil {
			log.Panic("cannot create consumer syncpoint store", zap.Error(err))
		}
		lastSyncpointTs, err := store.Init(ctx)
		if err != nil {
			log.Panic("cannot initialize consumer syncpoint store", zap.Error(err))
		}
		w.consumerSyncpointStore = store
		w.lastSyncedSyncpointTs = lastSyncpointTs
		if lastSyncpointTs > 0 {
			w.nextSyncpointTs = nextAlignedSyncpointTs(lastSyncpointTs, o.syncpointInterval)
		}
		log.Info("consumer syncpoint initialized",
			zap.Uint64("lastSyncedSyncpointTs", w.lastSyncedSyncpointTs),
			zap.Uint64("nextSyncpointTs", w.nextSyncpointTs),
			zap.Duration("syncpointInterval", o.syncpointInterval))
	}
	return w
}

func (w *writer) run(ctx context.Context) error {
	if w.consumerSyncpointStore != nil {
		defer func() {
			if err := w.consumerSyncpointStore.Close(); err != nil {
				log.Warn("close consumer syncpoint store failed", zap.Error(err))
			}
		}()
	}
	return w.mysqlSink.Run(ctx)
}

func (w *writer) flushDDLEvent(ctx context.Context, ddl *commonEvent.DDLEvent) error {
	tableIDs := w.getBlockTableIDs(ddl)
	commitTs := ddl.GetCommitTs()
	trackers := w.dispatchDMLEventsByTargetTs(commitTs, tableIDs)
	trackers = append(trackers, w.pendingDMLFlushesUpTo(commitTs, tableIDs)...)
	if err := w.waitDMLFlushes(ctx, trackers, "DDL", commitTs); err != nil {
		return err
	}
	return w.mysqlSink.WriteBlockEvent(ddl)
}

func (w *writer) getBlockTableIDs(ddl *commonEvent.DDLEvent) map[int64]struct{} {
	// The DDL event is delivered after all messages belongs to the tables which are blocked by the DDL event
	// so we can make assumption that the all DMLs received before the DDL event.
	// since one table's events may be produced to the different partitions, so we have to flush all partitions.
	// if block the whole database, flush all tables, otherwise flush the blocked tables.
	tableIDs := make(map[int64]struct{})
	switch ddl.GetBlockedTables().InfluenceType {
	case commonEvent.InfluenceTypeDB, commonEvent.InfluenceTypeAll:
		for _, progress := range w.progresses {
			for tableID := range progress.eventsGroup {
				tableIDs[tableID] = struct{}{}
			}
		}
	case commonEvent.InfluenceTypeNormal:
		for _, item := range ddl.GetBlockedTables().TableIDs {
			tableIDs[item] = struct{}{}
		}
	default:
		log.Panic("unsupported influence type", zap.Any("influenceType", ddl.GetBlockedTables().InfluenceType))
	}
	return tableIDs
}

// appendDDL enqueues a DDL event to be flushed later.
//
// DDLs may be received out of commit-ts order (e.g. due to MQ delivery or buffering), so Write() sorts
// ddlList by commit-ts before executing. ddlWithMaxCommitTs is a guard against per-table commit-ts
// regressions: executing an older DDL after a newer one may corrupt downstream schema/DML ordering.
func (w *writer) appendDDL(ddl *commonEvent.DDLEvent) {
	// If commitTs goes backwards for a blocked table, ignore this DDL instead of applying it out of order.
	tableIDs := w.getBlockTableIDs(ddl)
	for tableID := range tableIDs {
		maxCommitTs, ok := w.ddlWithMaxCommitTs[tableID]
		if ok && ddl.GetCommitTs() < maxCommitTs {
			log.Warn("DDL CommitTs < maxCommitTsDDL.CommitTs",
				zap.Uint64("commitTs", ddl.GetCommitTs()),
				zap.Uint64("maxCommitTs", maxCommitTs),
				zap.String("DDL", ddl.Query))
			return
		}
	}

	w.ddlList = append(w.ddlList, ddl)
	for tableID := range tableIDs {
		w.ddlWithMaxCommitTs[tableID] = ddl.GetCommitTs()
	}
}

func (w *writer) globalWatermark() uint64 {
	watermark := uint64(math.MaxUint64)
	for _, progress := range w.progresses {
		if progress.watermark < watermark {
			watermark = progress.watermark
		}
	}
	return watermark
}

func (w *writer) flushDMLEventsByWatermark(ctx context.Context) error {
	return w.flushDMLEventsByTargetTs(ctx, w.globalWatermark())
}

func (w *writer) flushDMLEventsByTargetTs(ctx context.Context, targetTs uint64) error {
	trackers := w.dispatchDMLEventsByTargetTs(targetTs, nil)
	trackers = append(trackers, w.pendingDMLFlushesUpTo(targetTs, nil)...)
	return w.waitDMLFlushes(ctx, trackers, "watermark", targetTs)
}

func (w *writer) dispatchDMLEventsByTargetTs(targetTs uint64, tableIDs map[int64]struct{}) []*dmlFlushTracker {
	var (
		total    int
		trackers []*dmlFlushTracker
	)

	for _, p := range w.progresses {
		for tableID, group := range p.eventsGroup {
			if tableIDs != nil {
				if _, ok := tableIDs[tableID]; !ok {
					continue
				}
			}
			resolvedEvents := make([]*commonEvent.DMLEvent, 0)
			before := len(resolvedEvents)
			resolvedEvents = group.ResolveInto(targetTs, resolvedEvents)
			resolvedCount := len(resolvedEvents) - before
			if resolvedCount == 0 {
				continue
			}

			tracker := &dmlFlushTracker{
				tableID:     tableID,
				group:       group,
				maxCommitTs: resolvedEvents[len(resolvedEvents)-1].GetCommitTs(),
				total:       int64(resolvedCount),
				done:        make(chan struct{}),
			}
			w.trackDMLFlush(tracker)
			trackers = append(trackers, tracker)
			total += resolvedCount

			for _, e := range resolvedEvents {
				e.AddPostFlushFunc(func() {
					if tracker.flushed.Inc() == tracker.total {
						w.mu.Lock()
						if tracker.maxCommitTs > tracker.group.AppliedWatermark {
							tracker.group.AppliedWatermark = tracker.maxCommitTs
						}
						w.mu.Unlock()
						close(tracker.done)
					}
				})
				w.mysqlSink.AddDMLEvent(e)
				log.Debug("dispatch DML event", zap.Int64("tableID", e.GetTableID()),
					zap.Uint64("commitTs", e.GetCommitTs()), zap.Any("startTs", e.GetStartTs()))
			}
		}
	}

	if total > 0 {
		log.Info("dispatch DML events by watermark", zap.Uint64("watermark", targetTs), zap.Int("total", total))
	}
	return trackers
}

func (w *writer) trackDMLFlush(tracker *dmlFlushTracker) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pendingDMLFlushes = append(w.pendingDMLFlushes, tracker)
}

func (w *writer) pendingDMLFlushesUpTo(targetTs uint64, tableIDs map[int64]struct{}) []*dmlFlushTracker {
	w.mu.Lock()
	defer w.mu.Unlock()

	trackers := make([]*dmlFlushTracker, 0)
	for _, tracker := range w.pendingDMLFlushes {
		if tracker.maxCommitTs > targetTs {
			continue
		}
		if tableIDs != nil {
			if _, ok := tableIDs[tracker.tableID]; !ok {
				continue
			}
		}
		trackers = append(trackers, tracker)
	}
	return trackers
}

func (w *writer) cleanupDoneDMLFlushes() {
	w.mu.Lock()
	defer w.mu.Unlock()

	remaining := w.pendingDMLFlushes[:0]
	for _, tracker := range w.pendingDMLFlushes {
		select {
		case <-tracker.done:
		default:
			remaining = append(remaining, tracker)
		}
	}
	clear(w.pendingDMLFlushes[len(remaining):])
	w.pendingDMLFlushes = remaining
}

func (w *writer) waitDMLFlushes(
	ctx context.Context,
	trackers []*dmlFlushTracker,
	reason string,
	targetTs uint64,
) error {
	if len(trackers) == 0 {
		return nil
	}
	seen := make(map[*dmlFlushTracker]struct{}, len(trackers))
	uniqueTrackers := trackers[:0]
	var total int64
	for _, tracker := range trackers {
		if _, ok := seen[tracker]; ok {
			continue
		}
		seen[tracker] = struct{}{}
		uniqueTrackers = append(uniqueTrackers, tracker)
		total += tracker.total
	}

	log.Info("wait DML events flushed", zap.String("reason", reason), zap.Uint64("targetTs", targetTs),
		zap.Int("groups", len(uniqueTrackers)), zap.Int64("total", total))
	start := time.Now()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for _, tracker := range uniqueTrackers {
		for {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-tracker.done:
				goto nextTracker
			case <-ticker.C:
				var flushed int64
				for _, item := range uniqueTrackers {
					flushed += item.flushed.Load()
				}
				log.Warn("DML events cannot be flushed in time", zap.String("reason", reason),
					zap.Uint64("targetTs", targetTs), zap.Int64("total", total), zap.Int64("flushed", flushed))
			}
		}
	nextTracker:
	}

	log.Info("DML events flushed", zap.String("reason", reason), zap.Uint64("targetTs", targetTs),
		zap.Int64("total", total), zap.Duration("duration", time.Since(start)))
	w.cleanupDoneDMLFlushes()
	return nil
}

func (w *writer) flushDDLEventsUpTo(ctx context.Context, targetTs uint64) {
	if len(w.ddlList) > 1 {
		sort.SliceStable(w.ddlList, func(i, j int) bool {
			return w.ddlList[i].GetCommitTs() < w.ddlList[j].GetCommitTs()
		})
	}
	remaining := make([]*commonEvent.DDLEvent, 0, len(w.ddlList))
	for _, ddl := range w.ddlList {
		if ddl.GetCommitTs() > targetTs {
			remaining = append(remaining, ddl)
			continue
		}
		if err := w.flushDDLEvent(ctx, ddl); err != nil {
			log.Panic("write DDL event failed", zap.Error(err),
				zap.String("DDL", ddl.Query), zap.Uint64("commitTs", ddl.GetCommitTs()))
		}
	}
	w.ddlList = remaining
}

func (w *writer) maybeFlushConsumerSyncpoint(ctx context.Context, watermark uint64) (bool, error) {
	if !w.syncpointEnabled || w.consumerSyncpointStore == nil || w.syncpointInterval <= 0 || watermark == 0 {
		return false, nil
	}
	if w.nextSyncpointTs == 0 {
		w.nextSyncpointTs = calculateFloorAlignedSyncpointTs(watermark, w.syncpointInterval)
		if w.nextSyncpointTs == 0 {
			return false, nil
		}
	}
	flushed := false
	for w.nextSyncpointTs != 0 && w.nextSyncpointTs <= watermark {
		targetTs := w.nextSyncpointTs
		if targetTs <= w.lastSyncedSyncpointTs {
			w.nextSyncpointTs = nextAlignedSyncpointTs(targetTs, w.syncpointInterval)
			continue
		}
		w.flushDDLEventsUpTo(ctx, targetTs)
		trackers := w.dispatchDMLEventsByTargetTs(targetTs, nil)
		trackers = append(trackers, w.pendingDMLFlushesUpTo(targetTs, nil)...)
		if err := w.waitDMLFlushes(ctx, trackers, "consumer syncpoint", targetTs); err != nil {
			return false, err
		}
		if err := w.consumerSyncpointStore.Write(ctx, targetTs); err != nil {
			return false, err
		}
		flushed = true
		w.lastSyncedSyncpointTs = targetTs
		w.nextSyncpointTs = nextAlignedSyncpointTs(targetTs, w.syncpointInterval)
		log.Info("consumer syncpoint flushed",
			zap.Uint64("syncpointTs", targetTs),
			zap.Uint64("nextSyncpointTs", w.nextSyncpointTs))
	}
	return flushed, nil
}

// WriteMessage is to decode kafka message to event.
// return true if the message is flushed to the downstream.
// return error if flush messages failed.
func (w *writer) WriteMessage(ctx context.Context, message *kafka.Message) bool {
	var (
		partition = message.TopicPartition.Partition
		offset    = message.TopicPartition.Offset
	)

	progress := w.progresses[partition]
	progress.decoder.AddKeyValue(message.Key, message.Value)

	messageType, hasNext := progress.decoder.HasNext()
	if !hasNext {
		log.Panic("try to fetch the next event failed, this should not happen", zap.Bool("hasNext", hasNext))
	}

	needFlush := false
	switch messageType {
	case common.MessageTypeResolved:
		newWatermark := progress.decoder.NextResolvedEvent()
		progress.updateWatermark(newWatermark, offset)
		needFlush = true
	case common.MessageTypeDDL:
		// for some protocol, DDL would be dispatched to all partitions,
		// Consider that DDL a, b, c received from partition-0, the latest DDL is c,
		// if we receive `a` from partition-1, which would be seemed as DDL regression,
		// then cause the consumer panic, but it was a duplicate one.
		// so we only handle DDL received from partition-0 should be enough.
		// but all DDL event messages should be consumed.
		ddl := progress.decoder.NextDDLEvent()

		if dec, ok := progress.decoder.(*simple.Decoder); ok {
			cachedEvents := dec.GetCachedEvents()
			for _, row := range cachedEvents {
				log.Info("simple protocol cached event resolved, append to the group",
					zap.Int64("tableID", row.GetTableID()), zap.Uint64("commitTs", row.CommitTs),
					zap.Int32("partition", partition), zap.Any("offset", offset))
				w.appendRow2Group(row, progress, offset)
			}
		}

		w.onDDL(ddl)
		// DDL is broadcast to all partitions, but only handle the DDL from partition-0.
		if partition != 0 {
			return false
		}

		// the Query maybe empty if using simple protocol, it's comes from `bootstrap` event, no need to handle it.
		if ddl.Query == "" {
			return false
		}
		w.appendDDL(ddl)
		log.Info("DDL event received",
			zap.Int32("partition", partition), zap.Any("offset", offset),
			zap.String("schema", ddl.GetSchemaName()), zap.String("table", ddl.GetTableName()),
			zap.Uint64("commitTs", ddl.GetCommitTs()), zap.String("query", ddl.Query),
			zap.Any("blockedTables", ddl.GetBlockedTables()))

		needFlush = true
	case common.MessageTypeRow:
		var counter int
		row := progress.decoder.NextDMLEvent()
		if row == nil {
			if w.protocol != config.ProtocolSimple {
				log.Panic("DML event is nil, it's not expected",
					zap.Int32("partition", partition), zap.Any("offset", offset))
			}
			log.Debug("DML event is nil, it's cached", zap.Int32("partition", partition), zap.Any("offset", offset))
			break
		}

		w.appendRow2Group(row, progress, offset)
		counter++
		for {
			_, hasNext = progress.decoder.HasNext()
			if !hasNext {
				break
			}
			row = progress.decoder.NextDMLEvent()
			w.appendRow2Group(row, progress, offset)
			counter++
		}
		// If the message containing only one event exceeds the length limit, CDC will allow it and issue a warning.
		if len(message.Key)+len(message.Value) > w.maxMessageBytes && counter > 1 {
			log.Panic("kafka max-messages-bytes exceeded",
				zap.Int32("partition", partition), zap.Any("offset", offset),
				zap.Int("max-message-bytes", w.maxMessageBytes),
				zap.Int("receivedBytes", len(message.Key)+len(message.Value)))
		}
		if counter > w.maxBatchSize {
			log.Panic("Open Protocol max-batch-size exceeded",
				zap.Int("maxBatchSize", w.maxBatchSize), zap.Int("actualBatchSize", counter),
				zap.Int32("partition", partition), zap.Any("offset", offset))
		}
	default:
		log.Panic("unknown message type", zap.Any("messageType", messageType),
			zap.Int32("partition", partition), zap.Any("offset", offset))
	}
	if needFlush {
		return w.Write(ctx, messageType)
	}
	return false
}

// Write will synchronously write data downstream
func (w *writer) Write(ctx context.Context, messageType common.MessageType) bool {
	// DDL events can be received out of commit-ts order (e.g. due to protocol-level broadcasting and
	// buffering differences between DDL kinds). We must execute DDLs in commit-ts order; otherwise a
	// "future" DDL that is not yet eligible (commitTs > watermark) can block executing earlier DDLs
	// that are already eligible, and the subsequent watermark-based DML flush can observe an out-of-date
	// downstream schema (e.g. DML applied before its ALTER TABLE), causing test failures like common_1.
	if len(w.ddlList) > 1 {
		sort.SliceStable(w.ddlList, func(i, j int) bool {
			return w.ddlList[i].GetCommitTs() < w.ddlList[j].GetCommitTs()
		})
	}

	watermark := w.globalWatermark()
	consumerSyncpointFlushed := false
	if messageType == common.MessageTypeResolved && w.syncpointEnabled {
		var err error
		consumerSyncpointFlushed, err = w.maybeFlushConsumerSyncpoint(ctx, watermark)
		if err != nil {
			log.Panic("flush consumer syncpoint failed", zap.Error(err), zap.Uint64("watermark", watermark))
		}
	}
	ddlList := make([]*commonEvent.DDLEvent, 0)
	for i, todoDDL := range w.ddlList {
		// DDL ordering must follow commitTs (see appendDDL). Traditionally we wait until the global
		// resolved-ts (watermark) has reached the DDL commitTs, which guarantees all partitions have
		// consumed events <= commitTs.
		//
		// However, some DDLs are safe to execute as soon as they are received. In particular, CREATE
		// SCHEMA and "independent" CREATE TABLE (i.e. ones that do not depend on any existing table)
		// do not need to wait for watermark to protect DML ordering, and waiting can deadlock integration
		// tests that intentionally pause dispatcher creation (thus holding back the upstream resolved-ts/
		// watermark).
		//
		// Safety guard: CREATE TABLE ... LIKE ... is also ActionCreateTable, but it depends on the referenced
		// table schema being present and up-to-date downstream. The event builder encodes that dependency by
		// populating BlockedTableNames and/or adding referenced table IDs (or partition IDs) into
		// BlockedTables.TableIDs. We only bypass watermark for CREATE TABLE when the DDL only blocks the
		// special DDL span and has no referenced blocked table names.
		action := timodel.ActionType(todoDDL.Type)
		bypassWatermark := false
		switch action {
		case timodel.ActionCreateSchema:
			bypassWatermark = true
		case timodel.ActionCreateTable:
			blockedTables := todoDDL.GetBlockedTables()
			bypassWatermark = blockedTables != nil &&
				blockedTables.InfluenceType == commonEvent.InfluenceTypeNormal &&
				len(blockedTables.TableIDs) == 1 &&
				blockedTables.TableIDs[0] == commonType.DDLSpanTableID &&
				len(todoDDL.GetBlockedTableNames()) == 0
		}
		if !bypassWatermark && todoDDL.GetCommitTs() > watermark {
			ddlList = append(ddlList, w.ddlList[i:]...)
			break
		}
		if err := w.flushDDLEvent(ctx, todoDDL); err != nil {
			log.Panic("write DDL event failed", zap.Error(err),
				zap.String("DDL", todoDDL.Query), zap.Uint64("commitTs", todoDDL.GetCommitTs()))
		}
	}

	if messageType == common.MessageTypeResolved {
		// With consumer syncpoint enabled, normal resolved events should keep the
		// consumer moving: dispatch DMLs to the sink and only wait at aligned
		// syncpoint barriers. Without syncpoint, keep the historical behavior so
		// offset commits only happen after DMLs are flushed.
		if w.syncpointEnabled {
			w.dispatchDMLEventsByTargetTs(watermark, nil)
		} else {
			// since watermark is broadcast to all partitions, so that each partition can flush events individually.
			err := w.flushDMLEventsByWatermark(ctx)
			if err != nil {
				log.Panic("flush dml events by the watermark failed", zap.Error(err))
			}
		}
	}

	w.ddlList = ddlList
	// The DDL events will only execute in partition0
	if messageType == common.MessageTypeDDL && len(w.ddlList) != 0 {
		log.Info("some DDL events will be flushed in the future",
			zap.Uint64("watermark", watermark),
			zap.Int("length", len(w.ddlList)))
		return false
	}
	if messageType == common.MessageTypeResolved && w.syncpointEnabled {
		return consumerSyncpointFlushed
	}
	return true
}

func calculateAlignedSyncpointTs(ts uint64, interval time.Duration, skipSyncpointAtTs bool) uint64 {
	if interval <= 0 {
		return 0
	}
	k := oracle.GetTimeFromTS(ts).Sub(time.Unix(0, 0)) / interval
	if oracle.GetTimeFromTS(ts).Sub(time.Unix(0, 0))%interval != 0 || oracle.ExtractLogical(ts) != 0 {
		k++
	} else if skipSyncpointAtTs {
		k++
	}
	return oracle.GoTimeToTS(time.Unix(0, 0).Add(k * interval))
}

func nextAlignedSyncpointTs(ts uint64, interval time.Duration) uint64 {
	return calculateAlignedSyncpointTs(ts, interval, true)
}

func calculateFloorAlignedSyncpointTs(ts uint64, interval time.Duration) uint64 {
	if interval <= 0 {
		return 0
	}
	k := oracle.GetTimeFromTS(ts).Sub(time.Unix(0, 0)) / interval
	if k <= 0 {
		return 0
	}
	return oracle.GoTimeToTS(time.Unix(0, 0).Add(k * interval))
}

func (w *writer) onDDL(ddl *commonEvent.DDLEvent) {
	switch w.protocol {
	case config.ProtocolCanalJSON, config.ProtocolOpen, config.ProtocolAvro:
	default:
		return
	}
	// TODO: support more corner cases
	// e.g. create partition table + drop table(rename table) + create normal table: the partitionTableAccessor should drop the table when the table become normal.
	switch timodel.ActionType(ddl.Type) {
	case timodel.ActionCreateTable:
		stmt, err := parser.New().ParseOneStmt(ddl.Query, "", "")
		if err != nil {
			log.Panic("parse ddl query failed", zap.String("query", ddl.Query), zap.Error(err))
		}
		if v, ok := stmt.(*ast.CreateTableStmt); ok && v.Partition != nil {
			w.partitionTableAccessor.Add(ddl.GetSchemaName(), ddl.GetTableName())
		}
	case timodel.ActionRenameTable:
		if w.partitionTableAccessor.IsPartitionTable(ddl.ExtraSchemaName, ddl.ExtraTableName) {
			w.partitionTableAccessor.Add(ddl.GetSchemaName(), ddl.GetTableName())
		}
	}
}

func (w *writer) checkPartition(row *commonEvent.DMLEvent, partition int32, offset kafka.Offset) {
	var (
		partitioner  = w.eventRouter.GetPartitionGenerator(row.TableInfo.GetSchemaName(), row.TableInfo.GetTableName())
		partitionNum = int32(len(w.progresses))
	)
	for {
		change, ok := row.GetNextRow()
		if !ok {
			row.Rewind()
			break
		}

		target, _, err := partitioner.GeneratePartitionIndexAndKey(&change, partitionNum, row.TableInfo, row.GetCommitTs())
		if err != nil {
			log.Panic("generate partition index and key failed", zap.Error(err))
		}

		if partition != target {
			log.Panic("dml event dispatched to the wrong partition",
				zap.Int32("partition", partition), zap.Int32("expected", target),
				zap.Int("partitionNum", len(w.progresses)), zap.Any("offset", offset),
				zap.Int64("tableID", row.GetTableID()), zap.Stringer("row", row),
			)
		}
	}
}

func (w *writer) appendRow2Group(dml *commonEvent.DMLEvent, progress *partitionProgress, offset kafka.Offset) {
	w.checkPartition(dml, progress.partition, offset)
	// if the kafka cluster is normal, this should not hit.
	// else if the cluster is abnormal, the consumer may consume old message, then cause the watermark fallback.
	var (
		tableID  = dml.GetTableID()
		schema   = dml.TableInfo.GetSchemaName()
		table    = dml.TableInfo.GetTableName()
		commitTs = dml.GetCommitTs()
	)
	group := progress.eventsGroup[tableID]
	if group == nil {
		group = util.NewEventsGroup(progress.partition, tableID)
		progress.eventsGroup[tableID] = group
	}
	w.mu.Lock()
	appliedWatermark := group.AppliedWatermark
	highWatermark := group.HighWatermark
	w.mu.Unlock()
	// IMPORTANT: Kafka offsets are append-only, but CommitTs can go backwards after
	// a TiCDC restart/retry (at-least-once replay). We must not drop such events
	// solely based on a "seen" watermark (e.g. HighWatermark). The only safe
	// ignore condition is "already flushed to downstream".
	if commitTs <= appliedWatermark {
		log.Warn("DML event replayed after applied, ignore it",
			zap.Int64("tableID", tableID), zap.Int32("partition", group.Partition),
			zap.Uint64("commitTs", commitTs), zap.Any("offset", offset),
			zap.Uint64("appliedWatermark", appliedWatermark), zap.Uint64("highWatermark", highWatermark),
			zap.Uint64("partitionWatermark", progress.watermark), zap.Any("watermarkOffset", progress.watermarkOffset),
			zap.String("schema", schema), zap.String("table", table), zap.Any("protocol", w.protocol))
		return
	}
	forceInsert := commitTs < highWatermark || commitTs < progress.watermark || w.enableTableAcrossNodes
	if forceInsert {
		log.Warn("DML event commit ts fallback, append with forceInsert",
			zap.Int32("partition", group.Partition), zap.Any("offset", offset),
			zap.Uint64("commitTs", commitTs), zap.Uint64("highWatermark", highWatermark),
			zap.Uint64("appliedWatermark", appliedWatermark),
			zap.Uint64("partitionWatermark", progress.watermark), zap.Any("watermarkOffset", progress.watermarkOffset),
			zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
			zap.Stringer("eventType", dml.RowTypes[0]), zap.Any("protocol", w.protocol),
			zap.Bool("IsPartition", dml.TableInfo.TableName.IsPartition))
		group.Append(dml, true)
		return
	}
	group.Append(dml, false)
	log.Info("DML event append to the group",
		zap.Int32("partition", group.Partition), zap.Any("offset", offset),
		zap.Uint64("commitTs", commitTs), zap.Uint64("highWatermark", group.HighWatermark),
		zap.Uint64("appliedWatermark", group.AppliedWatermark),
		zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
		zap.Stringer("eventType", dml.RowTypes[0]))
}

func openDB(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Error("open db failed", zap.Error(err))
		return nil, errors.Trace(err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(10 * time.Minute)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		log.Error("ping db failed", zap.String("dsn", dsn), zap.Error(err))
		return nil, errors.Trace(err)
	}
	log.Info("open db success", zap.String("dsn", dsn))
	return db, nil
}
