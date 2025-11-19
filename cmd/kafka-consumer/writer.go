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
		log.Info("watermark received", zap.Int32("partition", p.partition), zap.Any("offset", offset),
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

	// this should be used by the canal-json, avro and open protocol
	partitionTableAccessor *common.PartitionTableAccessor

	eventRouter            *eventrouter.EventRouter
	protocol               config.Protocol
	maxMessageBytes        int
	maxBatchSize           int
	mysqlSink              sink.Sink
	enableTableAcrossNodes bool
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

	changefeedID := commonType.NewChangeFeedIDWithName("kafka-consumer", commonType.DefaultKeyspaceNamme)
	cfg := &config.ChangefeedConfig{
		ChangefeedID: changefeedID,
		SinkURI:      o.downstreamURI,
		SinkConfig:   o.sinkConfig,
	}
	w.mysqlSink, err = sink.New(ctx, cfg, changefeedID)
	if err != nil {
		log.Panic("cannot create the mysql sink", zap.Error(err))
	}
	return w
}

func (w *writer) run(ctx context.Context) error {
	return w.mysqlSink.Run(ctx)
}

func (w *writer) flushDDLEvent(ctx context.Context, ddl *commonEvent.DDLEvent) error {
	var (
		done = make(chan struct{}, 1)

		total   int
		flushed atomic.Int64
	)

	tableIDs := w.getBlockTableIDs(ddl)
	commitTs := ddl.GetCommitTs()
	resolvedEvents := make([]*commonEvent.DMLEvent, 0)
	for tableID := range tableIDs {
		for _, progress := range w.progresses {
			g, ok := progress.eventsGroup[tableID]
			if !ok {
				continue
			}
			events := g.Resolve(commitTs)
			resolvedCount := len(events)
			if resolvedCount == 0 {
				continue
			}
			resolvedEvents = append(resolvedEvents, events...)
			total += resolvedCount
		}
	}

	if total == 0 {
		return w.mysqlSink.WriteBlockEvent(ddl)
	}
	for _, e := range resolvedEvents {
		e.AddPostFlushFunc(func() {
			if flushed.Inc() == int64(total) {
				close(done)
			}
		})
		w.mysqlSink.AddDMLEvent(e)
	}

	log.Info("flush DML events before DDL", zap.Uint64("DDLCommitTs", commitTs), zap.Int("total", total))
	start := time.Now()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-done:
		log.Info("flush DML events before DDL done", zap.Uint64("DDLCommitTs", commitTs),
			zap.Int("total", total), zap.Duration("duration", time.Since(start)),
			zap.Any("tables", tableIDs))
	case <-ticker.C:
		log.Panic("DDL event timeout, since the DML events are not flushed in time",
			zap.Uint64("DDLCommitTs", commitTs), zap.String("query", ddl.Query),
			zap.Int("total", total), zap.Int64("flushed", flushed.Load()))
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

// append DDL wait to be handled, only consider the constraint among DDLs.
// for DDL a / b received in the order, a.CommitTs < b.CommitTs should be true.
func (w *writer) appendDDL(ddl *commonEvent.DDLEvent) {
	// DDL CommitTs fallback, just crash it to indicate the bug.
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
	var (
		done = make(chan struct{}, 1)

		total   int
		flushed atomic.Int64
	)

	watermark := w.globalWatermark()
	resolvedEvents := make([]*commonEvent.DMLEvent, 0)
	for _, p := range w.progresses {
		for _, group := range p.eventsGroup {
			events := group.Resolve(watermark)
			resolvedCount := len(events)
			if resolvedCount == 0 {
				continue
			}
			resolvedEvents = append(resolvedEvents, events...)
			total += resolvedCount
		}
	}
	if total == 0 {
		return nil
	}
	for _, e := range resolvedEvents {
		e.AddPostFlushFunc(func() {
			if flushed.Inc() == int64(total) {
				close(done)
			}
		})
		w.mysqlSink.AddDMLEvent(e)
		log.Info("flush DML event", zap.Int64("tableID", e.GetTableID()), zap.Uint64("commitTs", e.GetCommitTs()), zap.Any("startTs", e.GetStartTs()))
	}

	log.Info("flush DML events by watermark", zap.Uint64("watermark", watermark), zap.Int("total", total))
	start := time.Now()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-done:
		log.Info("flush DML events done", zap.Uint64("watermark", watermark),
			zap.Int("total", total), zap.Duration("duration", time.Since(start)))
	case <-ticker.C:
		log.Panic("DML events cannot be flushed in 1 minute", zap.Uint64("watermark", watermark),
			zap.Int("total", total), zap.Int64("flushed", flushed.Load()))
	}
	return nil
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
			log.Warn("DML event is nil, it's cached ", zap.Int32("partition", partition), zap.Any("offset", offset))
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
	watermark := w.globalWatermark()
	ddlList := make([]*commonEvent.DDLEvent, 0)
	for _, todoDDL := range w.ddlList {
		// watermark is the min value for all partitions,
		// the DDL only executed by the first partition, other partitions may be slow
		// so that the watermark can be smaller than the DDL's commitTs,
		// which means some DML events may not be consumed yet, so cannot execute the DDL right now.
		if todoDDL.GetCommitTs() > watermark {
			ddlList = append(ddlList, todoDDL)
			continue
		}
		if err := w.flushDDLEvent(ctx, todoDDL); err != nil {
			log.Panic("write DDL event failed", zap.Error(err),
				zap.String("DDL", todoDDL.Query), zap.Uint64("commitTs", todoDDL.GetCommitTs()))
		}
	}

	if messageType == common.MessageTypeResolved {
		// since watermark is broadcast to all partitions, so that each partition can flush events individually.
		err := w.flushDMLEventsByWatermark(ctx)
		if err != nil {
			log.Panic("flush dml events by the watermark failed", zap.Error(err))
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
	return true
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
				zap.Int64("tableID", row.GetTableID()), zap.Any("row", row),
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
	if commitTs >= group.HighWatermark {
		group.Append(dml, false)
		log.Info("DML event append to the group",
			zap.Int32("partition", group.Partition), zap.Any("offset", offset),
			zap.Uint64("commitTs", commitTs), zap.Uint64("HighWatermark", group.HighWatermark),
			zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
			zap.Stringer("eventType", dml.RowTypes[0]))
		return
	}
	if w.enableTableAcrossNodes {
		log.Warn("DML events fallback, but enableTableAcrossNodes is true, still append it",
			zap.Int32("partition", group.Partition), zap.Any("offset", offset),
			zap.Uint64("commitTs", commitTs), zap.Uint64("HighWatermark", group.HighWatermark),
			zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
			zap.Stringer("eventType", dml.RowTypes[0]))
		group.Append(dml, true)
		return
	}
	switch w.protocol {
	case config.ProtocolSimple, config.ProtocolDebezium:
		// simple protocol set the table id for all row message, it can be known which table the row message belongs to,
		// also consider the table partition.
		// open protocol set the partition table id if the table is partitioned.
		// for normal table, the table id is generated by the fake table id generator by using schema and table name.
		// so one event group for one normal table or one table partition, replayed messages can be ignored.
		log.Warn("DML event fallback row, since less than the group high watermark, ignore it",
			zap.Int32("partition", progress.partition), zap.Any("offset", offset),
			zap.Uint64("commitTs", commitTs), zap.Uint64("highWatermark", group.HighWatermark),
			zap.Any("partitionWatermark", progress.watermark), zap.Any("watermarkOffset", progress.watermarkOffset),
			zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
			zap.Stringer("eventType", dml.RowTypes[0]),
			// zap.Any("columns", row.Columns), zap.Any("preColumns", row.PreColumns),
			zap.Any("protocol", w.protocol), zap.Bool("IsPartition", dml.TableInfo.TableName.IsPartition))
	case config.ProtocolCanalJSON, config.ProtocolOpen, config.ProtocolAvro:
		// for partition table, the canal-json, avro and open-protocol message cannot assign physical table id to each dml message,
		// we cannot distinguish whether it's a real fallback event or not, still append it.
		if w.partitionTableAccessor.IsPartitionTable(schema, table) {
			log.Warn("DML events fallback, but it's canal-json, avro or open-protocol and the table is a partition table, still append it",
				zap.Int32("partition", group.Partition), zap.Any("offset", offset),
				zap.Uint64("commitTs", commitTs), zap.Uint64("highWatermark", group.HighWatermark),
				zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
				zap.Stringer("eventType", dml.RowTypes[0]))
			group.Append(dml, true)
			return
		}
		log.Warn("DML event fallback row, since less than the group high watermark, ignore it",
			zap.Int32("partition", progress.partition), zap.Any("offset", offset),
			zap.Uint64("commitTs", commitTs), zap.Uint64("HighWatermark", group.HighWatermark),
			zap.Any("partitionWatermark", progress.watermark), zap.Any("watermarkOffset", progress.watermarkOffset),
			zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
			zap.Stringer("eventType", dml.RowTypes[0]),
			// zap.Any("columns", row.Columns), zap.Any("preColumns", row.PreColumns),
			zap.Any("protocol", w.protocol), zap.Bool("IsPartition", dml.TableInfo.TableName.IsPartition))
	default:
		log.Panic("unknown protocol", zap.Any("protocol", w.protocol))
	}
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
