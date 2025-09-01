// Copyright 2025 PingCAP, Inc.
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

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	putil "github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type partitionProgress struct {
	partition   int32
	watermark   uint64
	eventGroups map[int64]*eventsGroup
	decoder     common.Decoder
}

func newPartitionProgress(partition int32, decoder common.Decoder) *partitionProgress {
	return &partitionProgress{
		partition:   partition,
		eventGroups: make(map[int64]*eventsGroup),
		decoder:     decoder,
	}
}

func (p *partitionProgress) updateWatermark(newWatermark uint64) {
	if newWatermark >= p.watermark {
		p.watermark = newWatermark
		log.Info("watermark received",
			zap.Uint64("watermark", newWatermark))
		return
	}
	log.Warn("partition resolved ts fall back, ignore it, since consumer read old  message",
		zap.Uint64("newWatermark", newWatermark),
		zap.Uint64("watermark", p.watermark), zap.Any("watermark", p.watermark))
}

type writer struct {
	progresses         []*partitionProgress
	ddlList            []*commonEvent.DDLEvent
	ddlWithMaxCommitTs map[int64]uint64

	// this should only be used by the canal-json protocol
	partitionTableAccessor *partitionTableAccessor

	eventRouter *eventrouter.EventRouter
	protocol    config.Protocol
	mysqlSink   sink.Sink
}

func newWriter(ctx context.Context, o *option) *writer {
	w := &writer{
		protocol:               o.protocol,
		progresses:             make([]*partitionProgress, o.partitionNum),
		partitionTableAccessor: newPartitionTableAccessor(),
		ddlList:                make([]*commonEvent.DDLEvent, 0),
		ddlWithMaxCommitTs:     make(map[int64]uint64),
	}
	var (
		db  *sql.DB
		err error
	)
	tz, err := putil.GetTimezone(o.timezone)
	if err != nil {
		log.Panic("can not load timezone", zap.Error(err))
	}

	codecConfig := common.NewConfig(o.protocol)
	codecConfig.TimeZone = tz
	codecConfig.EnableTiDBExtension = o.enableTiDBExtension
	// the TiDB source ID should never be set to 0
	o.replicaConfig.Sink.TiDBSourceID = 1
	o.replicaConfig.Sink.Protocol = putil.AddressOf(o.protocol.String())

	for i := 0; i < int(o.partitionNum); i++ {
		decoder, err := codec.NewEventDecoder(ctx, i, codecConfig, o.topic, db)
		if err != nil {
			log.Panic("cannot create the decoder", zap.Error(err))
		}
		w.progresses[i] = newPartitionProgress(int32(i), decoder)
	}

	eventRouter, err := eventrouter.NewEventRouter(o.replicaConfig.Sink, o.topic, false, o.protocol == config.ProtocolAvro)
	if err != nil {
		log.Panic("initialize the event router failed",
			zap.Any("protocol", o.protocol), zap.Any("topic", o.topic),
			zap.Any("dispatcherRules", o.replicaConfig.Sink.DispatchRules), zap.Error(err))
	}
	w.eventRouter = eventRouter
	log.Info("event router created", zap.Any("protocol", o.protocol),
		zap.Any("topic", o.topic), zap.Any("dispatcherRules", o.replicaConfig.Sink.DispatchRules))

	changefeedID := commonType.NewChangeFeedIDWithName("pulsar-consumer")
	cfg := &config.ChangefeedConfig{
		ChangefeedID: changefeedID,
		SinkURI:      o.downstreamURI,
		SinkConfig:   o.replicaConfig.Sink,
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
			g, ok := progress.eventGroups[tableID]
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
			for tableID := range progress.eventGroups {
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
		for _, group := range p.eventGroups {
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

// WriteMessage is to decode pulsar message to event.
// return true if the message is flushed to the downstream.
// return error if flush messages failed.
func (w *writer) WriteMessage(ctx context.Context, message pulsar.Message) bool {
	progress := w.progresses[0]
	progress.decoder.AddKeyValue([]byte(message.Key()), message.Payload())

	messageType, hasNext := progress.decoder.HasNext()
	if !hasNext {
		log.Panic("try to fetch the next event failed, this should not happen", zap.Bool("hasNext", hasNext))
	}

	needFlush := false
	switch messageType {
	case common.MessageTypeResolved:
		newWatermark := progress.decoder.NextResolvedEvent()
		progress.updateWatermark(newWatermark)
		needFlush = true
	case common.MessageTypeDDL:
		// for some protocol, DDL would be dispatched to all partitions,
		// Consider that DDL a, b, c received from partition-0, the latest DDL is c,
		// if we receive `a` from partition-1, which would be seemed as DDL regression,
		// then cause the consumer panic, but it was a duplicate one.
		// so we only handle DDL received from partition-0 should be enough.
		// but all DDL event messages should be consumed.
		ddl := progress.decoder.NextDDLEvent()

		w.onDDL(ddl)

		// the Query maybe empty if using simple protocol, it's comes from `bootstrap` event, no need to handle it.
		if ddl.Query == "" {
			return false
		}
		w.appendDDL(ddl)
		log.Info("DDL event received",
			zap.String("schema", ddl.GetSchemaName()), zap.String("table", ddl.GetTableName()),
			zap.Uint64("commitTs", ddl.GetCommitTs()), zap.String("query", ddl.Query),
			zap.Any("blockedTables", ddl.GetBlockedTables()))
		needFlush = true
	case common.MessageTypeRow:
		row := progress.decoder.NextDMLEvent()
		if row == nil {
			log.Panic("DML event is nil, it's not expected")
		}

		w.appendRow2Group(row, progress)
	default:
		log.Panic("unknown message type", zap.Any("messageType", messageType))
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

type tableKey struct {
	schema string
	table  string
}

type partitionTableAccessor struct {
	memo map[tableKey]struct{}
}

func (w *writer) onDDL(ddl *commonEvent.DDLEvent) {
	switch w.protocol {
	case config.ProtocolCanalJSON, config.ProtocolOpen:
	default:
		return
	}
	if ddl.Type != byte(timodel.ActionCreateTable) {
		return
	}
	stmt, err := parser.New().ParseOneStmt(ddl.Query, "", "")
	if err != nil {
		log.Panic("parse ddl query failed", zap.String("query", ddl.Query), zap.Error(err))
	}
	if v, ok := stmt.(*ast.CreateTableStmt); ok && v.Partition != nil {
		w.partitionTableAccessor.add(ddl.GetSchemaName(), ddl.GetTableName())
	}
}

func (w *writer) appendRow2Group(dml *commonEvent.DMLEvent, progress *partitionProgress) {
	var (
		tableID  = dml.GetTableID()
		schema   = dml.TableInfo.GetSchemaName()
		table    = dml.TableInfo.GetTableName()
		commitTs = dml.GetCommitTs()
	)
	group := progress.eventGroups[tableID]
	if group == nil {
		group = NewEventsGroup(progress.partition, tableID)
		progress.eventGroups[tableID] = group
	}
	if commitTs >= group.highWatermark {
		group.Append(dml, false)
		log.Info("DML event append to the group",
			zap.Uint64("commitTs", commitTs), zap.Uint64("highWatermark", group.highWatermark),
			zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
			zap.Stringer("eventType", dml.RowTypes[0]))
		return
	}
	switch w.protocol {
	case config.ProtocolCanalJSON:
		// for partition table, the canal-json message cannot assign physical table id to each dml message,
		// we cannot distinguish whether it's a real fallback event or not, still append it.
		if w.partitionTableAccessor.isPartitionTable(schema, table) {
			log.Warn("DML events fallback, but it's canal-json and partition table, still append it",
				zap.Uint64("commitTs", commitTs), zap.Uint64("highWatermark", group.highWatermark),
				zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
				zap.Stringer("eventType", dml.RowTypes[0]))
			group.Append(dml, true)
			return
		}
		log.Warn("DML event fallback row, since less than the group high watermark, ignore it",
			zap.Uint64("commitTs", commitTs), zap.Uint64("highWatermark", group.highWatermark),
			zap.Any("partitionWatermark", progress.watermark), zap.Any("watermark", progress.watermark),
			zap.String("schema", schema), zap.String("table", table), zap.Int64("tableID", tableID),
			zap.Stringer("eventType", dml.RowTypes[0]),
			zap.Any("protocol", w.protocol), zap.Bool("IsPartition", dml.TableInfo.TableName.IsPartition))
	default:
		log.Panic("unknown protocol", zap.Any("protocol", w.protocol))
	}
}

func newPartitionTableAccessor() *partitionTableAccessor {
	return &partitionTableAccessor{
		memo: make(map[tableKey]struct{}),
	}
}

func (m *partitionTableAccessor) add(schema, table string) {
	key := tableKey{schema: schema, table: table}
	_, ok := m.memo[key]
	if ok {
		return
	}
	m.memo[key] = struct{}{}
	log.Info("add partition table to the accessor", zap.String("schema", schema), zap.String("table", table))
}

func (m *partitionTableAccessor) isPartitionTable(schema, table string) bool {
	key := tableKey{schema: schema, table: table}
	_, ok := m.memo[key]
	return ok
}
