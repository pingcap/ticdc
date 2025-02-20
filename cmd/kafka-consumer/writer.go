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
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper/eventrouter"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

type partitionProgress struct {
	partition       int32
	watermark       uint64
	watermarkOffset kafka.Offset

	eventGroups map[model.TableID]*eventsGroup
	decoder     common.RowEventDecoder
}

func newPartitionProgress(partition int32, decoder common.RowEventDecoder) *partitionProgress {
	return &partitionProgress{
		partition:   partition,
		eventGroups: make(map[model.TableID]*eventsGroup),
		decoder:     decoder,
	}
}

func (p *partitionProgress) updateWatermark(newWatermark uint64, offset kafka.Offset) {
	watermark := p.loadWatermark()
	if newWatermark >= watermark {
		p.watermark = newWatermark
		p.watermarkOffset = offset
		log.Info("watermark received", zap.Int32("partition", p.partition), zap.Any("offset", offset),
			zap.Uint64("watermark", newWatermark))
		return
	}
	if offset > p.watermarkOffset {
		log.Panic("partition resolved ts fallback",
			zap.Int32("partition", p.partition),
			zap.Uint64("newWatermark", newWatermark), zap.Any("offset", offset),
			zap.Uint64("watermark", watermark), zap.Any("watermarkOffset", p.watermarkOffset))
	}
	log.Warn("partition resolved ts fall back, ignore it, since consumer read old offset message",
		zap.Int32("partition", p.partition),
		zap.Uint64("newWatermark", newWatermark), zap.Any("offset", offset),
		zap.Uint64("watermark", watermark), zap.Any("watermarkOffset", p.watermarkOffset))
}

func (p *partitionProgress) loadWatermark() uint64 {
	return p.watermark
}

type writer struct {
	ddlList            []*commonEvent.DDLEvent
	ddlWithMaxCommitTs *commonEvent.DDLEvent

	progresses []*partitionProgress

	eventRouter     *eventrouter.EventRouter
	protocol        config.Protocol
	maxMessageBytes int
	maxBatchSize    int
	mysqlSink       sink.Sink
}

func newWriter(ctx context.Context, o *option) *writer {
	w := &writer{
		protocol:        o.protocol,
		maxMessageBytes: o.maxMessageBytes,
		maxBatchSize:    o.maxBatchSize,
		progresses:      make([]*partitionProgress, o.partitionNum),
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
		decoder, err := codec.NewEventDecoder(ctx, o.codecConfig, db)
		if err != nil {
			log.Panic("cannot create the decoder", zap.Error(err))
		}
		w.progresses[i] = newPartitionProgress(int32(i), decoder)
	}

	eventRouter, err := eventrouter.NewEventRouter(o.sinkConfig, o.protocol, o.topic, "kafka")
	if err != nil {
		log.Panic("initialize the event router failed",
			zap.Any("protocol", o.protocol), zap.Any("topic", o.topic),
			zap.Any("dispatcherRules", o.sinkConfig.DispatchRules), zap.Error(err))
	}
	w.eventRouter = eventRouter
	log.Info("event router created", zap.Any("protocol", o.protocol),
		zap.Any("topic", o.topic), zap.Any("dispatcherRules", o.sinkConfig.DispatchRules))

	changefeedID := commonType.NewChangeFeedIDWithName("kafka-consumer")
	cfg := &config.ChangefeedConfig{
		ChangefeedID: changefeedID,
		SinkURI:      o.downstreamURI,
	}
	mysqlSink, err := sink.NewSink(ctx, cfg, changefeedID)
	if err != nil {
		log.Panic("cannot create the mysql sink", zap.Error(err))
	}
	w.mysqlSink = mysqlSink
	return w
}

func (w *writer) run(ctx context.Context) error {
	return w.mysqlSink.Run(ctx)
}

// append DDL wait to be handled, only consider the constraint among DDLs.
// for DDL a / b received in the order, a.CommitTs < b.CommitTs should be true.
func (w *writer) appendDDL(ddl *commonEvent.DDLEvent, offset kafka.Offset) {
	// DDL CommitTs fallback, just crash it to indicate the bug.
	if w.ddlWithMaxCommitTs != nil && ddl.GetCommitTs() < w.ddlWithMaxCommitTs.GetCommitTs() {
		log.Warn("DDL CommitTs < maxCommitTsDDL.CommitTs",
			zap.Uint64("commitTs", ddl.GetCommitTs()),
			zap.Uint64("maxCommitTs", w.ddlWithMaxCommitTs.GetCommitTs()),
			zap.String("DDL", ddl.Query))
		return
	}

	// A rename tables DDL job contains multiple DDL events with same CommitTs.
	// So to tell if a DDL is redundant or not, we must check the equivalence of
	// the current DDL and the DDL with max CommitTs.
	if ddl == w.ddlWithMaxCommitTs {
		log.Warn("ignore redundant DDL, the DDL is equal to ddlWithMaxCommitTs",
			zap.Uint64("commitTs", ddl.GetCommitTs()), zap.String("DDL", ddl.Query))
		return
	}

	w.ddlList = append(w.ddlList, ddl)
	w.ddlWithMaxCommitTs = ddl
	log.Info("DDL message received", zap.Any("offset", offset),
		zap.Uint64("commitTs", ddl.GetCommitTs()), zap.String("DDL", ddl.Query))
}

func (w *writer) getFrontDDL() *commonEvent.DDLEvent {
	if len(w.ddlList) > 0 {
		return w.ddlList[0]
	}
	return nil
}

func (w *writer) popDDL() {
	if len(w.ddlList) > 0 {
		w.ddlList = w.ddlList[1:]
	}
}

func (w *writer) getMinWatermark() uint64 {
	result := uint64(math.MaxUint64)
	for _, p := range w.progresses {
		watermark := p.loadWatermark()
		if watermark < result {
			result = watermark
		}
	}
	return result
}

// partition progress could be executed at the same time
func (w *writer) forEachPartition(fn func(p *partitionProgress)) {
	var wg sync.WaitGroup
	for _, p := range w.progresses {
		wg.Add(1)
		go func(p *partitionProgress) {
			defer wg.Done()
			fn(p)
		}(p)
	}
	wg.Wait()
}

// Write will synchronously write data downstream
func (w *writer) flush(ctx context.Context, messageType common.MessageType) bool {
	watermark := w.getMinWatermark()
	var todoDDL *commonEvent.DDLEvent
	for {
		todoDDL = w.getFrontDDL()
		// watermark is the min value for all partitions,
		// the DDL only executed by the first partition, other partitions may be slow
		// so that the watermark can be smaller than the DDL's commitTs,
		// which means some DML events may not be consumed yet, so cannot execute the DDL right now.
		if todoDDL == nil || todoDDL.GetCommitTs() > watermark {
			break
		}
		// flush DMLs
		//w.forEachPartition(func(sink *partitionProgress) {
		//	syncFlushRowChangedEvents(ctx, sink, todoDDL.CommitTs)
		//})
		// DDL can be executed, do it first.
		if err := w.mysqlSink.WriteBlockEvent(todoDDL); err != nil {
			log.Panic("write DDL event failed", zap.Error(err),
				zap.String("DDL", todoDDL.Query), zap.Uint64("commitTs", todoDDL.GetCommitTs()))
		}
		w.popDDL()
	}

	//if messageType == common.MessageTypeResolved {
	//	w.forEachPartition(func(sink *partitionProgress) {
	//		syncFlushRowChangedEvents(ctx, sink, watermark)
	//	})
	//}

	// The DDL events will only execute in partition0
	if messageType == common.MessageTypeDDL && todoDDL != nil {
		log.Info("DDL event will be flushed in the future",
			zap.Uint64("watermark", watermark),
			zap.Uint64("CommitTs", todoDDL.GetCommitTs()),
			zap.String("Query", todoDDL.Query))
		return false
	}
	return true
}

// WriteMessage is to decode kafka message to event.
func (w *writer) WriteMessage(ctx context.Context, message *kafka.Message) bool {
	var (
		key       = message.Key
		value     = message.Value
		partition = message.TopicPartition.Partition
		offset    = message.TopicPartition.Offset
	)

	progress := w.progresses[partition]
	if err := progress.decoder.AddKeyValue(key, value); err != nil {
		log.Panic("add key value to the decoder failed",
			zap.Int32("partition", partition), zap.Any("offset", offset), zap.Error(err))
	}
	var (
		counter     int
		needFlush   bool
		messageType common.MessageType
	)
	for {
		ty, hasNext, err := progress.decoder.HasNext()
		if err != nil {
			log.Panic("decode message key failed",
				zap.Int32("partition", partition), zap.Any("offset", offset), zap.Error(err))
		}
		if !hasNext {
			break
		}
		counter++
		// If the message containing only one event exceeds the length limit, CDC will allow it and issue a warning.
		if len(key)+len(value) > w.maxMessageBytes && counter > 1 {
			log.Panic("kafka max-messages-bytes exceeded",
				zap.Int32("partition", partition), zap.Any("offset", offset),
				zap.Int("max-message-bytes", w.maxMessageBytes),
				zap.Int("receivedBytes", len(key)+len(value)))
		}
		messageType = ty
		switch messageType {
		case common.MessageTypeDDL:
			// for some protocol, DDL would be dispatched to all partitions,
			// Consider that DDL a, b, c received from partition-0, the latest DDL is c,
			// if we receive `a` from partition-1, which would be seemed as DDL regression,
			// then cause the consumer panic, but it was a duplicate one.
			// so we only handle DDL received from partition-0 should be enough.
			// but all DDL event messages should be consumed.
			ddl, err := progress.decoder.NextDDLEvent()
			if err != nil {
				log.Panic("decode message value failed",
					zap.Int32("partition", partition), zap.Any("offset", offset),
					zap.ByteString("value", value), zap.Error(err))
			}

			// todo: enable this logic, after simple decoder is supported.
			//if dec, ok := progress.decoder.(*simple.Decoder); ok {
			//	cachedEvents := dec.GetCachedEvents()
			//	for _, row := range cachedEvents {
			//		w.checkPartition(row, partition, message.TopicPartition.Offset)
			//		log.Info("simple protocol cached event resolved, append to the group",
			//			zap.Int64("tableID", row.GetTableID()), zap.Uint64("commitTs", row.CommitTs),
			//			zap.Int32("partition", partition), zap.Any("offset", offset))
			//		w.appendRow2Group(row, progress, offset)
			//	}
			//}

			// the Query maybe empty if using simple protocol, it's comes from `bootstrap` event, no need to handle it.
			if ddl.Query == "" {
				continue
			}

			if partition == 0 {
				w.appendDDL(ddl, offset)
			}
			needFlush = true
		case common.MessageTypeRow:
			row, err := progress.decoder.NextRowChangedEvent()
			if err != nil {
				log.Panic("decode message value failed",
					zap.Int32("partition", partition), zap.Any("offset", offset),
					zap.ByteString("value", value),
					zap.Error(err))
			}
			// when using simple protocol, the row may be nil, since it's table info not received yet,
			// it's cached in the decoder, so just continue here.
			if w.protocol == config.ProtocolSimple && row == nil {
				continue
			}
			// w.checkPartition(row, partition, message.TopicPartition.Offset)
			w.appendRow2Group(row, progress, offset)
		case common.MessageTypeResolved:
			newWatermark, err := progress.decoder.NextResolvedEvent()
			if err != nil {
				log.Panic("decode message value failed",
					zap.Int32("partition", partition), zap.Any("offset", offset),
					zap.ByteString("value", value), zap.Error(err))
			}

			progress.updateWatermark(newWatermark, offset)
			w.resolveRowChangedEvents(progress, newWatermark)
			needFlush = true
		default:
			log.Panic("unknown message type", zap.Any("messageType", messageType),
				zap.Int32("partition", partition), zap.Any("offset", offset))
		}
	}

	if counter > w.maxBatchSize {
		log.Panic("Open Protocol max-batch-size exceeded",
			zap.Int("maxBatchSize", w.maxBatchSize), zap.Int("actualBatchSize", counter),
			zap.Int32("partition", partition), zap.Any("offset", offset))
	}

	if !needFlush {
		return false
	}
	// flush when received DDL event or resolvedTs
	return w.flush(ctx, messageType)
}

func (w *writer) resolveRowChangedEvents(progress *partitionProgress, newWatermark uint64) {
	for _, group := range progress.eventGroups {
		events := group.Resolve(newWatermark)
		if len(events) == 0 {
			continue
		}
		for _, e := range events {
			w.mysqlSink.AddDMLEvent(e)
		}
	}
}

//func (w *writer) checkPartition(row *model.RowChangedEvent, partition int32, offset kafka.Offset) {
//	partitioner := w.eventRouter.GetPartitionGenerator(row.TableInfo.GetSchemaName(), row.TableInfo.GetSchemaName())
//
//	partitioner.GeneratePartitionIndexAndKey()
//
//	if partition != target {
//		log.Panic("RowChangedEvent dispatched to wrong partition",
//			zap.Int32("partition", partition), zap.Int32("expected", target),
//			zap.Int32("partitionNum", w.option.partitionNum), zap.Any("offset", offset),
//			zap.Int64("tableID", row.GetTableID()), zap.Any("row", row),
//		)
//	}
//}

func (w *writer) appendRow2Group(row *commonEvent.DMLEvent, progress *partitionProgress, offset kafka.Offset) {
	// if the kafka cluster is normal, this should not hit.
	// else if the cluster is abnormal, the consumer may consume old message, then cause the watermark fallback.
	watermark := progress.loadWatermark()
	partition := progress.partition

	tableID := row.GetTableID()
	group := progress.eventGroups[tableID]
	if group == nil {
		group = NewEventsGroup(partition, tableID)
		progress.eventGroups[tableID] = group
	}
	if row.CommitTs < watermark {
		log.Warn("RowChanged Event fallback row, since les than the partition watermark, ignore it",
			zap.Int64("tableID", tableID), zap.Int32("partition", partition),
			zap.Uint64("commitTs", row.CommitTs), zap.Any("offset", offset),
			zap.Uint64("watermark", watermark), zap.Any("watermarkOffset", progress.watermarkOffset),
			zap.String("schema", row.TableInfo.GetSchemaName()), zap.String("table", row.TableInfo.GetTableName()),
			// zap.Any("columns", row.Columns), zap.Any("preColumns", row.PreColumns),
			zap.String("protocol", w.protocol.String()), zap.Bool("IsPartition", row.TableInfo.TableName.IsPartition))
		return
	}
	if row.CommitTs >= group.highWatermark {
		group.Append(row, offset)
		return
	}
	switch w.protocol {
	case config.ProtocolSimple, config.ProtocolOpen, config.ProtocolCanalJSON:
		// simple protocol set the table id for all row message, it can be known which table the row message belongs to,
		// also consider the table partition.
		// open protocol set the partition table id if the table is partitioned.
		// for normal table, the table id is generated by the fake table id generator by using schema and table name.
		// so one event group for one normal table or one table partition, replayed messages can be ignored.
		log.Warn("RowChangedEvent fallback row, since less than the group high watermark, ignore it",
			zap.Int64("tableID", tableID), zap.Int32("partition", partition),
			zap.Uint64("commitTs", row.CommitTs), zap.Any("offset", offset),
			zap.Uint64("highWatermark", group.highWatermark),
			zap.Any("partitionWatermark", watermark), zap.Any("watermarkOffset", progress.watermarkOffset),
			zap.String("schema", row.TableInfo.GetSchemaName()), zap.String("table", row.TableInfo.GetTableName()),
			// zap.Any("columns", row.Columns), zap.Any("preColumns", row.PreColumns),
			zap.String("protocol", w.protocol.String()), zap.Bool("IsPartition", row.TableInfo.TableName.IsPartition))
		return
	default:
	}
	log.Warn("RowChangedEvent fallback row, since less than the group high watermark, do not ignore it",
		zap.Int64("tableID", tableID), zap.Int32("partition", partition),
		zap.Uint64("commitTs", row.CommitTs), zap.Any("offset", offset),
		zap.Uint64("highWatermark", group.highWatermark),
		zap.Any("partitionWatermark", watermark), zap.Any("watermarkOffset", progress.watermarkOffset),
		zap.String("schema", row.TableInfo.GetSchemaName()), zap.String("table", row.TableInfo.GetTableName()),
		// zap.Any("columns", row.Columns), zap.Any("preColumns", row.PreColumns),
		zap.String("protocol", w.protocol.String()))
	group.Append(row, offset)
}

//func syncFlushRowChangedEvents(ctx context.Context, progress *partitionProgress, watermark uint64) {
//	resolvedTs := model.NewResolvedTs(watermark)
//	for {
//		select {
//		case <-ctx.Done():
//			log.Warn("sync flush row changed event canceled", zap.Error(ctx.Err()))
//			return
//		default:
//		}
//		flushedResolvedTs := true
//		for _, tableSink := range progress.tableSinkMap {
//			if err := tableSink.UpdateResolvedTs(resolvedTs); err != nil {
//				log.Panic("Failed to update resolved ts", zap.Error(err))
//			}
//			if tableSink.GetCheckpointTs().Less(resolvedTs) {
//				flushedResolvedTs = false
//			}
//		}
//		if flushedResolvedTs {
//			return
//		}
//	}
//}

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
