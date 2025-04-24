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
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/simple"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/atomic"
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
	if newWatermark >= p.watermark {
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
			zap.Uint64("watermark", p.watermark), zap.Any("watermarkOffset", p.watermarkOffset))
	}
	log.Warn("partition resolved ts fall back, ignore it, since consumer read old offset message",
		zap.Int32("partition", p.partition),
		zap.Uint64("newWatermark", newWatermark), zap.Any("offset", offset),
		zap.Uint64("watermark", p.watermark), zap.Any("watermarkOffset", p.watermarkOffset))
}

type writer struct {
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
	// The DDL event is delivered after all messages belongs to the tables which are blocked by the DDL event
	// so we can make assumption that the all DMLs received before the DDL event.
	// since one table's events may be produced to the different partitions, so we have to flush all partitions.
	var (
		done = make(chan struct{}, 1)

		totalCount   int
		flushedCount atomic.Int64
	)
	for _, tableID := range ddl.GetBlockedTables().TableIDs {
		for _, progress := range w.progresses {
			events := progress.eventGroups[tableID].Resolve(progress.watermark)
			totalCount += len(events)
			for _, e := range events {
				e.AddPostFlushFunc(func() {
					flushedCount.Inc()
					if int(flushedCount.Load()) == totalCount {
						close(done)
					}
				})
			}
			event := mergeDMLEvent(events)
			w.mysqlSink.AddDMLEvent(event)
		}
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-done:
	case <-ticker.C:
		log.Panic("DDL event timeout, since the DML events are not flushed in time",
			zap.Int("count", totalCount), zap.Int64("flushed", flushedCount.Load()),
			zap.String("query", ddl.Query))
	}
	return w.mysqlSink.WriteBlockEvent(ddl)
}

func mergeDMLEvent(events []*commonEvent.DMLEvent) *commonEvent.DMLEvent {
	if len(events) == 0 {
		log.Panic("DMLEvent: empty events")
	}
	event := events[0]
	for _, e := range events[1:] {
		event.Rows.Append(e.Rows, 0, e.Rows.NumRows())
		event.RowTypes = append(event.RowTypes, e.RowTypes...)
		event.Length += e.Length
		event.PostTxnFlushed = append(event.PostTxnFlushed, e.PostTxnFlushed...)
		event.ApproximateSize += e.ApproximateSize
	}
	return event
}

func (w *writer) flushDMLEventsByWatermark(ctx context.Context, progress *partitionProgress) error {
	var (
		done = make(chan struct{}, 1)

		totalCount   int
		flushedCount atomic.Int64
	)
	for _, group := range progress.eventGroups {
		events := group.Resolve(progress.watermark)
		totalCount += len(events)
		for _, e := range events {
			e.AddPostFlushFunc(func() {
				flushedCount.Inc()
				if int(flushedCount.Load()) == totalCount {
					close(done)
				}
			})
		}
		e := mergeDMLEvent(events)
		w.mysqlSink.AddDMLEvent(e)
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-done:
	case <-ticker.C:
		log.Panic("DDL event timeout, since the DML events are not flushed in time",
			zap.Int("count", totalCount), zap.Int64("flushed", flushedCount.Load()))
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

	// If the message containing only one event exceeds the length limit, CDC will allow it and issue a warning.
	if len(message.Key)+len(message.Value) > w.maxMessageBytes {
		log.Panic("kafka max-messages-bytes exceeded",
			zap.Int32("partition", partition), zap.Any("offset", offset),
			zap.Int("max-message-bytes", w.maxMessageBytes),
			zap.Int("receivedBytes", len(message.Key)+len(message.Value)))
	}

	progress := w.progresses[partition]
	progress.decoder.AddKeyValue(message.Key, message.Value)

	messageType, hasNext := progress.decoder.HasNext()
	if !hasNext {
		log.Panic("try to fetch the next event failed, this should not happen", zap.Bool("hasNext", hasNext))
	}

	switch messageType {
	case common.MessageTypeResolved:
		newWatermark := progress.decoder.NextResolvedEvent()
		progress.updateWatermark(newWatermark, offset)

		// since watermark is broadcast to all partitions, so that each partition can flush events individually.
		err := w.flushDMLEventsByWatermark(ctx, progress)
		if err != nil {
			log.Panic("flush dml events by the watermark failed", zap.Error(err))
		}
		return true
	case common.MessageTypeDDL:
		// for some protocol, DDL would be dispatched to all partitions,
		// Consider that DDL a, b, c received from partition-0, the latest DDL is c,
		// if we receive `a` from partition-1, which would be seemed as DDL regression,
		// then cause the consumer panic, but it was a duplicate one.
		// so we only handle DDL received from partition-0 should be enough.
		// but all DDL event messages should be consumed.
		ddl := progress.decoder.NextDDLEvent()

		// todo: enable this logic, after simple decoder is supported.
		if dec, ok := progress.decoder.(*simple.Decoder); ok {
			cachedEvents := dec.GetCachedEvents()
			for _, row := range cachedEvents {
				log.Info("simple protocol cached event resolved, append to the group",
					zap.Int64("tableID", row.GetTableID()), zap.Uint64("commitTs", row.CommitTs),
					zap.Int32("partition", partition), zap.Any("offset", offset))
				w.appendRow2Group(row, progress, offset)
			}
		}

		// the Query maybe empty if using simple protocol, it's comes from `bootstrap` event, no need to handle it.
		if ddl.Query == "" {
			return false
		}

		if partition != 0 {
			return false
		}

		err := w.flushDDLEvent(ctx, ddl)
		if err != nil {
			log.Panic("flush DDL event failed", zap.String("DDL", ddl.Query), zap.Error(err))
		}
		return true
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
		for _, hasNext = progress.decoder.HasNext(); hasNext; {
			row = progress.decoder.NextDMLEvent()
			if row != nil {
				w.appendRow2Group(row, progress, offset)
			}
			counter++
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
	return false
}

func (w *writer) checkPartition(row *commonEvent.DMLEvent, partition int32, offset kafka.Offset) {
	var (
		partitioner  = w.eventRouter.GetPartitionGenerator(row.TableInfo.GetSchemaName(), row.TableInfo.GetSchemaName())
		partitionNum = int32(len(w.progresses))
	)
	for {
		change, ok := row.GetNextRow()
		if !ok {
			break
		}

		target, _, err := partitioner.GeneratePartitionIndexAndKey(&change, partitionNum, row.TableInfo, row.GetCommitTs())
		if err != nil {
			log.Panic("generate partition index and key failed", zap.Error(err))
		}

		if partition != target {
			log.Panic("RowChangedEvent dispatched to wrong partition",
				zap.Int32("partition", partition), zap.Int32("expected", target),
				zap.Int("partitionNum", len(w.progresses)), zap.Any("offset", offset),
				zap.Int64("tableID", row.GetTableID()), zap.Any("row", row),
			)
		}
	}
	row.Rewind()
}

func (w *writer) appendRow2Group(dml *commonEvent.DMLEvent, progress *partitionProgress, offset kafka.Offset) {
	w.checkPartition(dml, progress.partition, offset)
	// if the kafka cluster is normal, this should not hit.
	// else if the cluster is abnormal, the consumer may consume old message, then cause the watermark fallback.
	tableID := dml.GetTableID()
	group := progress.eventGroups[tableID]
	if group == nil {
		group = NewEventsGroup(progress.partition, tableID)
		progress.eventGroups[tableID] = group
	}
	commitTs := dml.GetCommitTs()
	if commitTs < progress.watermark {
		log.Warn("DML Event fallback row, since less than the partition watermark, ignore it",
			zap.Int64("tableID", tableID), zap.Int32("partition", progress.partition),
			zap.Uint64("commitTs", commitTs), zap.Any("offset", offset),
			zap.Uint64("watermark", progress.watermark), zap.Any("watermarkOffset", progress.watermarkOffset),
			zap.String("schema", dml.TableInfo.GetSchemaName()), zap.String("table", dml.TableInfo.GetTableName()),
			// zap.Any("columns", row.Columns), zap.Any("preColumns", row.PreColumns),
			zap.Any("protocol", w.protocol), zap.Bool("IsPartition", dml.TableInfo.TableName.IsPartition))
		return
	}
	if commitTs >= group.highWatermark {
		group.Append(dml, offset)
		return
	}
	switch w.protocol {
	case config.ProtocolSimple, config.ProtocolOpen, config.ProtocolCanalJSON, config.ProtocolDebezium:
		// simple protocol set the table id for all row message, it can be known which table the row message belongs to,
		// also consider the table partition.
		// open protocol set the partition table id if the table is partitioned.
		// for normal table, the table id is generated by the fake table id generator by using schema and table name.
		// so one event group for one normal table or one table partition, replayed messages can be ignored.
		log.Warn("DML event fallback row, since less than the group high watermark, ignore it",
			zap.Int64("tableID", tableID), zap.Int32("partition", progress.partition),
			zap.Uint64("commitTs", commitTs), zap.Any("offset", offset),
			zap.Uint64("highWatermark", group.highWatermark),
			zap.Any("partitionWatermark", progress.watermark), zap.Any("watermarkOffset", progress.watermarkOffset),
			zap.String("schema", dml.TableInfo.GetSchemaName()), zap.String("table", dml.TableInfo.GetTableName()),
			// zap.Any("columns", row.Columns), zap.Any("preColumns", row.PreColumns),
			zap.Any("protocol", w.protocol), zap.Bool("IsPartition", dml.TableInfo.TableName.IsPartition))
		return
	default:
	}
	log.Warn("DML event fallback row, since less than the group high watermark, do not ignore it",
		zap.Int64("tableID", tableID), zap.Int32("partition", progress.partition),
		zap.Uint64("commitTs", commitTs), zap.Any("offset", offset),
		zap.Uint64("highWatermark", group.highWatermark),
		zap.Any("partitionWatermark", progress.watermark), zap.Any("watermarkOffset", progress.watermarkOffset),
		zap.String("schema", dml.TableInfo.GetSchemaName()), zap.String("table", dml.TableInfo.GetTableName()),
		// zap.Any("columns", row.Columns), zap.Any("preColumns", row.PreColumns),
		zap.Any("protocol", w.protocol))
	group.Append(dml, offset)
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
