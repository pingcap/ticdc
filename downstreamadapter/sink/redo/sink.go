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

package redo

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/redo/writer/factory"
	sinkutil "github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/chann"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Sink manages redo log writer, buffers un-persistent redo logs, calculates
// redo log resolved ts. It implements Sink interface.
type Sink struct {
	ctx          context.Context
	changefeedID common.ChangeFeedID
	ddlWriter    writer.RedoDDLWriter
	dmlWriter    writer.RedoDMLWriter

	logBuffer *chann.UnlimitedChannel[*commonEvent.RedoRowEvent, any]

	// router is used for schema/table name routing in DDL queries.
	// When routing is configured, DDL queries are rewritten to use target table names
	// before being written to the redo log.
	router *sinkutil.Router

	// isNormal indicate whether the sink is in the normal state.
	isNormal *atomic.Bool
	isClosed *atomic.Bool

	metricCollector *metricCollector
}

func Verify(ctx context.Context, changefeedID common.ChangeFeedID, cfg *config.ConsistentConfig) error {
	if cfg == nil || !redo.IsConsistentEnabled(util.GetOrZero(cfg.Level)) {
		return nil
	}
	return nil
}

// New creates a new redo sink.
// The router parameter is used for DDL query rewriting when sink routing is configured.
func New(ctx context.Context, changefeedID common.ChangeFeedID,
	cfg *config.ConsistentConfig,
	router *sinkutil.Router,
) (*Sink, error) {
	var err error
	config, err := writer.NewConfig(changefeedID, cfg)
	if err != nil {
		return nil, err
	}
	s := &Sink{
		ctx:          ctx,
		changefeedID: changefeedID,
		logBuffer:    chann.NewUnlimitedChannelDefault[*commonEvent.RedoRowEvent](),
		isNormal:     atomic.NewBool(true),
		isClosed:     atomic.NewBool(false),
	}

	var (
		start     = time.Now()
		ddlWriter writer.RedoDDLWriter
		dmlWriter writer.RedoDMLWriter
	)
	defer func() {
		if err == nil {
			return
		}
		if ddlWriter != nil {
			ddlWriter.Close()
		}
		if dmlWriter != nil {
			dmlWriter.Close()
		}
	}()

	ddlWriter, err = factory.NewRedoDDLWriter(ctx, config)
	if err != nil {
		log.Error("redo: failed to create redo log writer",
			zap.String("keyspace", changefeedID.Keyspace()),
			zap.String("changefeed", changefeedID.Name()),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return nil, err
	}
	dmlWriter, err = factory.NewRedoDMLWriter(ctx, config)
	if err != nil {
		log.Error("redo: failed to create redo log writer",
			zap.String("keyspace", changefeedID.Keyspace()),
			zap.String("changefeed", changefeedID.Name()),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return nil, err
	}
	s.ddlWriter = ddlWriter
	s.dmlWriter = dmlWriter
	s.metricCollector = newMetricCollector(changefeedID)
	return s, nil
}

func (s *Sink) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer s.logBuffer.Close()
		return s.dmlWriter.Run(ctx)
	})
	g.Go(func() error {
		return s.sendMessages(ctx)
	})
	err := g.Wait()
	s.isNormal.Store(false)
	return err
}

func (s *Sink) FlushDMLBeforeBlock(_ commonEvent.BlockEvent) error {
	return nil
}

func (s *Sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch e := event.(type) {
	case *commonEvent.DDLEvent:
		start := time.Now()
		// Rewrite DDL query with routing before writing to redo log.
		// This ensures the redo log contains already-routed DDL statements,
		// so replay will write to the correct target tables.
		if s.router != nil {
			if err := s.rewriteDDLQueryWithRouting(e); err != nil {
				s.isNormal.Store(false)
				return errors.Trace(err)
			}
		}
		err := s.ddlWriter.WriteDDLEvent(s.ctx, e)
		if err != nil {
			s.isNormal.Store(false)
			return err
		}
		if s.metricCollector != nil {
			s.metricCollector.observeDDLWrite(time.Since(start))
		}
		log.Info("redo sink send DDL event",
			zap.String("keyspace", s.changefeedID.Keyspace()), zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("event", e.GetDDLQuery()), zap.Any("startTs", event.GetStartTs()), zap.Any("commitTs", event.GetCommitTs()),
			zap.String("schema", e.GetSchemaName()), zap.String("table", e.GetTableName()), zap.Int64("tableID", e.GetTableID()))
	}
	return nil
}

func (s *Sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	rowsCount := event.Len()
	events := make([]*commonEvent.RedoRowEvent, 0, rowsCount)
	rowCallback := helper.NewTxnPostFlushRowCallback(event, uint64(rowsCount))

	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}
		events = append(events, &commonEvent.RedoRowEvent{
			StartTs:         event.StartTs,
			CommitTs:        event.CommitTs,
			Event:           row,
			PhysicalTableID: event.PhysicalTableID,
			TableInfo:       event.TableInfo,
			Callback:        rowCallback,
		})
	}
	s.logBuffer.Push(events...)
}

func (s *Sink) IsNormal() bool {
	return s.isNormal.Load()
}

func (s *Sink) SinkType() common.SinkType {
	return common.RedoSinkType
}

// GetRouter returns the router for schema/table name routing.
func (s *Sink) GetRouter() *sinkutil.Router {
	return s.router
}

func (s *Sink) SetTableSchemaStore(tableSchemaStore *commonEvent.TableSchemaStore) {
	s.ddlWriter.SetTableSchemaStore(tableSchemaStore)
}

func (s *Sink) Close(_ bool) {
	if !s.isClosed.CompareAndSwap(false, true) {
		return
	}
	start := time.Now()
	s.logBuffer.Close()
	if s.ddlWriter != nil {
		if err := s.ddlWriter.Close(); err != nil && errors.Cause(err) != context.Canceled {
			log.Error("redo sink fails to close ddl writer",
				zap.String("keyspace", s.changefeedID.Keyspace()),
				zap.String("changefeed", s.changefeedID.Name()),
				zap.Error(err))
		}
	}
	if s.dmlWriter != nil {
		if err := s.dmlWriter.Close(); err != nil && errors.Cause(err) != context.Canceled {
			log.Error("redo sink fails to close dml writer",
				zap.String("keyspace", s.changefeedID.Keyspace()),
				zap.String("changefeed", s.changefeedID.Name()),
				zap.Error(err))
		}
	}
	if s.metricCollector != nil {
		s.metricCollector.close()
	}
	log.Info("redo sink closed",
		zap.String("keyspace", s.changefeedID.Keyspace()),
		zap.String("changefeed", s.changefeedID.Name()),
		zap.Duration("duration", time.Since(start)))
}

func (s *Sink) sendMessages(ctx context.Context) error {
	buffer := make([]*commonEvent.RedoRowEvent, 0, redo.DefaultFlushBatchSize)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		default:
		}
		events, ok := s.logBuffer.GetMultipleNoGroup(buffer)
		if !ok {
			return nil
		}
		if len(events) == 0 {
			continue
		}
		buffer = events[:0]

		start := time.Now()
		err := s.dmlWriter.AddDMLEvents(ctx, events...)
		if err != nil {
			return err
		}
		if s.metricCollector != nil {
			s.metricCollector.observeRowWrite(len(events), time.Since(start))
		}
	}
}

func (s *Sink) AddCheckpointTs(_ uint64) {}

// rewriteDDLQueryWithRouting rewrites the DDL query by applying routing rules
// to transform source table names to target table names.
//
// IMPORTANT: This function mutates ddl.Query and ddl.BlockedTableNames in place. This is safe because:
// 1. This is only called once per DDL event at the start of WriteBlockEvent
// 2. The redo writer's WriteEvents has no internal retries for DDL events
// 3. If WriteBlockEvent fails, the dispatcher reports an error and does not retry with the same event
func (s *Sink) rewriteDDLQueryWithRouting(ddl *commonEvent.DDLEvent) error {
	result, err := sinkutil.RewriteDDLQueryWithRouting(s.router, ddl, s.changefeedID.String())
	if err != nil {
		return errors.Trace(err)
	}

	// Only update the query if it actually changed
	if result.QueryChanged {
		ddl.Query = result.NewQuery
	}
	// Note: TargetSchemaName is not used by redo sink (no USE statement needed for redo logs)

	// Route BlockedTableNames to maintain consistency between the rewritten query and
	// the table names stored in redo logs. This ensures that when redo logs are replayed,
	// the BlockedTableNames match the routed table names in the query.
	s.routeBlockedTableNames(ddl)

	return nil
}

// routeBlockedTableNames applies routing rules to BlockedTableNames so that
// redo logs contain consistent target table names throughout.
func (s *Sink) routeBlockedTableNames(ddl *commonEvent.DDLEvent) {
	if s.router == nil || len(ddl.BlockedTableNames) == 0 {
		return
	}

	for i := range ddl.BlockedTableNames {
		srcSchema := ddl.BlockedTableNames[i].SchemaName
		srcTable := ddl.BlockedTableNames[i].TableName
		targetSchema, targetTable := s.router.Route(srcSchema, srcTable)
		ddl.BlockedTableNames[i].SchemaName = targetSchema
		ddl.BlockedTableNames[i].TableName = targetTable
	}
}
