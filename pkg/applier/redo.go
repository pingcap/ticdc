// Copyright 2021 PingCAP, Inc.
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

package applier

import (
	"context"
	"net/url"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mysql"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/reader"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	applierChangefeed = "redo-applier"
	warnDuration      = 3 * time.Minute
	flushWaitDuration = 200 * time.Millisecond
)

var (
	// In the boundary case, non-idempotent DDLs will not be executed.
	// TODO: fix this
	unsupportedDDL = map[timodel.ActionType]struct{}{
		timodel.ActionExchangeTablePartition: {},
	}
	errApplyFinished = errors.New("apply finished, can exit safely")
)

// RedoApplierConfig is the configuration used by a redo log applier
type RedoApplierConfig struct {
	SinkURI string
	Storage string
	Dir     string
}

// RedoApplier implements a redo log applier
type RedoApplier struct {
	cfg            *RedoApplierConfig
	rd             reader.RedoLogReader
	updateSplitter *updateEventSplitter

	mysqlSink       *mysql.Sink
	appliedDDLCount uint64

	eventsGroup     map[commonType.TableID]*eventsGroup
	appliedLogCount uint64

	errCh chan error

	// changefeedID is used to identify the changefeed that this applier belongs to.
	// not used for now.
	changefeedID commonType.ChangeFeedID
}

// NewRedoApplier creates a new RedoApplier instance
func NewRedoApplier(cfg *RedoApplierConfig) *RedoApplier {
	return &RedoApplier{
		cfg:   cfg,
		errCh: make(chan error, 1024),
	}
}

// toLogReaderConfig is an adapter to translate from applier config to redo reader config
// returns storageType, *reader.toLogReaderConfig and error
func (rac *RedoApplierConfig) toLogReaderConfig() (string, *reader.LogReaderConfig, error) {
	uri, err := url.Parse(rac.Storage)
	if err != nil {
		return "", nil, errors.WrapError(errors.ErrConsistentStorage, err)
	}
	if redo.IsLocalStorage(uri.Scheme) {
		uri.Scheme = "file"
	}
	cfg := &reader.LogReaderConfig{
		URI:                *uri,
		Dir:                rac.Dir,
		UseExternalStorage: redo.IsExternalStorage(uri.Scheme),
	}
	return uri.Scheme, cfg, nil
}

func (ra *RedoApplier) catchError(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-ra.errCh:
			return err
		}
	}
}

func (ra *RedoApplier) initSink(ctx context.Context) (err error) {
	if ra.mysqlSink != nil || ra.eventsGroup != nil {
		log.Warn("redo applier has initialized")
		return nil
	}
	replicaConfig := &config.ChangefeedConfig{
		SinkURI:    ra.cfg.SinkURI,
		SinkConfig: &config.SinkConfig{},
	}
	sink, err := sink.New(ctx, replicaConfig, ra.changefeedID)
	if err != nil {
		return err
	}
	ra.mysqlSink = sink.(*mysql.Sink)

	ra.eventsGroup = make(map[commonType.TableID]*eventsGroup)
	return nil
}

func (ra *RedoApplier) consumeLogs(ctx context.Context) error {
	checkpointTs, resolvedTs, err := ra.rd.ReadMeta(ctx)
	if err != nil {
		return err
	}
	log.Info("apply redo log starts",
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Uint64("resolvedTs", resolvedTs))
	if err := ra.initSink(ctx); err != nil {
		return err
	}
	defer ra.mysqlSink.Close(true)

	shouldApplyDDL := func(row *commonEvent.RedoDMLEvent, ddl *commonEvent.RedoDDLEvent) bool {
		if ddl == nil {
			return false
		} else if row == nil {
			// no more rows to apply
			return true
		}
		// If all rows before the DDL (which means row.CommitTs <= ddl.CommitTs)
		// are applied, we should apply this DDL.
		return row.Row.CommitTs > ddl.DDL.CommitTs
	}

	row, err := ra.updateSplitter.readNextRow(ctx)
	if err != nil {
		return err
	}
	ddl, ok, err := ra.rd.ReadNextDDL(ctx)
	if err != nil || !ok {
		return err
	}
	for {
		if row == nil {
			break
		}
		if shouldApplyDDL(row, ddl) {
			if err := ra.applyDDL(ctx, ddl, checkpointTs); err != nil {
				return err
			}
			if ddl, ok, err = ra.rd.ReadNextDDL(ctx); err != nil {
				return err
			}
		} else {
			if err := ra.applyRow(row, checkpointTs); err != nil {
				return err
			}
			if row, err = ra.updateSplitter.readNextRow(ctx); err != nil {
				return err
			}
		}
	}
	// wait all tables to flush data
	for tableID := range ra.eventsGroup {
		if err := ra.waitTableFlush(ctx, tableID, resolvedTs); err != nil {
			return err
		}
	}

	log.Info("apply redo log finishes",
		zap.Uint64("appliedLogCount", ra.appliedLogCount),
		zap.Uint64("appliedDDLCount", ra.appliedDDLCount),
		zap.Uint64("currentCheckpoint", resolvedTs))
	return errApplyFinished
}

func (ra *RedoApplier) applyDDL(
	ctx context.Context, ddl *commonEvent.RedoDDLEvent, checkpointTs uint64,
) error {
	shouldSkip := func() bool {
		if ddl.DDL == nil {
			// Note this could only happen when using old version of cdc, and the commit ts
			// of the DDL should be equal to checkpoint ts or resolved ts.
			log.Warn("ignore DDL without table info", zap.Any("ddl", ddl))
			return true
		}
		if ddl.DDL.CommitTs == checkpointTs {
			if _, ok := unsupportedDDL[timodel.ActionType(ddl.Type)]; ok {
				log.Error("ignore unsupported DDL", zap.Any("ddl", ddl))
				return true
			}
		}
		newStartTsList, _, err := ra.mysqlSink.GetStartTsList([]int64{ddl.TableName.TableID}, []int64{0}, false)
		if err != nil {
			log.Error("get startTs list failed", zap.Any("ddl", ddl), zap.Error(err))
		}
		if len(newStartTsList) > 0 && newStartTsList[0] > int64(checkpointTs) {
			return true
		}
		return false
	}
	if shouldSkip() {
		return nil
	}
	log.Warn("apply DDL", zap.Any("ddl", ddl))
	// Wait all tables to flush data before applying DDL.
	// TODO: only block tables that are affected by this DDL.
	for tableID := range ra.eventsGroup {
		if err := ra.waitTableFlush(ctx, tableID, ddl.DDL.CommitTs); err != nil {
			return err
		}
	}
	if err := ra.mysqlSink.WriteBlockEvent(ddl.ToDDLEvent()); err != nil {
		return err
	}
	ra.appliedDDLCount++
	return nil
}

func (ra *RedoApplier) applyRow(
	row *commonEvent.RedoDMLEvent, checkpointTs commonType.Ts,
) error {
	tableID := row.Row.Table.TableID
	if _, ok := ra.eventsGroup[tableID]; !ok {
		ra.eventsGroup[tableID] = NewEventsGroup(0, tableID)
	}

	if row.Row.CommitTs < ra.eventsGroup[tableID].highWatermark {
		log.Panic("commit ts of redo log regressed",
			zap.Int64("tableID", tableID),
			zap.Uint64("commitTs", row.Row.CommitTs),
			zap.Any("resolvedTs", ra.eventsGroup[tableID].highWatermark))
	}
	ra.eventsGroup[tableID].Append(row.ToDMLEvent(), false)

	ra.appliedLogCount++
	return nil
}

func (ra *RedoApplier) waitTableFlush(
	ctx context.Context, tableID commonType.TableID, rts commonType.Ts,
) error {
	if ra.eventsGroup[tableID].highWatermark > rts {
		log.Panic("resolved ts of redo log regressed",
			zap.Any("oldResolvedTs", ra.eventsGroup[tableID].highWatermark),
			zap.Any("newResolvedTs", rts))
	}

	var flushed atomic.Int64

	events := ra.eventsGroup[tableID].Resolve(rts)
	total := len(events)
	done := make(chan struct{}, 1)
	for _, e := range events {
		e.AddPostFlushFunc(func() {
			if flushed.Inc() == int64(total) {
				close(done)
			}
		})
		ra.mysqlSink.AddDMLEvent(e)
	}
	// Make sure all events are flushed to downstream.
	start := time.Now()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-done:
		log.Info("flush DML events done", zap.Uint64("resolvedTs", rts),
			zap.Int("total", total), zap.Duration("duration", time.Since(start)))
	case <-ticker.C:
		log.Panic("DML events cannot be flushed in 1 minute", zap.Uint64("resolvedTs", rts),
			zap.Int("total", total), zap.Int64("flushed", flushed.Load()))
	}

	return nil
}

var createRedoReader = createRedoReaderImpl

func createRedoReaderImpl(ctx context.Context, cfg *RedoApplierConfig) (reader.RedoLogReader, error) {
	storageType, readerCfg, err := cfg.toLogReaderConfig()
	if err != nil {
		return nil, err
	}
	return reader.NewRedoLogReader(ctx, storageType, readerCfg)
}

// processEvent return (event to emit, pending event)
func processEvent(
	event *commonEvent.RedoDMLEvent,
	prevTxnStartTs commonType.Ts,
	tempStorage *tempTxnInsertEventStorage,
) (*commonEvent.RedoDMLEvent, *commonEvent.RedoDMLEvent, error) {
	if event == nil {
		log.Panic("event should not be nil")
	}

	// meet a new transaction
	if prevTxnStartTs != 0 && prevTxnStartTs != event.Row.StartTs {
		if tempStorage.hasEvent() {
			// emit the insert events in the previous transaction
			return nil, event, nil
		}
	}
	if event.IsDelete() {
		return event, nil, nil
	} else if event.IsInsert() {
		if tempStorage.hasEvent() {
			// pend current event and emit the insert events in temp storage first to release memory
			return nil, event, nil
		}
		return event, nil, nil
	} else if !shouldSplitUpdateEvent(event) {
		return event, nil, nil
	} else {
		deleteEvent, insertEvent, err := splitUpdateEvent(event)
		if err != nil {
			return nil, nil, err
		}
		err = tempStorage.addEvent(insertEvent)
		if err != nil {
			return nil, nil, err
		}
		return deleteEvent, nil, nil
	}
}

// ReadMeta creates a new redo applier and read meta from reader
func (ra *RedoApplier) ReadMeta(ctx context.Context) (checkpointTs uint64, resolvedTs uint64, err error) {
	rd, err := createRedoReader(ctx, ra.cfg)
	if err != nil {
		return 0, 0, err
	}
	return rd.ReadMeta(ctx)
}

// Apply applies redo log to given target
func (ra *RedoApplier) Apply(egCtx context.Context) (err error) {
	eg, egCtx := errgroup.WithContext(egCtx)

	if ra.rd, err = createRedoReader(egCtx, ra.cfg); err != nil {
		return err
	}
	eg.Go(func() error {
		return ra.rd.Run(egCtx)
	})
	ra.updateSplitter = newUpdateEventSplitter(ra.rd, ra.cfg.Dir)

	eg.Go(func() error {
		return ra.consumeLogs(egCtx)
	})
	eg.Go(func() error {
		return ra.catchError(egCtx)
	})

	err = eg.Wait()
	if errors.Cause(err) != errApplyFinished {
		return err
	}
	return nil
}
