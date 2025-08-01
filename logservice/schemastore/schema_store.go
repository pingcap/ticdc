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

package schemastore

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type SchemaStore interface {
	common.SubModule

	GetAllPhysicalTables(snapTs uint64, filter filter.Filter) ([]commonEvent.Table, error)

	RegisterTable(tableID int64, startTs uint64) error

	UnregisterTable(tableID int64) error

	// GetTableInfo return table info with the largest version <= ts
	GetTableInfo(tableID int64, ts uint64) (*common.TableInfo, error)

	// TODO: how to respect tableFilter
	GetTableDDLEventState(tableID int64) DDLEventState

	// FetchTableDDLEvents returns the next ddl events which finishedTs are within the range (start, end]
	// The caller must ensure end <= current resolvedTs
	// TODO: add a parameter limit
	FetchTableDDLEvents(dispatcherID common.DispatcherID, tableID int64, tableFilter filter.Filter, start, end uint64) ([]commonEvent.DDLEvent, error)

	FetchTableTriggerDDLEvents(tableFilter filter.Filter, start uint64, limit int) ([]commonEvent.DDLEvent, uint64, error)
}

type DDLEventState struct {
	ResolvedTs       uint64
	MaxEventCommitTs uint64
}

type schemaStore struct {
	pdClock pdutil.Clock

	ddlJobFetcher *ddlJobFetcher

	// store unresolved ddl event in memory, it is thread safe
	unsortedCache *ddlCache

	// store ddl event and other metadata on disk, it is thread safe
	dataStorage *persistentStorage

	notifyCh chan interface{}

	// pendingResolvedTs is the largest resolvedTs the pending ddl events
	pendingResolvedTs atomic.Uint64
	// resolvedTs is the largest resolvedTs of all applied ddl events
	// Invariant: resolvedTs >= pendingResolvedTs
	resolvedTs atomic.Uint64

	// the following two fields are used to filter out duplicate ddl events
	// they will just be updated and read by a single goroutine, so no lock is needed

	// max finishedTs of all applied ddl events
	finishedDDLTs uint64
	// max schemaVersion of all applied ddl events
	schemaVersion int64
}

func New(
	ctx context.Context,
	root string,
	subClient logpuller.SubscriptionClient,
	pdCli pd.Client,
	kvStorage kv.Storage,
) SchemaStore {
	dataStorage := newPersistentStorage(ctx, root, pdCli, kvStorage)
	s := &schemaStore{
		pdClock:       appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		unsortedCache: newDDLCache(),
		dataStorage:   dataStorage,
		notifyCh:      make(chan interface{}, 4),
	}
	s.ddlJobFetcher = newDDLJobFetcher(
		ctx,
		subClient,
		kvStorage,
		s.writeDDLEvent,
		s.advancePendingResolvedTs)
	return s
}

func (s *schemaStore) Name() string {
	return appcontext.SchemaStore
}

func (s *schemaStore) initialize(ctx context.Context) {
	s.dataStorage.initialize(ctx)

	upperBound := s.dataStorage.getUpperBound()
	s.finishedDDLTs = upperBound.FinishedDDLTs
	s.schemaVersion = upperBound.SchemaVersion
	s.pendingResolvedTs.Store(upperBound.ResolvedTs)
	s.resolvedTs.Store(upperBound.ResolvedTs)
	s.ddlJobFetcher.run(upperBound.ResolvedTs)
	log.Info("schema store initialized",
		zap.Uint64("resolvedTs", s.resolvedTs.Load()),
		zap.Uint64("finishedDDLTS", s.finishedDDLTs),
		zap.Int64("schemaVersion", s.schemaVersion))
}

func (s *schemaStore) Run(ctx context.Context) error {
	log.Info("schema store begin to run")
	defer func() {
		log.Info("schema store exited")
	}()

	s.initialize(ctx)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return s.updateResolvedTsPeriodically(ctx)
	})

	eg.Go(func() error {
		return s.dataStorage.gc(ctx)
	})

	eg.Go(func() error {
		return s.dataStorage.persistUpperBoundPeriodically(ctx)
	})

	return eg.Wait()
}

func (s *schemaStore) Close(ctx context.Context) error {
	log.Info("schema store start to close")
	defer log.Info("schema store closed")
	return s.dataStorage.close()
}

func (s *schemaStore) updateResolvedTsPeriodically(ctx context.Context) error {
	tryUpdateResolvedTs := func() {
		pendingTs := s.pendingResolvedTs.Load()
		defer func() {
			pdPhyTs := oracle.GetPhysical(s.pdClock.CurrentTime())
			resolvedPhyTs := oracle.ExtractPhysical(pendingTs)
			resolvedLag := float64(pdPhyTs-resolvedPhyTs) / 1e3
			metrics.SchemaStoreResolvedTsLagGauge.Set(float64(resolvedLag))
		}()

		if pendingTs <= s.resolvedTs.Load() {
			return
		}
		resolvedEvents := s.unsortedCache.fetchSortedDDLEventBeforeTS(pendingTs)
		if len(resolvedEvents) != 0 {
			log.Info("schema store begin to apply resolved ddl events",
				zap.Uint64("resolvedTs", pendingTs),
				zap.Int("resolvedEventsLen", len(resolvedEvents)))

			for _, event := range resolvedEvents {
				if event.Job.BinlogInfo.FinishedTS <= s.finishedDDLTs ||
					event.Job.BinlogInfo.SchemaVersion == 0 /* means the ddl is ignored in upstream */ {
					log.Info("skip already applied ddl job",
						zap.Any("type", event.Job.Type),
						zap.String("job", event.Job.Query),
						zap.Int64("jobSchemaVersion", event.Job.BinlogInfo.SchemaVersion),
						zap.Uint64("jobFinishTs", event.Job.BinlogInfo.FinishedTS),
						zap.Uint64("jobCommitTs", event.CommitTs),
						zap.Any("storeSchemaVersion", s.schemaVersion),
						zap.Uint64("storeFinishedDDLTS", s.finishedDDLTs))
					continue
				}
				log.Info("handle a ddl job",
					zap.Int64("schemaID", event.Job.SchemaID),
					zap.String("schemaName", event.Job.SchemaName),
					zap.Int64("tableID", event.Job.TableID),
					zap.String("tableName", event.Job.TableName),
					zap.Any("type", event.Job.Type),
					zap.String("job", event.Job.Query),
					zap.Int64("jobSchemaVersion", event.Job.BinlogInfo.SchemaVersion),
					zap.Uint64("jobFinishTs", event.Job.BinlogInfo.FinishedTS),
					zap.Uint64("jobCommitTs", event.CommitTs),
					zap.Any("storeSchemaVersion", s.schemaVersion),
					zap.Any("tableInfo", event.Job.BinlogInfo.TableInfo),
					zap.Uint64("storeFinishedDDLTS", s.finishedDDLTs))

				// need to update the following two members for every event to filter out later duplicate events
				s.schemaVersion = event.Job.BinlogInfo.SchemaVersion
				s.finishedDDLTs = event.Job.BinlogInfo.FinishedTS

				s.dataStorage.handleDDLJob(event.Job)
			}
		}
		// When register a new table, it will load all ddl jobs from disk for the table,
		// so we can only update resolved ts after all ddl jobs are written to disk
		// Can we optimize it to update resolved ts more eagerly?
		s.resolvedTs.Store(pendingTs)
		s.dataStorage.updateUpperBound(UpperBoundMeta{
			FinishedDDLTs: s.finishedDDLTs,
			SchemaVersion: s.schemaVersion,
			ResolvedTs:    pendingTs,
		})
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			tryUpdateResolvedTs()
		case <-s.notifyCh:
			tryUpdateResolvedTs()
		}
	}
}

func (s *schemaStore) GetAllPhysicalTables(snapTs uint64, filter filter.Filter) ([]commonEvent.Table, error) {
	s.waitResolvedTs(0, snapTs, 10*time.Second)
	return s.dataStorage.getAllPhysicalTables(snapTs, filter)
}

func (s *schemaStore) RegisterTable(tableID int64, startTs uint64) error {
	metrics.SchemaStoreResolvedRegisterTableGauge.Inc()
	s.waitResolvedTs(tableID, startTs, 5*time.Second)
	log.Info("register table",
		zap.Int64("tableID", tableID),
		zap.Uint64("startTs", startTs),
		zap.Uint64("resolvedTs", s.resolvedTs.Load()))
	return s.dataStorage.registerTable(tableID, startTs)
}

func (s *schemaStore) UnregisterTable(tableID int64) error {
	metrics.SchemaStoreResolvedRegisterTableGauge.Dec()
	return s.dataStorage.unregisterTable(tableID)
}

func (s *schemaStore) GetTableInfo(tableID int64, ts uint64) (*common.TableInfo, error) {
	metrics.SchemaStoreGetTableInfoCounter.Inc()
	start := time.Now()
	defer func() {
		metrics.SchemaStoreGetTableInfoLagHist.Observe(time.Since(start).Seconds())
	}()
	s.waitResolvedTs(tableID, ts, 2*time.Second)
	return s.dataStorage.getTableInfo(tableID, ts)
}

func (s *schemaStore) GetTableDDLEventState(tableID int64) DDLEventState {
	resolvedTs := s.resolvedTs.Load()
	maxEventCommitTs := s.dataStorage.getMaxEventCommitTs(tableID, resolvedTs)
	return DDLEventState{
		ResolvedTs:       resolvedTs,
		MaxEventCommitTs: maxEventCommitTs,
	}
}

func (s *schemaStore) FetchTableDDLEvents(dispatcherID common.DispatcherID, tableID int64, tableFilter filter.Filter, start, end uint64) ([]commonEvent.DDLEvent, error) {
	currentResolvedTs := s.resolvedTs.Load()
	if end > currentResolvedTs {
		log.Panic("end should not be greater than current resolved ts",
			zap.Stringer("dispatcherID", dispatcherID),
			zap.Int64("tableID", tableID),
			zap.Uint64("start", start),
			zap.Uint64("end", end),
			zap.Uint64("currentResolvedTs", currentResolvedTs))
	}
	events, err := s.dataStorage.fetchTableDDLEvents(dispatcherID, tableID, tableFilter, start, end)
	if err != nil {
		return nil, err
	}
	return events, nil
}

// FetchTableTriggerDDLEvents returns the next ddl events which finishedTs are within the range (start, end]
func (s *schemaStore) FetchTableTriggerDDLEvents(tableFilter filter.Filter, start uint64, limit int) ([]commonEvent.DDLEvent, uint64, error) {
	if limit == 0 {
		log.Panic("limit cannot be 0")
	}
	// must get resolved ts first
	currentResolvedTs := s.resolvedTs.Load()
	if currentResolvedTs <= start {
		return nil, currentResolvedTs, nil
	}

	events, err := s.dataStorage.fetchTableTriggerDDLEvents(tableFilter, start, limit)
	if err != nil {
		return nil, 0, err
	}
	if len(events) == limit {
		return events, events[limit-1].FinishedTs, nil
	}
	end := currentResolvedTs
	if len(events) > 0 && events[len(events)-1].FinishedTs > currentResolvedTs {
		end = events[len(events)-1].FinishedTs
	}
	log.Debug("FetchTableTriggerDDLEvents end",
		zap.Uint64("start", start),
		zap.Int("limit", limit),
		zap.Uint64("end", end),
		zap.Any("events", events))
	return events, end, nil
}

func (s *schemaStore) writeDDLEvent(ddlEvent DDLJobWithCommitTs) {
	log.Debug("write ddl event",
		zap.Int64("schemaID", ddlEvent.Job.SchemaID),
		zap.Int64("tableID", ddlEvent.Job.TableID),
		zap.Uint64("finishedTs", ddlEvent.Job.BinlogInfo.FinishedTS),
		zap.String("query", ddlEvent.Job.Query))

	if !filter.IsSysSchema(ddlEvent.Job.SchemaName) {
		s.unsortedCache.addDDLEvent(ddlEvent)
	}
}

// advancePendingResolvedTs will be call by ddlJobFetcher when it fetched a new ddl event
// it will update the pendingResolvedTs and notify the updateResolvedTs goroutine to apply the ddl event
func (s *schemaStore) advancePendingResolvedTs(resolvedTs uint64) {
	for {
		currentTs := s.pendingResolvedTs.Load()
		if resolvedTs <= currentTs {
			return
		}
		if s.pendingResolvedTs.CompareAndSwap(currentTs, resolvedTs) {
			select {
			case s.notifyCh <- struct{}{}:
			default:
			}
			return
		}
	}
}

// TODO: use notify instead of sleep
// waitResolvedTs will wait until the schemaStore resolved ts is greater than or equal to ts.
func (s *schemaStore) waitResolvedTs(tableID int64, ts uint64, logInterval time.Duration) {
	start := time.Now()
	lastLogTime := time.Now()
	for {
		if s.resolvedTs.Load() >= ts {
			return
		}
		time.Sleep(time.Millisecond * 10)
		if time.Since(lastLogTime) > logInterval {
			log.Info("wait resolved ts slow",
				zap.Int64("tableID", tableID),
				zap.Any("ts", ts),
				zap.Uint64("resolvedTS", s.resolvedTs.Load()),
				zap.Any("time", time.Since(start)))
			lastLogTime = time.Now()
		}
	}
}
