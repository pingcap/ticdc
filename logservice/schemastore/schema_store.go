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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

type SchemaStore interface {
	common.SubModule

	GetAllPhysicalTables(keyspaceID uint32, snapTs uint64, filter filter.Filter) ([]commonEvent.Table, error)

	RegisterTable(keyspaceID uint32, tableID int64, startTs uint64) error

	UnregisterTable(keyspaceID uint32, tableID int64) error

	// GetTableInfo return table info with the largest version <= ts
	GetTableInfo(keyspaceID uint32, tableID int64, ts uint64) (*common.TableInfo, error)

	// TODO: how to respect tableFilter
	GetTableDDLEventState(keyspaceID uint32, tableID int64) (DDLEventState, error)

	// FetchTableDDLEvents returns the next ddl events which finishedTs are within the range (start, end]
	// The caller must ensure end <= current resolvedTs
	// TODO: add a parameter limit
	FetchTableDDLEvents(keyspaceID uint32, dispatcherID common.DispatcherID, tableID int64, tableFilter filter.Filter, start, end uint64) ([]commonEvent.DDLEvent, error)

	FetchTableTriggerDDLEvents(keyspaceID uint32, dispatcherID common.DispatcherID, tableFilter filter.Filter, start uint64, limit int) ([]commonEvent.DDLEvent, uint64, error)

	// RegisterKeyspace register a keyspace to fetch table ddl
	RegisterKeyspace(ctx context.Context, keyspaceName string) error
}

type DDLEventState struct {
	ResolvedTs       uint64
	MaxEventCommitTs uint64
}

type keyspaceSchemaStore struct {
	ddlJobFetcher *ddlJobFetcher
	pdClock       pdutil.Clock

	// store unresolved ddl event in memory, it is thread safe
	unsortedCache *ddlCache

	// store ddl event and other metadata on disk, it is thread safe
	dataStorage *persistentStorage

	notifyCh chan any

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

func (s *keyspaceSchemaStore) tryUpdateResolvedTs() {
	pendingTs := s.pendingResolvedTs.Load()
	defer func() {
		pdPhyTs := oracle.GetPhysical(s.pdClock.CurrentTime())
		resolvedPhyTs := oracle.ExtractPhysical(pendingTs)
		resolvedLag := float64(pdPhyTs-resolvedPhyTs) / 1e3
		metrics.SchemaStoreResolvedTsLagGauge.Set(resolvedLag)
	}()

	if pendingTs <= s.resolvedTs.Load() {
		return
	}
	resolvedEvents := s.unsortedCache.fetchSortedDDLEventBeforeTS(pendingTs)
	for _, event := range resolvedEvents {
		if event.Job.BinlogInfo.SchemaVersion == 0 /* means the ddl is ignored in upstream */ {
			log.Info("skip ddl job with empty SchemaVersion",
				zap.Any("type", event.Job.Type),
				zap.String("job", event.Job.Query),
				zap.Int64("jobSchemaVersion", event.Job.BinlogInfo.SchemaVersion),
				zap.Uint64("jobFinishTs", event.Job.BinlogInfo.FinishedTS),
				zap.Uint64("jobCommitTs", event.CommitTs),
				zap.Any("storeSchemaVersion", s.schemaVersion),
				zap.Uint64("storeFinishedDDLTS", s.finishedDDLTs))
			continue
		}
		if event.Job.BinlogInfo.FinishedTS <= s.finishedDDLTs {
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
			zap.String("DDL", event.Job.Query),
			zap.Int64("schemaVersion", event.Job.BinlogInfo.SchemaVersion),
			zap.Uint64("jobFinishTs", event.Job.BinlogInfo.FinishedTS),
			zap.Any("storeSchemaVersion", s.schemaVersion),
			zap.Uint64("storeFinishedDDLTS", s.finishedDDLTs))

		// need to update the following two members for every event to filter out later duplicate events
		s.schemaVersion = event.Job.BinlogInfo.SchemaVersion
		s.finishedDDLTs = event.Job.BinlogInfo.FinishedTS

		s.dataStorage.handleDDLJob(event.Job)
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

// TODO tenfyzhong 2025-09-13 13:40:26 use a chan to decoupling
func (s *keyspaceSchemaStore) writeDDLEvent(ddlEvent DDLJobWithCommitTs) {
	log.Debug("write ddl event",
		zap.Int64("schemaID", ddlEvent.Job.SchemaID),
		zap.Int64("tableID", ddlEvent.Job.TableID),
		zap.Uint64("finishedTs", ddlEvent.Job.BinlogInfo.FinishedTS),
		zap.String("query", ddlEvent.Job.Query))

	serverConfig := config.GetGlobalServerConfig()
	for _, ts := range serverConfig.Debug.SchemaStore.IgnoreDDLCommitTs {
		if ts == ddlEvent.CommitTs {
			log.Info("ignore ddl job by commit ts",
				zap.Uint64("commitTs", ts),
				zap.String("query", ddlEvent.Job.Query))
			return
		}
	}

	if !filter.IsSysSchema(ddlEvent.Job.SchemaName) {
		s.unsortedCache.addDDLEvent(ddlEvent)
	}
}

// TODO tenfyzhong 2025-09-13 13:40:26 use a chan to decoupling
// advancePendingResolvedTs will be call by ddlJobFetcher when it fetched a new ddl event
// it will update the pendingResolvedTs and notify the updateResolvedTs goroutine to apply the ddl event
func (s *keyspaceSchemaStore) advancePendingResolvedTs(resolvedTs uint64) {
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
func (s *keyspaceSchemaStore) waitResolvedTs(tableID int64, ts uint64, logInterval time.Duration) {
	start := time.Now()
	lastLogTime := time.Now()
	defer func() {
		metrics.SchemaStoreWaitResolvedTsDurationHist.Observe(time.Since(start).Seconds())
	}()
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

type schemaStore struct {
	pdClock pdutil.Clock
	pdCli   pd.Client
	root    string

	// keyspaceSchemaStoreMap is a map to store *keyspaceSchemaStore for every keyspace.
	// The key is keyspaceID
	keyspaceSchemaStoreMap map[uint32]*keyspaceSchemaStore
	keyspaceLocker         sync.RWMutex
}

func New(root string, pdCli pd.Client) SchemaStore {
	s := &schemaStore{
		pdClock:                appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock),
		pdCli:                  pdCli,
		root:                   root,
		keyspaceSchemaStoreMap: make(map[uint32]*keyspaceSchemaStore),
	}
	return s
}

func (s *schemaStore) Name() string {
	return appcontext.SchemaStore
}

func (s *schemaStore) getKeyspaceSchemaStore(keyspaceID uint32) (*keyspaceSchemaStore, error) {
	s.keyspaceLocker.RLock()
	store, ok := s.keyspaceSchemaStoreMap[keyspaceID]
	s.keyspaceLocker.RUnlock()
	if ok {
		return store, nil
	}

	ctx := context.Background()
	// If the schemastore does not contain the keyspace, it means it is not a maintainer node.
	// It should register the keyspace when it tries to get keyspace schema_store.
	keyspaceManager := appcontext.GetService[keyspace.Manager](appcontext.KeyspaceManager)
	keyspaceMeta, err := keyspaceManager.GetKeyspaceByID(ctx, keyspaceID)
	if err != nil {
		return nil, err
	}

	if err = s.RegisterKeyspace(ctx, keyspaceMeta.Name); err != nil {
		return nil, err
	}

	s.keyspaceLocker.RLock()
	store, ok = s.keyspaceSchemaStoreMap[keyspaceID]
	s.keyspaceLocker.RUnlock()
	if ok {
		return store, nil
	}

	return nil, errors.ErrKeyspaceNotFound.FastGenByArgs(keyspaceID)
}

func (s *schemaStore) Run(ctx context.Context) error {
	log.Info("schema store begin to run")
	// we should fetch ddl at startup for classic mode
	if kerneltype.IsClassic() {
		err := s.RegisterKeyspace(ctx, common.DefaultKeyspace)
		if err != nil {
			// initialize is called when the server starts
			// if the keyspace register failed, we can panic the server to let
			// it register again
			log.Panic("RegisterKeyspace failed", zap.Error(err))
		}
	}
	return nil
}

func (s *schemaStore) Close(_ context.Context) error {
	s.keyspaceLocker.Lock()
	defer s.keyspaceLocker.Unlock()

	for keyspaceID, store := range s.keyspaceSchemaStoreMap {
		err := store.dataStorage.close()
		if err != nil {
			log.Error("dataStorage close failed", zap.Uint32("keyspaceID", keyspaceID), zap.Error(err))
		}
	}
	log.Info("schema store closed")
	return nil
}

func (s *schemaStore) GetAllPhysicalTables(keyspaceID uint32, snapTs uint64, filter filter.Filter) ([]commonEvent.Table, error) {
	store, err := s.getKeyspaceSchemaStore(keyspaceID)
	if err != nil {
		return nil, err
	}

	store.waitResolvedTs(0, snapTs, 10*time.Second)
	return store.dataStorage.getAllPhysicalTables(snapTs, filter)
}

func (s *schemaStore) RegisterTable(keyspaceID uint32, tableID int64, startTs uint64) error {
	store, err := s.getKeyspaceSchemaStore(keyspaceID)
	if err != nil {
		return err
	}

	metrics.SchemaStoreResolvedRegisterTableGauge.Inc()
	store.waitResolvedTs(tableID, startTs, 5*time.Second)
	log.Info("register table",
		zap.Uint32("keyspaceID", keyspaceID),
		zap.Int64("tableID", tableID),
		zap.Uint64("startTs", startTs),
		zap.Uint64("resolvedTs", store.resolvedTs.Load()))
	return store.dataStorage.registerTable(tableID, startTs)
}

func (s *schemaStore) UnregisterTable(keyspaceID uint32, tableID int64) error {
	store, err := s.getKeyspaceSchemaStore(keyspaceID)
	if err != nil {
		return err
	}
	metrics.SchemaStoreResolvedRegisterTableGauge.Dec()
	return store.dataStorage.unregisterTable(tableID)
}

func (s *schemaStore) GetTableInfo(keyspaceID uint32, tableID int64, ts uint64) (*common.TableInfo, error) {
	store, err := s.getKeyspaceSchemaStore(keyspaceID)
	if err != nil {
		return nil, err
	}

	metrics.SchemaStoreGetTableInfoCounter.Inc()
	start := time.Now()
	defer func() {
		metrics.SchemaStoreGetTableInfoLagHist.Observe(time.Since(start).Seconds())
	}()
	store.waitResolvedTs(tableID, ts, 2*time.Second)
	return store.dataStorage.getTableInfo(tableID, ts)
}

func (s *schemaStore) GetTableDDLEventState(keyspaceID uint32, tableID int64) (DDLEventState, error) {
	store, err := s.getKeyspaceSchemaStore(keyspaceID)
	if err != nil {
		return DDLEventState{}, err
	}

	resolvedTs := store.resolvedTs.Load()
	maxEventCommitTs := store.dataStorage.getMaxEventCommitTs(tableID, resolvedTs)
	return DDLEventState{
		ResolvedTs:       resolvedTs,
		MaxEventCommitTs: maxEventCommitTs,
	}, nil
}

// FetchTableDDLEvents returns the ddl events which finishedTs are within the range (start, end]
func (s *schemaStore) FetchTableDDLEvents(
	keyspaceID uint32, dispatcherID common.DispatcherID, tableID int64, tableFilter filter.Filter, start, end uint64,
) ([]commonEvent.DDLEvent, error) {
	store, err := s.getKeyspaceSchemaStore(keyspaceID)
	if err != nil {
		return nil, err
	}

	currentResolvedTs := store.resolvedTs.Load()
	if end > currentResolvedTs {
		log.Warn("end should not be greater than current resolved ts",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Stringer("dispatcherID", dispatcherID),
			zap.Int64("tableID", tableID),
			zap.Uint64("start", start),
			zap.Uint64("end", end),
			zap.Uint64("currentResolvedTs", currentResolvedTs))
		return nil, errors.New(fmt.Sprintf("end %d should not be greater than current resolved ts %d", end, currentResolvedTs))
	}
	events, err := store.dataStorage.fetchTableDDLEvents(dispatcherID, tableID, tableFilter, start, end)
	if err != nil {
		return nil, err
	}
	return events, nil
}

// FetchTableTriggerDDLEvents returns the next ddl events which finishedTs are within the range (start, end]
func (s *schemaStore) FetchTableTriggerDDLEvents(keyspaceID uint32, dispatcherID common.DispatcherID, tableFilter filter.Filter, start uint64, limit int) ([]commonEvent.DDLEvent, uint64, error) {
	store, err := s.getKeyspaceSchemaStore(keyspaceID)
	if err != nil {
		return nil, 0, err
	}

	// must get resolved ts first
	currentResolvedTs := store.resolvedTs.Load()
	if currentResolvedTs <= start {
		return nil, currentResolvedTs, nil
	}

	events, err := store.dataStorage.fetchTableTriggerDDLEvents(tableFilter, start, limit)
	if err != nil {
		return nil, 0, err
	}

	if len(events) == limit {
		return events, events[limit-1].FinishedTs, nil
	}
	end := currentResolvedTs
	// after we get currentResolvedTs, there may be new ddl events with FinishedTs > currentResolvedTs
	// so we need to extend the end to include these new ddl events
	if len(events) > 0 && events[len(events)-1].FinishedTs > currentResolvedTs {
		end = events[len(events)-1].FinishedTs
	}
	log.Debug("FetchTableTriggerDDLEvents end",
		zap.Uint32("keyspaceID", keyspaceID),
		zap.Stringer("dispatcherID", dispatcherID),
		zap.Uint64("start", start),
		zap.Int("limit", limit),
		zap.Uint64("end", end),
		zap.Any("events", events))
	return events, end, nil
}

// RegisterKeyspace register a keyspace to fetch table ddl
// Should be called after changefeed creates
// For classic mode, the keyspace is nil
func (s *schemaStore) RegisterKeyspace(
	ctx context.Context,
	keyspaceName string,
) error {
	keyspaceManager := appcontext.GetService[keyspace.Manager](appcontext.KeyspaceManager)
	keyspaceMeta, err := keyspaceManager.LoadKeyspace(ctx, keyspaceName)
	if err != nil {
		return err
	}

	keyspaceID := keyspaceMeta.Id

	s.keyspaceLocker.Lock()
	defer s.keyspaceLocker.Unlock()
	// If the keyspace has already been registered
	// No need to register again
	if _, ok := s.keyspaceSchemaStoreMap[keyspaceID]; ok {
		return nil
	}

	kvStorage, err := keyspaceManager.GetStorage(ctx, keyspaceName)
	if err != nil {
		return err
	}

	storage, err := newPersistentStorage(ctx, s.root, keyspaceID, s.pdCli, kvStorage)
	if err != nil {
		return err
	}
	store := &keyspaceSchemaStore{
		pdClock:       s.pdClock,
		unsortedCache: newDDLCache(),
		dataStorage:   storage,
		notifyCh:      make(chan any, 4),
	}

	upperBound := store.dataStorage.getUpperBound()
	store.finishedDDLTs = upperBound.FinishedDDLTs
	store.schemaVersion = upperBound.SchemaVersion
	store.pendingResolvedTs.Store(upperBound.ResolvedTs)
	store.resolvedTs.Store(upperBound.ResolvedTs)
	log.Info("schema store initialized",
		zap.String("keyspaceName", keyspaceName),
		zap.Uint32("keyspaceID", keyspaceID),
		zap.Uint64("resolvedTs", store.resolvedTs.Load()),
		zap.Uint64("finishedDDLTS", store.finishedDDLTs),
		zap.Int64("schemaVersion", store.schemaVersion))

	subClient := appcontext.GetService[logpuller.SubscriptionClient](appcontext.SubscriptionClient)
	fetcher := newDDLJobFetcher(
		ctx,
		subClient,
		kvStorage,
		keyspaceID,
		store.writeDDLEvent,
		store.advancePendingResolvedTs,
	)
	store.ddlJobFetcher = fetcher

	err = fetcher.run(upperBound.ResolvedTs)
	if err != nil {
		return err
	}
	store.dataStorage.run()

	go func(ctx context.Context, schemaStore *keyspaceSchemaStore) {
		ticker := time.NewTicker(50 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				schemaStore.tryUpdateResolvedTs()
			case <-schemaStore.notifyCh:
				schemaStore.tryUpdateResolvedTs()
			}
		}
	}(ctx, store)

	s.keyspaceSchemaStoreMap[keyspaceID] = store

	return nil
}
