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

package schemastore

import (
	"context"
	"math"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/utils/heap"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"go.uber.org/zap"
)

// ddl puller should never filter any DDL jobs even if
// the changefeed is in BDR mode, because the DDL jobs should
// be filtered before they are sent to the sink
const ddlPullerFilterLoop = false

type ddlJobFetcher struct {
	ctx               context.Context
	subClient         logpuller.SubscriptionClient
	resolvedTsTracker struct {
		sync.Mutex
		resolvedTsItemMap map[logpuller.SubscriptionID]*resolvedTsItem
		resolvedTsHeap    *heap.Heap[*resolvedTsItem]
	}

	// cacheDDLEvent and advanceResolvedTs may be called concurrently
	// the only guarantee is that when call advanceResolvedTs with ts, all ddl job with commit ts <= ts has been passed to cacheDDLEvent
	cacheDDLEvent     func(ddlEvent DDLJobWithCommitTs)
	advanceResolvedTs func(resolvedTS uint64)

	kvStorage kv.Storage

	keyspaceID uint32

	ddlTableInfo         *event.DDLTableInfo
	onceInitDDLTableInfo sync.Once
}

func newDDLJobFetcher(
	ctx context.Context,
	subClient logpuller.SubscriptionClient,
	kvStorage kv.Storage,
	keyspaceID uint32,
	cacheDDLEvent func(ddlEvent DDLJobWithCommitTs),
	advanceResolvedTs func(resolvedTS uint64),
) *ddlJobFetcher {
	fetcher := &ddlJobFetcher{
		ctx:               ctx,
		subClient:         subClient,
		cacheDDLEvent:     cacheDDLEvent,
		advanceResolvedTs: advanceResolvedTs,
		kvStorage:         kvStorage,
		keyspaceID:        keyspaceID,
	}
	fetcher.resolvedTsTracker.resolvedTsItemMap = make(map[logpuller.SubscriptionID]*resolvedTsItem)
	fetcher.resolvedTsTracker.resolvedTsHeap = heap.NewHeap[*resolvedTsItem]()

	return fetcher
}

func (p *ddlJobFetcher) run(startTs uint64) error {
	spans, err := getAllDDLSpan(p.keyspaceID)
	if err != nil {
		return err
	}
	for _, span := range spans {
		subID := p.subClient.AllocSubscriptionID()
		item := &resolvedTsItem{
			resolvedTs: 0,
		}
		p.resolvedTsTracker.resolvedTsItemMap[subID] = item
		p.resolvedTsTracker.resolvedTsHeap.AddOrUpdate(item)
		advanceSubSpanResolvedTs := func(ts uint64) {
			p.tryAdvanceResolvedTs(subID, ts)
		}
		p.subClient.Subscribe(subID, span, startTs, p.input, advanceSubSpanResolvedTs, 0, ddlPullerFilterLoop)
	}
	return nil
}

func (p *ddlJobFetcher) tryAdvanceResolvedTs(subID logpuller.SubscriptionID, newResolvedTs uint64) {
	p.resolvedTsTracker.Lock()
	defer p.resolvedTsTracker.Unlock()
	item, ok := p.resolvedTsTracker.resolvedTsItemMap[subID]
	if !ok {
		log.Panic("unknown subscriptionID, should not happen",
			zap.Uint64("subscriptionID", uint64(subID)))
	}
	if newResolvedTs < item.resolvedTs {
		log.Panic("resolved ts should not fallback",
			zap.Uint64("newResolvedTs", newResolvedTs),
			zap.Uint64("oldResolvedTs", item.resolvedTs))
	}
	item.resolvedTs = newResolvedTs
	p.resolvedTsTracker.resolvedTsHeap.AddOrUpdate(item)

	minResolvedTsItem, ok := p.resolvedTsTracker.resolvedTsHeap.PeekTop()
	if !ok || minResolvedTsItem.resolvedTs == math.MaxUint64 {
		log.Panic("should not happen")
	}
	// minResolvedTsItem may be 0, it's ok to send it because it will be filtered later.
	// it is ok to send redundant resolved ts to advanceResolvedTs.
	p.advanceResolvedTs(minResolvedTsItem.resolvedTs)
}

func (p *ddlJobFetcher) input(kvs []common.RawKVEntry, _ func()) bool {
	for _, entry := range kvs {
		job, err := p.unmarshalDDL(&entry)
		if err != nil {
			log.Fatal("unmarshal ddl failed", zap.Any("entry", entry), zap.Error(err))
		}

		if job == nil {
			continue
		}

		// cache ddl job in memory until the resolve ts pass its commit ts
		p.cacheDDLEvent(DDLJobWithCommitTs{
			Job:      job,
			CommitTs: entry.CRTs,
		})
	}
	return false
}

// unmarshalDDL unmarshal a ddl job from a raw kv entry.
func (p *ddlJobFetcher) unmarshalDDL(rawKV *common.RawKVEntry) (*model.Job, error) {
	if rawKV.OpType != common.OpTypePut {
		return nil, nil
	}
	if !event.IsLegacyFormatJob(rawKV) {
		p.onceInitDDLTableInfo.Do(func() {
			if err := p.initDDLTableInfo(p.ctx, p.kvStorage); err != nil {
				log.Fatal("init ddl table info failed", zap.Error(err))
			}
		})
	}

	return event.ParseDDLJob(rawKV, p.ddlTableInfo)
}

// getSnapshotMeta returns tidb meta information
func getSnapshotMeta(tiStore kv.Storage, ts uint64) meta.Reader {
	snapshot := tiStore.GetSnapshot(kv.NewVersion(ts))
	return meta.NewReader(snapshot)
}

func (p *ddlJobFetcher) initDDLTableInfo(ctx context.Context, kvStorage kv.Storage) error {
	version, err := kvStorage.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return errors.Trace(err)
	}
	snap := getSnapshotMeta(kvStorage, version.Ver)

	dbInfos, err := snap.ListDatabases()
	if err != nil {
		return errors.WrapError(errors.ErrMetaListDatabases, err)
	}

	db, err := findDBByName(dbInfos, mysql.SystemDB)
	if err != nil {
		return errors.Trace(err)
	}

	tbls, err := snap.ListTables(ctx, db.ID)
	if err != nil {
		return errors.Trace(err)
	}

	// for tidb_ddl_job
	tableInfo, err := findTableByName(tbls, "tidb_ddl_job")
	if err != nil {
		return errors.Trace(err)
	}

	col, err := findColumnByName(tableInfo.Columns, "job_meta")
	if err != nil {
		return errors.Trace(err)
	}

	p.ddlTableInfo = &event.DDLTableInfo{}
	p.ddlTableInfo.DDLJobTable = common.WrapTableInfo(db.Name.L, tableInfo)
	p.ddlTableInfo.JobMetaColumnIDinJobTable = col.ID

	// for tidb_ddl_history
	historyTableInfo, err := findTableByName(tbls, "tidb_ddl_history")
	if err != nil {
		return errors.Trace(err)
	}

	historyTableCol, err := findColumnByName(historyTableInfo.Columns, "job_meta")
	if err != nil {
		return errors.Trace(err)
	}

	p.ddlTableInfo.DDLHistoryTable = common.WrapTableInfo(db.Name.L, historyTableInfo)
	p.ddlTableInfo.JobMetaColumnIDinHistoryTable = historyTableCol.ID

	return nil
}

// Below are some helper functions for ddl puller.
func findDBByName(dbs []*model.DBInfo, name string) (*model.DBInfo, error) {
	for _, db := range dbs {
		if db.Name.L == name {
			return db, nil
		}
	}
	return nil, errors.WrapError(
		errors.ErrDDLSchemaNotFound,
		errors.Errorf("can't find schema %s", name))
}

func findTableByName(tbls []*model.TableInfo, name string) (*model.TableInfo, error) {
	for _, t := range tbls {
		if t.Name.L == name {
			return t, nil
		}
	}
	return nil, errors.WrapError(
		errors.ErrDDLSchemaNotFound,
		errors.Errorf("can't find table %s", name))
}

func findColumnByName(cols []*model.ColumnInfo, name string) (*model.ColumnInfo, error) {
	for _, c := range cols {
		if c.Name.L == name {
			return c, nil
		}
	}
	return nil, errors.WrapError(
		errors.ErrDDLSchemaNotFound,
		errors.Errorf("can't find column %s", name))
}

const (
	// JobTableID is the id of `tidb_ddl_job`.
	JobTableID = metadef.TiDBDDLJobTableID
	// JobHistoryID is the id of `tidb_ddl_history`
	JobHistoryID = metadef.TiDBDDLHistoryTableID
)

func getAllDDLSpan(keyspaceID uint32) ([]heartbeatpb.TableSpan, error) {
	spans := make([]heartbeatpb.TableSpan, 0, 2)

	start, end, err := common.GetKeyspaceTableRange(keyspaceID, JobTableID)
	if err != nil {
		return nil, err
	}

	spans = append(spans, heartbeatpb.TableSpan{
		TableID:    JobTableID,
		StartKey:   common.ToComparableKey(start),
		EndKey:     common.ToComparableKey(end),
		KeyspaceID: keyspaceID,
	})

	start, end, err = common.GetKeyspaceTableRange(keyspaceID, JobHistoryID)
	if err != nil {
		return nil, err
	}
	spans = append(spans, heartbeatpb.TableSpan{
		TableID:    JobHistoryID,
		StartKey:   common.ToComparableKey(start),
		EndKey:     common.ToComparableKey(end),
		KeyspaceID: keyspaceID,
	})
	return spans, nil
}

type resolvedTsItem struct {
	resolvedTs uint64
	heapIndex  int
}

func (m *resolvedTsItem) SetHeapIndex(index int) { m.heapIndex = index }

func (m *resolvedTsItem) GetHeapIndex() int { return m.heapIndex }

func (m *resolvedTsItem) LessThan(other *resolvedTsItem) bool {
	return m.resolvedTs < other.resolvedTs
}
