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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/utils/heap"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestAdvanceSchemaStoreResolvedTs(t *testing.T) {
	tsCh := make(chan uint64, 100)
	advanceResolvedTs := func(resolvedTS uint64) {
		tsCh <- resolvedTS
	}
	ddlJobFetcher := &ddlJobFetcher{
		advanceResolvedTs: advanceResolvedTs,
	}
	ddlJobFetcher.resolvedTsTracker.resolvedTsItemMap = make(map[logpuller.SubscriptionID]*resolvedTsItem)
	ddlJobFetcher.resolvedTsTracker.resolvedTsHeap = heap.NewHeap[*resolvedTsItem]()

	addSubscription := func(subID logpuller.SubscriptionID) {
		item := &resolvedTsItem{
			resolvedTs: 0,
		}
		ddlJobFetcher.resolvedTsTracker.resolvedTsItemMap[subID] = item
		ddlJobFetcher.resolvedTsTracker.resolvedTsHeap.AddOrUpdate(item)
	}
	subID1 := logpuller.SubscriptionID(100)
	addSubscription(subID1)
	subID2 := logpuller.SubscriptionID(101)
	addSubscription(subID2)

	{
		ddlJobFetcher.tryAdvanceResolvedTs(subID1, 100)
		ddlJobFetcher.tryAdvanceResolvedTs(subID1, 200)

		select {
		case ts := <-tsCh:
			require.Equal(t, uint64(0), ts)
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
		select {
		case ts := <-tsCh:
			require.Equal(t, uint64(0), ts)
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
	}

	{
		ddlJobFetcher.tryAdvanceResolvedTs(subID2, 100)

		select {
		case ts := <-tsCh:
			require.Equal(t, uint64(100), ts)
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
	}

	{
		ddlJobFetcher.tryAdvanceResolvedTs(subID2, 300)

		select {
		case ts := <-tsCh:
			require.Equal(t, uint64(200), ts)
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
	}

	{
		ddlJobFetcher.tryAdvanceResolvedTs(subID2, 400)

		select {
		case ts := <-tsCh:
			require.Equal(t, uint64(200), ts)
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
	}

	{
		ddlJobFetcher.tryAdvanceResolvedTs(subID1, 300)

		select {
		case ts := <-tsCh:
			require.Equal(t, uint64(300), ts)
		case <-time.NewTimer(100 * time.Millisecond).C:
			require.True(t, false, "must get an event")
		}
	}
}

func TestGetAllDDLSpan(t *testing.T) {
	// Scenario: TiCDC now watches only tidb_ddl_job after TiDB normalized
	// create-table DDL delivery back onto the job table. Next Gen requires a
	// non-default keyspace ID, while Classic keeps using the default keyspace.
	// Steps: build the watched spans for a keyspace valid in the active kernel
	// and verify there is exactly one subscription span for tidb_ddl_job.
	keyspaceID := common.DefaultKeyspaceID
	if kerneltype.IsNextGen() {
		keyspaceID = 1
	}
	spans, err := getAllDDLSpan(keyspaceID)
	require.NoError(t, err)
	require.Len(t, spans, 1)
	require.Equal(t, common.JobTableID, spans[0].TableID)
}

func TestDDLJobFetcherInputLegacyFormat(t *testing.T) {
	job := &model.Job{
		ID:         100,
		Type:       model.ActionCreateTable,
		State:      model.JobStateDone,
		BinlogInfo: &model.HistoryInfo{},
	}
	rawJob, err := job.Encode(true)
	require.NoError(t, err)

	var cached []DDLJobWithCommitTs
	fetcher := newDDLJobFetcher(
		context.Background(), nil, nil, common.DefaultKeyspaceID,
		func(event DDLJobWithCommitTs) { cached = append(cached, event) },
		func(uint64) {},
	)
	entries := []common.RawKVEntry{
		{OpType: common.OpTypeDelete, Key: []byte("mDDLJobList"), Value: rawJob, CRTs: 200},
		{OpType: common.OpTypePut, Key: []byte("mDDLJobList"), Value: rawJob, StartTs: 101, CRTs: 202},
	}

	require.False(t, fetcher.input(entries, nil))
	require.Len(t, cached, 1)
	require.Equal(t, uint64(202), cached[0].CommitTs)
	require.Equal(t, uint64(101), cached[0].Job.StartTS)
	require.Equal(t, uint64(202), cached[0].Job.BinlogInfo.FinishedTS)
	require.Equal(t, int64(100), cached[0].Job.ID)
}

func TestDDLJobFetcherInputJobTableFormat(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	t.Cleanup(helper.Close)
	job := &model.Job{
		ID:         101,
		Type:       model.ActionDropTable,
		State:      model.JobStateDone,
		SchemaID:   10,
		TableID:    20,
		BinlogInfo: &model.HistoryInfo{},
	}
	rawJob, err := job.Encode(true)
	require.NoError(t, err)
	rawKVs := helper.DML2RawKv(
		common.JobTableID,
		100,
		fmt.Sprintf(
			"insert into mysql.tidb_ddl_job(job_id, reorg, schema_ids, table_ids, job_meta, type, processing) "+
				"values (%d, 0, '%d', '%d', x'%x', %d, 0)",
			job.ID, job.SchemaID, job.TableID, rawJob, job.Type),
	)
	require.Len(t, rawKVs, 1)

	var cached []DDLJobWithCommitTs
	fetcher := newDDLJobFetcher(
		context.Background(), nil, helper.Storage(), common.DefaultKeyspaceID,
		func(event DDLJobWithCommitTs) { cached = append(cached, event) },
		func(uint64) {},
	)
	require.False(t, fetcher.input([]common.RawKVEntry{*rawKVs[0]}, nil))
	require.Len(t, cached, 1)
	require.Equal(t, job.ID, cached[0].Job.ID)
	require.Equal(t, job.Type, cached[0].Job.Type)
	require.Equal(t, rawKVs[0].StartTs, cached[0].Job.StartTS)
	require.Equal(t, rawKVs[0].CRTs, cached[0].Job.BinlogInfo.FinishedTS)
	require.NotNil(t, fetcher.ddlTableInfo)
	require.Equal(t, common.JobTableID, fetcher.ddlTableInfo.DDLJobTable.TableName.TableID)
}
