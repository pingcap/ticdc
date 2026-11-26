// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package decode

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/pingcap/ticdc/cmd/consumer/source"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/debezium"
	"github.com/pingcap/ticdc/pkg/sink/codec/schemamanager"
	"github.com/pingcap/ticdc/pkg/sink/codec/simple"
	"github.com/stretchr/testify/require"
)

func TestDecodeStorageCanalJSONSharesAlignedTableInfo(t *testing.T) {
	cfg := common.NewConfig(config.ProtocolCanalJSON)
	cfg.EnableTiDBExtension = true
	cfg.Terminator = "\n"

	group := &Group{codecConfig: cfg}
	row1 := `{"id":0,"database":"test","table":"t1","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1,"ts":1,"sql":"","sqlType":{"id":4,"v":4},"mysqlType":{"id":"int","v":"int"},"data":[{"id":"1","v":"10"}],"old":null,"_tidb":{"commitTs":200}}`
	row2 := `{"id":0,"database":"test","table":"t1","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1,"ts":1,"sql":"","sqlType":{"id":4,"v":4},"mysqlType":{"id":"int","v":"int"},"data":[{"id":"2","v":"20"}],"old":null,"_tidb":{"commitTs":201}}`

	const tableVersion uint64 = 150
	const tableID int64 = 42
	f := &Future{
		Payload: &source.Payload{
			Value: []byte(row1 + "\n" + row2 + "\n"),
			File: &source.File{
				TableID: tableID,
				Schema: &cloudstorage.SchemaFile{
					Schema:       "test",
					Table:        "t1",
					TableVersion: tableVersion,
					Columns: []cloudstorage.TableCol{
						{Name: "id", Tp: "int", IsPK: "true"},
						{Name: "v", Tp: "int", Nullable: "true"},
					},
				},
			},
		},
	}

	require.NoError(t, group.decodeStorage(t.Context(), f))
	require.Len(t, f.Events, 2)

	dml1 := f.Events[0].(*event.DMLEvent)
	dml2 := f.Events[1].(*event.DMLEvent)
	t.Cleanup(func() {
		dml1.PostFlush()
		dml2.PostFlush()
	})

	require.Same(t, dml1.TableInfo, dml2.TableInfo)
	require.Equal(t, tableVersion, dml1.TableInfo.UpdateTS)
	require.Equal(t, tableID, dml1.PhysicalTableID)
	require.Equal(t, tableID, dml2.PhysicalTableID)
}

func TestDecodeGroupBroadcastDDLDedupBootstrapAndStaleSkip(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	ddlEvent := helper.DDL2Event("create table test.t1(id int primary key, v int)")
	dmlEvent := helper.DML2Event("test", "t1", "insert into test.t1 values (1, 10)")
	bootDDL := helper.DDL2Event("create table test.t2(id int primary key)")
	bootDDL.TableInfo.UpdateTS = 150

	cfg := common.NewConfig(config.ProtocolSimple)
	enc, err := simple.NewEncoder(cfg, nil)
	require.NoError(t, err)

	ddlMsg, err := enc.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)
	bootMsg, err := enc.EncodeDDLEvent(codec.NewBootstrapDDLEvent(bootDDL.TableInfo))
	require.NoError(t, err)

	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	dmlEvent.TableInfo.UpdateTS = 50
	require.NoError(t, enc.AppendRowChangedEvent(t.Context(), "topic", &event.RowEvent{
		TableInfo:      dmlEvent.TableInfo,
		CommitTs:       200,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}))
	dmlMsgs := enc.Build()
	require.Len(t, dmlMsgs, 1)

	schemas := common.NewTableInfoProvider()
	newSimple := func() decoder {
		raw, err := simple.NewDecoder(t.Context(), cfg, nil)
		require.NoError(t, err)
		s := raw.(*simple.Decoder)
		s.ShareTableInfo(schemas)
		return decoder{Decoder: s, schema: s}
	}
	group := &Group{codecConfig: cfg, schemas: schemas}
	dec0 := newSimple()
	dec1 := newSimple()

	// Partition 1 arrives before partition 0 and forwards the DDL.
	f1 := &Future{Payload: &source.Payload{Partition: 1, Value: ddlMsg.Value}}
	require.NoError(t, group.decodeInput(t.Context(), dec1, f1))
	require.Len(t, f1.Events, 1)
	require.Zero(t, f1.Marker)

	// Partition 0 drops the duplicate DDL after updating state, keeping commit-ts as Marker.
	f0 := &Future{Payload: &source.Payload{Partition: 0, Value: ddlMsg.Value}}
	require.NoError(t, group.decodeInput(t.Context(), dec0, f0))
	require.Empty(t, f0.Events)
	require.Equal(t, ddlEvent.GetCommitTs(), f0.Marker)

	// Bootstrap updates shared schemas without emitting an event or setting Marker.
	fBoot := &Future{Payload: &source.Payload{Partition: 1, Value: bootMsg.Value}}
	require.NoError(t, group.decodeInput(t.Context(), dec1, fBoot))
	require.Empty(t, fBoot.Events)
	require.Zero(t, fBoot.Marker)
	require.NotNil(t, schemas.Read("test", "t2", 150))

	// Replayed DML with CommitTs <= ReleasedWatermark is skipped before caching.
	schemas.ReleaseBefore(200)
	fStale := &Future{Payload: &source.Payload{Partition: 0, Value: dmlMsgs[0].Value}}
	require.NoError(t, group.decodeInput(t.Context(), dec0, fStale))
	require.Empty(t, fStale.Events)
	require.Empty(t, fStale.Pending)
}

func TestDecodeGroupConcurrentPoolPreservesOrderAndDynamicPartitions(t *testing.T) {
	cfg := common.NewConfig(config.ProtocolCanalJSON)
	cfg.EnableTiDBExtension = true

	// 1. MQ group decoupled from partitions handles partition counts > decodeConcurrency and preserves submission order.
	upstreamURI, err := url.Parse("kafka://127.0.0.1:9092/test-topic")
	require.NoError(t, err)
	opts := Options{
		CodecConfig:   cfg,
		ReplicaConfig: config.GetDefaultReplicaConfig(),
		UpstreamURI:   upstreamURI,
	}

	group := NewGroup(t.Context(), opts)
	require.Len(t, group.decoders, decodeConcurrency)
	require.Len(t, group.queues, decodeConcurrency)

	runCtx, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() {
		runDone <- group.Run(runCtx)
	}()
	t.Cleanup(func() {
		cancel()
		require.ErrorIs(t, <-runDone, context.Canceled)
		group.Close()
	})

	partitions := []int32{0, 1, 5, 17, 35}
	var payloads []*source.Payload
	for i, part := range partitions {
		val := fmt.Appendf(nil,
			`{"id":0,"database":"test","table":"t1","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1,"ts":1,"sql":"","sqlType":{"id":4},"mysqlType":{"id":"int"},"data":[{"id":"1"}],"old":null,"_tidb":{"commitTs":%d}}`,
			100+i,
		)
		payloads = append(payloads, &source.Payload{Partition: part, Value: val})
	}
	require.NoError(t, group.Add(t.Context(), payloads))

	for i, part := range partitions {
		f := <-group.Output()
		require.NoError(t, f.Ready(t.Context()))
		require.Equal(t, part, f.Payload.Partition)
		require.Len(t, f.Events, 2)
		dml := f.Events[0].(*event.DMLEvent)
		dml.PostFlush()
		require.Equal(t, uint64(100+i), dml.CommitTs)
		require.Nil(t, f.Events[1])
	}

	// 2. Storage group decodes across multiple concurrent decoders while preserving submission order.
	storageURI, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)
	storageOpts := Options{
		CodecConfig:   cfg,
		ReplicaConfig: config.GetDefaultReplicaConfig(),
		UpstreamURI:   storageURI,
	}

	storageGroup := NewGroup(t.Context(), storageOpts)
	storageCtx, storageCancel := context.WithCancel(t.Context())
	storageDone := make(chan error, 1)
	go func() {
		storageDone <- storageGroup.Run(storageCtx)
	}()
	t.Cleanup(func() {
		storageCancel()
		require.ErrorIs(t, <-storageDone, context.Canceled)
		storageGroup.Close()
	})

	var storagePayloads []*source.Payload
	for i := range 4 {
		val := fmt.Appendf(nil,
			`{"id":0,"database":"test","table":"t1","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1,"ts":1,"sql":"","sqlType":{"id":4},"mysqlType":{"id":"int"},"data":[{"id":"1"}],"old":null,"_tidb":{"commitTs":%d}}`,
			200+i,
		)
		storagePayloads = append(storagePayloads, &source.Payload{
			Value: val,
			File: &source.File{
				TableID: int64(10 + i),
				Schema: &cloudstorage.SchemaFile{
					Schema:       "test",
					Table:        "t1",
					TableVersion: 150,
					Columns:      []cloudstorage.TableCol{{Name: "id", Tp: "int", IsPK: "true"}},
				},
			},
		})
	}
	require.NoError(t, storageGroup.Add(t.Context(), storagePayloads))

	for i := range 4 {
		f := <-storageGroup.Output()
		require.NoError(t, f.Ready(t.Context()))
		dml := f.Events[0].(*event.DMLEvent)
		dml.PostFlush()
		require.Equal(t, uint64(200+i), dml.CommitTs)
	}
}

func TestDecodeGroupDebeziumAvroSharesSchemaManagerAndCloses(t *testing.T) {
	const registryURL = "http://127.0.0.1:8081"

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	var (
		mu         sync.Mutex
		nextID     int
		schemas    = make(map[int]string)
		lookupHits atomic.Int32
	)
	httpmock.RegisterResponder(http.MethodGet, registryURL, httpmock.NewStringResponder(http.StatusOK, "{}"))
	httpmock.RegisterResponder(http.MethodPost, `=~^http://127\.0\.0\.1:8081/subjects/.+/versions$`,
		func(r *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			var req struct {
				Schema string `json:"schema"`
			}
			require.NoError(t, json.Unmarshal(body, &req))
			mu.Lock()
			nextID++
			id := nextID
			schemas[id] = req.Schema
			mu.Unlock()
			return httpmock.NewJsonResponse(http.StatusOK, map[string]int{"id": id})
		})
	httpmock.RegisterResponder(http.MethodGet, `=~^http://127\.0\.0\.1:8081/schemas/ids/\d+$`,
		func(r *http.Request) (*http.Response, error) {
			lookupHits.Add(1)
			id, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/schemas/ids/"))
			require.NoError(t, err)
			mu.Lock()
			schema, ok := schemas[id]
			mu.Unlock()
			if !ok {
				return httpmock.NewStringResponse(http.StatusNotFound, "Not Found"), nil
			}
			return httpmock.NewJsonResponse(http.StatusOK, map[string]any{"id": id, "schema": schema})
		})

	cfg := common.NewConfig(config.ProtocolDebeziumAvro)
	cfg.AvroConfluentSchemaRegistry = registryURL
	cfg.EnableTiDBExtension = true
	cfg.AvroEnableWatermark = true

	encMgr, err := schemamanager.NewConfluentSchemaManager(t.Context(), registryURL, nil)
	require.NoError(t, err)
	defer encMgr.Close()

	encoder, err := debezium.NewAvroBatchEncoder(cfg, "db1", encMgr)
	require.NoError(t, err)
	msg, err := encoder.EncodeCheckpointEvent(12345)
	require.NoError(t, err)
	require.NotNil(t, msg)

	upstreamURI, err := url.Parse("kafka://127.0.0.1:9092/test-topic")
	require.NoError(t, err)
	opts := Options{
		CodecConfig:   cfg,
		ReplicaConfig: config.GetDefaultReplicaConfig(),
		UpstreamURI:   upstreamURI,
	}

	group := NewGroup(t.Context(), opts)
	require.NotNil(t, group.schemaManager)

	runCtx, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() {
		runDone <- group.Run(runCtx)
	}()
	defer func() {
		cancel()
		require.ErrorIs(t, <-runDone, context.Canceled)
		group.Close()
	}()

	var payloads []*source.Payload
	for p := range int32(20) {
		payloads = append(payloads, &source.Payload{Partition: p, Key: msg.Key, Value: msg.Value})
	}
	require.NoError(t, group.Add(t.Context(), payloads))

	for range payloads {
		f := <-group.Output()
		require.NoError(t, f.Ready(t.Context()))
		require.Len(t, f.Events, 2)
		resolved := f.Events[0].(*event.ResolvedEvent)
		require.Equal(t, uint64(12345), resolved.ResolvedTs)
		require.Nil(t, f.Events[1])
	}
	// Shared SchemaManager singleflights and caches lookup across all decoders.
	require.Equal(t, int32(len(schemas)), lookupHits.Load())
}

func TestDecodeGroupSimpleCrossPartitionWakeupOutOfOrderAndOver128(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	bootDDL1 := helper.DDL2Event("create table test.t1(id int primary key, v int)")
	dml1 := helper.DML2Event("test", "t1", "insert into test.t1 values (1, 10)")
	bootDDL2 := helper.DDL2Event("create table test.t2(id int primary key, v int)")
	dml2 := helper.DML2Event("test", "t2", "insert into test.t2 values (2, 20)")

	bootDDL1.TableInfo.UpdateTS = 50
	bootDDL2.TableInfo.UpdateTS = 60
	dml1.TableInfo.UpdateTS = 50
	dml2.TableInfo.UpdateTS = 60

	cfg := common.NewConfig(config.ProtocolSimple)
	enc, err := simple.NewEncoder(cfg, nil)
	require.NoError(t, err)

	bootMsg1, err := enc.EncodeDDLEvent(codec.NewBootstrapDDLEvent(bootDDL1.TableInfo))
	require.NoError(t, err)
	bootMsg2, err := enc.EncodeDDLEvent(codec.NewBootstrapDDLEvent(bootDDL2.TableInfo))
	require.NoError(t, err)

	row1, ok := dml1.GetNextRow()
	require.True(t, ok)
	require.NoError(t, enc.AppendRowChangedEvent(t.Context(), "topic", &event.RowEvent{
		TableInfo:      dml1.TableInfo,
		CommitTs:       200,
		Event:          row1,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}))
	dmlMsgs1 := enc.Build()
	require.Len(t, dmlMsgs1, 1)

	row2, ok := dml2.GetNextRow()
	require.True(t, ok)
	require.NoError(t, enc.AppendRowChangedEvent(t.Context(), "topic", &event.RowEvent{
		TableInfo:      dml2.TableInfo,
		CommitTs:       210,
		Event:          row2,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}))
	dmlMsgs2 := enc.Build()
	require.Len(t, dmlMsgs2, 1)

	upstreamURI, err := url.Parse("kafka://127.0.0.1:9092/topic")
	require.NoError(t, err)
	group := NewGroup(t.Context(), Options{
		CodecConfig:   cfg,
		ReplicaConfig: config.GetDefaultReplicaConfig(),
		UpstreamURI:   upstreamURI,
	})

	runCtx, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() {
		runDone <- group.Run(runCtx)
	}()
	defer func() {
		cancel()
		require.ErrorIs(t, <-runDone, context.Canceled)
		group.Close()
	}()

	const t1Count = 150
	const t2Count = 2
	dmlPayloads := make([]*source.Payload, 0, t1Count+t2Count)
	for i := range t1Count {
		dmlPayloads = append(dmlPayloads, &source.Payload{
			Partition: 0,
			Offset:    int64(i),
			Value:     dmlMsgs1[0].Value,
		})
	}
	for i := range t2Count {
		dmlPayloads = append(dmlPayloads, &source.Payload{
			Partition: 1,
			Offset:    int64(200 + i),
			Value:     dmlMsgs2[0].Value,
		})
	}

	// Submit >128 missing-schema DMLs concurrently while draining Output() sequentially;
	// every Future must become Ready with a Pending placeholder before any Bootstrap arrives.
	addErr := make(chan error, 1)
	go func() {
		addErr <- group.Add(t.Context(), dmlPayloads)
	}()

	t1Pending := make(map[uint64]int64, t1Count)
	t2Pending := make(map[uint64]int64, t2Count)
	for range t1Count + t2Count {
		f := <-group.Output()
		require.NoError(t, f.Ready(t.Context()))
		require.Equal(t, []event.Event{nil}, f.Events)
		require.Len(t, f.Pending, 1)
		if f.Payload.Partition == 0 {
			require.Equal(t, uint64(200), f.Pending[0].CommitTs)
			t1Pending[f.Pending[0].ID] = f.Payload.Offset
		} else {
			require.Equal(t, uint64(210), f.Pending[0].CommitTs)
			t2Pending[f.Pending[0].ID] = f.Payload.Offset
		}
	}
	require.NoError(t, <-addErr)

	// Send Bootstrap(t2) on partition 2 out of order, then Bootstrap(t1) on partition 3.
	require.NoError(t, group.Add(t.Context(), []*source.Payload{
		{Partition: 2, Value: bootMsg2.Value},
		{Partition: 3, Value: bootMsg1.Value},
	}))

	fBoot2 := <-group.Output()
	require.NoError(t, fBoot2.Ready(t.Context()))
	require.Equal(t, []event.Event{nil}, fBoot2.Events)
	require.Len(t, fBoot2.Resolved, t2Count)
	for _, r := range fBoot2.Resolved {
		expectedOffset, ok := t2Pending[r.ID]
		require.True(t, ok)
		require.Equal(t, int32(1), r.Partition)
		require.Equal(t, expectedOffset, r.Offset)
		r.DML.PostFlush()
		delete(t2Pending, r.ID)
	}
	require.Empty(t, t2Pending)

	fBoot1 := <-group.Output()
	require.NoError(t, fBoot1.Ready(t.Context()))
	require.Equal(t, []event.Event{nil}, fBoot1.Events)
	require.Len(t, fBoot1.Resolved, t1Count)
	for _, r := range fBoot1.Resolved {
		expectedOffset, ok := t1Pending[r.ID]
		require.True(t, ok)
		require.Equal(t, int32(0), r.Partition)
		require.Equal(t, expectedOffset, r.Offset)
		r.DML.PostFlush()
		delete(t1Pending, r.ID)
	}
	require.Empty(t, t1Pending)

	group.waitMu.Lock()
	require.Empty(t, group.waiting)
	group.waitMu.Unlock()
}

func TestDecodeGroupSimpleDDLWakesPreAndPostSchemaWaiters(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	preDDL := helper.DDL2Event("create table test.t1(id int primary key, v int)")
	preDML := helper.DML2Event("test", "t1", "insert into test.t1 values (1, 10)")
	alterDDL := helper.DDL2Event("alter table test.t1 add column c int")
	postDML := helper.DML2Event("test", "t1", "insert into test.t1 values (2, 20, 30)")

	preDDL.TableInfo.UpdateTS = 100
	preDML.TableInfo.UpdateTS = 100
	alterDDL.FinishedTs = 200
	alterDDL.TableInfo.UpdateTS = 200
	alterDDL.MultipleTableInfos = []*commonType.TableInfo{alterDDL.TableInfo, preDDL.TableInfo}
	postDML.TableInfo.UpdateTS = 200

	cfg := common.NewConfig(config.ProtocolSimple)
	enc, err := simple.NewEncoder(cfg, nil)
	require.NoError(t, err)

	preRow, ok := preDML.GetNextRow()
	require.True(t, ok)
	require.NoError(t, enc.AppendRowChangedEvent(t.Context(), "topic", &event.RowEvent{
		TableInfo:      preDML.TableInfo,
		CommitTs:       180,
		Event:          preRow,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}))
	preMsgs := enc.Build()
	require.Len(t, preMsgs, 1)

	postRow, ok := postDML.GetNextRow()
	require.True(t, ok)
	require.NoError(t, enc.AppendRowChangedEvent(t.Context(), "topic", &event.RowEvent{
		TableInfo:      postDML.TableInfo,
		CommitTs:       210,
		Event:          postRow,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}))
	postMsgs := enc.Build()
	require.Len(t, postMsgs, 1)

	alterMsg, err := enc.EncodeDDLEvent(alterDDL)
	require.NoError(t, err)

	upstreamURI, err := url.Parse("kafka://127.0.0.1:9092/topic")
	require.NoError(t, err)
	group := NewGroup(t.Context(), Options{
		CodecConfig:   cfg,
		ReplicaConfig: config.GetDefaultReplicaConfig(),
		UpstreamURI:   upstreamURI,
	})

	runCtx, cancel := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() {
		runDone <- group.Run(runCtx)
	}()
	defer func() {
		cancel()
		require.ErrorIs(t, <-runDone, context.Canceled)
		group.Close()
	}()

	require.NoError(t, group.Add(t.Context(), []*source.Payload{
		{Partition: 0, Offset: 10, Value: preMsgs[0].Value},
		{Partition: 1, Offset: 20, Value: postMsgs[0].Value},
	}))

	fPre := <-group.Output()
	require.NoError(t, fPre.Ready(t.Context()))
	require.Len(t, fPre.Pending, 1)

	fPost := <-group.Output()
	require.NoError(t, fPost.Ready(t.Context()))
	require.Len(t, fPost.Pending, 1)

	require.NoError(t, group.Add(t.Context(), []*source.Payload{
		{Partition: 2, Offset: 30, Value: alterMsg.Value},
	}))

	// First ALTER arrival wakes both PreTableSchema and post-DDL waiters and forwards the DDL.
	fAlter := <-group.Output()
	require.NoError(t, fAlter.Ready(t.Context()))
	require.Zero(t, fAlter.Marker)
	require.Len(t, fAlter.Events, 2)
	forwardedDDL := fAlter.Events[0].(*event.DDLEvent)
	require.Equal(t, alterDDL.Query, forwardedDDL.Query)
	require.Equal(t, uint64(200), forwardedDDL.GetCommitTs())
	require.Nil(t, fAlter.Events[1])
	require.Len(t, fAlter.Resolved, 2)

	resolvedByID := make(map[uint64]ResolvedDML, 2)
	for _, r := range fAlter.Resolved {
		resolvedByID[r.ID] = r
		r.DML.PostFlush()
	}
	wokenPre := resolvedByID[fPre.Pending[0].ID]
	require.Equal(t, int32(0), wokenPre.Partition)
	require.Equal(t, int64(10), wokenPre.Offset)
	require.Equal(t, uint64(180), wokenPre.DML.CommitTs)
	wokenPost := resolvedByID[fPost.Pending[0].ID]
	require.Equal(t, int32(1), wokenPost.Partition)
	require.Equal(t, int64(20), wokenPost.Offset)
	require.Equal(t, uint64(210), wokenPost.DML.CommitTs)

	// Duplicate ALTER on another partition drops the DDL while retaining commit-ts as Marker.
	require.NoError(t, group.Add(t.Context(), []*source.Payload{
		{Partition: 3, Offset: 40, Value: alterMsg.Value},
	}))
	fDup := <-group.Output()
	require.NoError(t, fDup.Ready(t.Context()))
	require.Equal(t, []event.Event{nil}, fDup.Events)
	require.Equal(t, uint64(200), fDup.Marker)
	require.Empty(t, fDup.Resolved)

	group.waitMu.Lock()
	require.Empty(t, group.waiting)
	group.waitMu.Unlock()
}
