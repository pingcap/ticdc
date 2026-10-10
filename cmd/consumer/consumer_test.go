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

package main

import (
	"context"
	"net/url"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/open"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestKafkaReaderSmallMessageLimit(t *testing.T) {
	const topic = "small-message-limit"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	t.Cleanup(cluster.Close)
	for _, query := range []string{
		"protocol=canal-json&enable-tidb-extension=true",
		"protocol=open-protocol",
		"protocol=simple",
		"protocol=simple&encoding-format=avro",
		"protocol=debezium&enable-tidb-extension=true",
	} {
		t.Run(query, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			upstreamURI := &url.URL{Scheme: "kafka", Host: cluster.ListenAddrs()[0], Path: topic, RawQuery: query + "&max-message-bytes=262144"}
			input, err := newKafkaReader(ctx, upstreamURI, "small-message-limit", &memoryUsage{}, config.GetDefaultReplicaConfig())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, input.Close()) })
			decoding, err := newDecodeConfig(ctx, upstreamURI, "UTC", config.GetDefaultReplicaConfig())
			require.NoError(t, err)
			a, err := newAssembler(decoding, config.GetDefaultReplicaConfig(), input.memory)
			require.NoError(t, err)
			require.Equal(t, decoding.codec.Protocol, a.protocol)
			if !strings.HasPrefix(query, "protocol=canal-json") {
				return
			}
			// The topic limit can grow while this reader keeps the original URI.
			producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ProducerBatchMaxBytes(2<<20))
			require.NoError(t, err)
			t.Cleanup(producer.Close)
			payload := strings.Repeat("x", 1<<20)
			value := []byte(`{"database":"test","table":"t","pkNames":["id"],"isDdl":false,"type":"INSERT","sqlType":{"id":4,"v":12},"mysqlType":{"id":"int","v":"longtext"},"data":[{"id":"1","v":"` + payload + `"}],"_tidb":{"commitTs":10}}`)
			require.NoError(t, producer.ProduceSync(ctx,
				&kgo.Record{Topic: topic, Value: value},
				&kgo.Record{Topic: topic, Value: []byte(`{"isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":10}}`)},
			).FirstErr())
			result, err := a.next(ctx, input)
			require.NoError(t, err)
			require.NotNil(t, result.dml)
			require.EqualValues(t, 10, result.dml.CommitTs)
			require.Equal(t, payload, result.dml.Rows.GetRow(0).GetString(1))
		})
	}
}

func TestKafkaConfirmationKeepsConcurrentInput(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	const topic = "confirm-prefix"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	t.Cleanup(cluster.Close)
	r, err := newKafkaReader(ctx, &url.URL{Scheme: "kafka", Host: cluster.ListenAddrs()[0], Path: topic}, topic, &memoryUsage{}, config.GetDefaultReplicaConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	require.NoError(t, r.client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("a")}, &kgo.Record{Topic: topic, Value: []byte("b")}).FirstErr())
	first, err := r.Read(ctx)
	require.NoError(t, err)
	r.memory.decoded(first.record, 128)
	committing, release := make(chan struct{}), make(chan struct{})
	cluster.ControlKey(int16(kmsg.OffsetCommit), func(kmsg.Request) (kmsg.Response, error, bool) {
		close(committing)
		select {
		case <-release:
		case <-ctx.Done():
		}
		cluster.DropControl()
		return nil, nil, false
	})
	confirmed := make(chan error, 1)
	wg.Go(func() { confirmed <- r.Confirm(ctx) })
	select {
	case <-committing:
	case <-ctx.Done():
		t.Fatal("confirmation did not reach the broker")
	}
	read := make(chan *readData, 1)
	readErrors := make(chan error, 1)
	wg.Go(func() {
		data, err := r.Read(ctx)
		if err != nil {
			readErrors <- err
			return
		}
		read <- data
	})
	var second *readData
	select {
	case second = <-read:
	case err := <-readErrors:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("reading was blocked by confirmation")
	}
	// A broker commit must also leave the downstream dispatch loop runnable.
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("t")})
	dml := event.NewDMLEvent(common.NewDispatcherID(), 1, 0, 10, table)
	dml.Length = 1
	written := make(chan struct{})
	downstream := mock.NewMockSink(gomock.NewController(t))
	downstream.EXPECT().AddDMLEvent(dml).Do(func(dml *event.DMLEvent) { dml.PostFlush(); close(written) })
	c := &consumer{reader: r, assembler: &assembler{}, writer: &writer{memory: r.memory, downstream: downstream}}
	results := make(chan *writeEvent, 1)
	writeErrors := make(chan error, 1)
	wg.Go(func() { writeErrors <- c.write(ctx, results) })
	results <- &writeEvent{dml: dml}
	select {
	case <-written:
	case <-ctx.Done():
		t.Fatal("downstream dispatch was blocked by confirmation")
	}
	close(release)
	require.NoError(t, <-confirmed)
	require.Equal(t, []*ack{second.record}, r.records[0])
	require.EqualValues(t, 1, r.memory.confirmed.Load())
	require.EqualValues(t, 1, second.record.refs.Load())
	cancel()
	require.ErrorIs(t, <-writeErrors, context.Canceled)
}

func TestSimpleCacheConfirmsEachInputIndependently(t *testing.T) {
	memory := &memoryUsage{}
	a := &assembler{memory: memory, protocol: config.ProtocolSimple, codecConfig: codecCommon.NewConfig(config.ProtocolSimple), streams: make(map[int32]*decodeStream)}
	r := &pulsarReader{memory: memory}
	var records []*ack
	for _, table := range []string{"a", "b"} {
		value := []byte(`{"version":1,"database":"test","table":"` + table + `","tableID":1,"type":"INSERT","commitTs":101,"schemaVersion":100,"data":{"id":"1"}}`)
		record, err := memory.newAck(t.Context(), int64(len(value)+256))
		require.NoError(t, err)
		records = append(records, record)
		require.NoError(t, a.decodeMessages(t.Context(), &readData{value: value, record: record, retainedBytes: 256, dmlBoundary: &readBoundary{reached: true}}, r))
	}
	for index, table := range []string{"a", "b"} {
		value := []byte(`{"version":1,"type":"BOOTSTRAP","commitTs":100,"tableSchema":{"schema":"test","table":"` + table + `","tableID":1,"version":100,"columns":[{"name":"id","dataType":{"mysqlType":"bigint","charset":"binary","collate":"binary","length":20}}]}}`)
		record, err := memory.newAck(t.Context(), int64(len(value)+256))
		require.NoError(t, err)
		require.NoError(t, a.decodeMessages(t.Context(), &readData{value: value, record: record, retainedBytes: 256, dmlBoundary: &readBoundary{reached: true}}, r))
		item := a.nextReady(0)
		require.NotNil(t, item)
		require.Equal(t, table, item.dml.TableInfo.GetTableName())
		item.dml.PostFlush()
		memory.release(item.bytes)
		require.Zero(t, records[index].refs.Load())
		if index == 0 {
			require.EqualValues(t, 1, records[1].refs.Load())
		}
	}
	require.Empty(t, a.streams[0].cachedRecords)
}

func TestStorageReaderSmallMessageLimit(t *testing.T) {
	for _, protocol := range []string{"csv", "canal-json"} {
		t.Run(protocol, func(t *testing.T) {
			replicaConfig := config.GetDefaultReplicaConfig()
			replicaConfig.Sink.Protocol = new(protocol)
			upstreamURI := &url.URL{Scheme: "file", Path: t.TempDir(), RawQuery: "max-message-bytes=262144"}
			input, err := newStorageReader(t.Context(), upstreamURI, replicaConfig, &memoryUsage{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, input.Close()) })
			decoding, err := newDecodeConfig(t.Context(), upstreamURI, "UTC", replicaConfig)
			require.NoError(t, err)
			a, err := newAssembler(decoding, replicaConfig, input.memory)
			require.NoError(t, err)
			require.Equal(t, decoding.codec.Protocol, a.protocol)
		})
	}
}

func TestStorageReaderStreamsFileRanges(t *testing.T) {
	for name, end := range map[string]uint64{"completeGroup": 2, "largeBacklog": maxRecords + 1} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			replicaConfig := config.GetDefaultReplicaConfig()
			replicaConfig.Sink.Protocol = new("csv")
			replicaConfig.Sink.DateSeparator = new(config.DateSeparatorNone)
			r, err := newStorageReader(ctx, &url.URL{Scheme: "file", Path: t.TempDir()}, replicaConfig, &memoryUsage{})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, r.Close()) })
			schema := &cloudstorage.SchemaFile{Schema: "test", Table: "t", TableVersion: 1}
			require.NoError(t, r.storage.WriteFile(ctx, schema.Path(false, 0), schema.Marshal()))
			key := cloudstorage.DMLPathKey{SchemaPathKey: cloudstorage.SchemaPathKey{Schema: "test", Table: "t", TableVersion: 1}}
			index := cloudstorage.FileIndex{Idx: end}
			require.NoError(t, r.storage.WriteFile(ctx, key.GenerateIndexFilePath(index.FileIndexKey), []byte(path.Base(key.GenerateDMLFilePath(&index, r.fileExtension, r.fileIndexWidth)))))
			for id := uint64(1); id <= 2; id++ {
				index.Idx = id
				require.NoError(t, r.storage.WriteFile(ctx, key.GenerateDMLFilePath(&index, r.fileExtension, r.fileIndexWidth), []byte("row")))
			}
			for id := uint64(1); id <= 2; id++ {
				data, err := r.Read(ctx)
				require.NoError(t, err)
				require.Equal(t, id, r.fileIndices[key][index.FileIndexKey])
				require.NotNil(t, data.table)
				require.NotNil(t, data.group)
				require.Equal(t, rowFormat, data.format)
				require.Equal(t, []byte("row"), data.value)
			}
			if end == 2 {
				data, err := r.Read(ctx)
				require.NoError(t, err)
				require.True(t, data.groupEnd)
				require.Nil(t, data.record)
			}
		})
	}
}

func TestKafkaReaderSplitRenameDDL(t *testing.T) {
	codecConfig := codecCommon.NewConfig(config.ProtocolOpen)
	encoder, err := open.NewBatchEncoder(codecConfig, nil)
	require.NoError(t, err)
	// A rename chain can carry the same final table name in different messages.
	ddls := []*event.DDLEvent{
		{FinishedTs: 10, SchemaName: "common", TableName: "test4", Type: byte(timodel.ActionRenameTable), Query: "RENAME TABLE `common_1`.`test1` TO `common`.`test2`;"},
		{FinishedTs: 10, SchemaName: "common_1", TableName: "test4", Type: byte(timodel.ActionRenameTable), Query: "RENAME TABLE `common`.`test2` TO `common_1`.`test3`;"},
		{FinishedTs: 10, SchemaName: "common_1", TableName: "test1", Type: byte(timodel.ActionRenameTable), Query: "RENAME TABLE `common`.`test4` TO `common_1`.`test1`;"},
		{FinishedTs: 10, SchemaName: "common", TableName: "test4", Type: byte(timodel.ActionRenameTable), Query: "RENAME TABLE `common_1`.`test3` TO `common`.`test4`;"},
		{FinishedTs: 11, SchemaName: "common_1", TableName: "test5", Type: byte(timodel.ActionCreateTable), Query: "CREATE TABLE `common_1`.`test5` (id int primary key);"},
	}
	watermark, err := encoder.EncodeCheckpointEvent(11)
	require.NoError(t, err)
	// Partition zero can resume halfway through the DDL job, or after it,
	// while other partitions still replay every copy.
	for _, startIndex := range []int{0, 1, len(ddls)} {
		client, err := kgo.NewClient()
		require.NoError(t, err)
		t.Cleanup(client.Close)
		memory := &memoryUsage{}
		r := &kafkaReader{client: client, memory: memory, protocol: config.ProtocolOpen, progress: make(map[int32]*kafkaProgress), records: make(map[int32][]*ack), offsets: make(map[*ack]int64), ddlCopies: make(map[uint64]map[int32][]*ack)}
		c := &assembler{memory: memory, protocol: config.ProtocolOpen, streams: make(map[int32]*decodeStream)}
		for partitionID := range int32(2) {
			decoder, err := open.NewDecoder(t.Context(), int(partitionID), codecConfig, nil)
			require.NoError(t, err)
			r.progress[partitionID] = &kafkaProgress{}
			c.streams[partitionID] = &decodeStream{decoder: decoder}
		}
		// Noncanonical copies may arrive first and are never executed.
		for _, partitionID := range []int32{1, 0} {
			input := ddls
			if partitionID == 0 {
				input = input[startIndex:]
			}
			for offset, ddl := range input {
				message, err := encoder.EncodeDDLEvent(ddl)
				require.NoError(t, err)
				record, err := memory.newAck(t.Context(), int64(len(message.Key)+len(message.Value)+128))
				require.NoError(t, err)
				r.offsets[record] = int64(offset)
				r.records[partitionID] = append(r.records[partitionID], record)
				require.NoError(t, c.decodeMessages(t.Context(), &readData{stream: partitionID, key: message.Key, value: message.Value, record: record, retainedBytes: 128}, r))
			}
			record, err := memory.newAck(t.Context(), int64(len(watermark.Key)+len(watermark.Value)+128))
			require.NoError(t, err)
			r.offsets[record] = int64(len(input))
			r.records[partitionID] = append(r.records[partitionID], record)
			require.NoError(t, c.decodeMessages(t.Context(), &readData{stream: partitionID, key: watermark.Key, value: watermark.Value, record: record, retainedBytes: 128}, r))
		}
		downstream := mock.NewMockSink(gomock.NewController(t))
		w := &writer{downstream: downstream, memory: memory}
		for index, ddl := range ddls[startIndex:] {
			result, err := c.next(t.Context(), r)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, ddl.Query, result.ddl.Query)
			require.EqualValues(t, 1, r.records[0][index].refs.Load())
			downstream.EXPECT().FlushDMLBeforeBlock(result.ddl).Return(nil)
			downstream.EXPECT().WriteBlockEvent(result.ddl).Return(nil)
			require.NoError(t, w.writeDDL(t.Context(), result))
			require.Zero(t, r.records[0][index].refs.Load())
			for _, record := range r.records[1][:len(ddls)] {
				require.EqualValues(t, 1, record.refs.Load())
			}
		}
		result, err := c.next(t.Context(), r)
		require.NoError(t, err)
		require.True(t, result.hasWatermark)
		require.EqualValues(t, 11, result.watermark)
		require.NotNil(t, result.onFlush)
		result.onFlush()
		for _, records := range r.records {
			for _, record := range records {
				require.Zero(t, record.refs.Load())
			}
		}
		remainingBytes := int64(0)
		for record := range r.offsets {
			remainingBytes += record.memory.Load()
		}
		require.Equal(t, remainingBytes, memory.bytes.Load())
	}
}

func TestReaderDDLNormalization(t *testing.T) {
	memory := &memoryUsage{}
	buffer := &assembler{memory: memory}
	first, err := buffer.memory.newAck(t.Context(), 128)
	require.NoError(t, err)
	ddl := &event.DDLEvent{Type: byte(timodel.ActionAddColumn), SchemaName: "test", TableName: "t", Query: "alter table t add column v int", FinishedTs: 10}
	require.NoError(t, buffer.queueDDL(t.Context(), ddl, first))
	result := buffer.nextReady(9)
	require.Nil(t, result)
	first.refs.Add(-1)
	beforeDDL := &event.DMLEvent{CommitTs: 10}
	afterDDL := &event.DMLEvent{CommitTs: 11}
	buffer.pendingDML = []*writeEvent{{dml: beforeDDL}, {dml: afterDDL}}
	result = buffer.nextReady(10)
	require.Same(t, beforeDDL, result.dml)
	result = buffer.nextReady(10)
	require.Same(t, ddl, result.ddl)
	require.EqualValues(t, 1, first.refs.Load())

	downstream := mock.NewMockSink(gomock.NewController(t))
	gomock.InOrder(
		downstream.EXPECT().FlushDMLBeforeBlock(ddl).Return(nil),
		downstream.EXPECT().WriteBlockEvent(ddl).Return(nil),
	)
	w := &writer{downstream: downstream, memory: memory}
	require.NoError(t, w.writeDDL(t.Context(), result))
	require.Zero(t, first.refs.Load())
	result = buffer.nextReady(11)
	require.Same(t, afterDDL, result.dml)
	require.EqualValues(t, 128, memory.bytes.Load())
}

func TestKafkaReaderWatermark(t *testing.T) {
	c := &kafkaReader{progress: map[int32]*kafkaProgress{
		0: {watermark: 20, hasWatermark: true},
		1: {watermark: 10, hasWatermark: true},
	}}
	watermark, ready := c.globalWatermark()
	require.True(t, ready)
	require.EqualValues(t, 10, watermark)
	c.progress[1].hasWatermark = false
	_, ready = c.globalWatermark()
	require.False(t, ready)
	c.progress[1].hasWatermark = true
	c.progress[1].needsMoreInput = true
	_, ready = c.globalWatermark()
	require.False(t, ready)
	c.controls = []*readControl{{watermark: 20}}
	progress, err := c.Advance(t.Context(), readFeedback{})
	require.NoError(t, err)
	require.True(t, progress.needsMoreInput)
	require.False(t, progress.hasWatermark)
	require.Nil(t, progress.control)
}

func TestPulsarControlsUseTopicProgress(t *testing.T) {
	memory := &memoryUsage{}
	r := &pulsarReader{
		memory: memory, partitionIDs: map[string]int32{"test-partition-0": 0, "test-partition-1": 1},
	}
	decoding, err := newDecodeConfig(t.Context(), &url.URL{Scheme: "pulsar", Path: "test", RawQuery: "enable-tidb-extension=true"}, "UTC", config.GetDefaultReplicaConfig())
	require.NoError(t, err)
	a, err := newAssembler(decoding, config.GetDefaultReplicaConfig(), memory)
	require.NoError(t, err)
	for _, watermark := range []string{"100", "90"} {
		value := []byte(`{"isDdl":false,"type":"TIDB_WATERMARK","_tidb":{"watermarkTs":` + watermark + `}}`)
		record, err := memory.newAck(t.Context(), int64(len(value)+256))
		require.NoError(t, err)
		require.NoError(t, a.decodeMessages(t.Context(), &readData{value: value, record: record, retainedBytes: 256, ddlOrder: commitOrder}, r))
		require.EqualValues(t, 100, r.watermark)
		// Idle partitions do not require a broker snapshot or a new message.
		result, err := a.next(t.Context(), r)
		require.NoError(t, err)
		require.True(t, result.hasWatermark)
		require.EqualValues(t, 1, record.refs.Load())
		result.onFlush()
		require.Zero(t, record.refs.Load())
		memory.confirm(record)
	}
	require.Zero(t, memory.used())
	queries := []string{
		`{"database":"test","table":"a","isDdl":true,"type":"ALTER","sql":"ALTER TABLE test.a ADD COLUMN x INT","_tidb":{"commitTs":200}}`,
		`{"database":"test","table":"a","isDdl":true,"type":"ALTER","sql":"ALTER TABLE test.a ADD COLUMN y INT","_tidb":{"commitTs":100}}`,
		`{"database":"test","table":"a","isDdl":true,"type":"ALTER","sql":"ALTER TABLE test.a ADD COLUMN z INT","_tidb":{"commitTs":200}}`,
	}
	for _, query := range queries {
		record, err := memory.newAck(t.Context(), int64(len(query)+256))
		require.NoError(t, err)
		require.NoError(t, a.decodeMessages(t.Context(), &readData{value: []byte(query), record: record, retainedBytes: 256, ddlOrder: commitOrder}, r))
	}
	require.EqualValues(t, 100, a.nextReady(100).ddl.GetCommitTs())
	require.Nil(t, a.nextReady(100))
	// Equal timestamps retain their original order and are never merged.
	require.Contains(t, a.nextReady(200).ddl.Query, "COLUMN x")
	require.Contains(t, a.nextReady(200).ddl.Query, "COLUMN z")
	require.Empty(t, a.pendingDDL)
}

func TestKafkaReaderDDLWaitsForBufferedDML(t *testing.T) {
	dml := &event.DMLEvent{CommitTs: 20}
	ddl := &event.DDLEvent{FinishedTs: 30}
	copyRecord := &ack{}
	copyRecord.refs.Store(1)
	memory := &memoryUsage{}
	memory.bytes.Store(128)
	c := &assembler{
		memory:     memory,
		pendingDML: []*writeEvent{{dml: dml}},
		pendingDDL: []*writeEvent{{ddl: ddl}},
	}
	r := &kafkaReader{
		memory: memory, progress: map[int32]*kafkaProgress{0: {watermark: 40, hasWatermark: true}, 1: {watermark: 40, hasWatermark: true}},
		// An already delivered CREATE TABLE still has an unconfirmed copy.
		ddlCopies: map[uint64]map[int32][]*ack{10: {1: {copyRecord}}},
	}
	result, err := c.next(t.Context(), r)
	require.NoError(t, err)
	require.Same(t, dml, result.dml)
	require.EqualValues(t, 1, copyRecord.refs.Load())
	result, err = c.next(t.Context(), r)
	require.NoError(t, err)
	require.Same(t, ddl, result.ddl)
	result, err = c.next(t.Context(), r)
	require.NoError(t, err)
	require.True(t, result.hasWatermark)
	require.EqualValues(t, 40, result.watermark)
	require.EqualValues(t, 1, copyRecord.refs.Load())
	result.onFlush()
	require.Zero(t, copyRecord.refs.Load())
	require.Zero(t, memory.bytes.Load())
}

func TestInputCompletionAcrossBatches(t *testing.T) {
	memory := &memoryUsage{}
	buffer := &assembler{memory: memory}
	file, err := buffer.memory.newAck(t.Context(), 256)
	require.NoError(t, err)
	later, err := buffer.memory.newAck(t.Context(), 256)
	require.NoError(t, err)
	later.refs.Add(-1)
	file.refs.Add(2)
	firstDML, secondDML := &event.DMLEvent{}, &event.DMLEvent{}
	for _, dml := range []*event.DMLEvent{firstDML, secondDML} {
		dml.AddPostFlushFunc(func() {
			file.refs.Add(-1)
		})
	}
	first := &writeBatch{events: []*event.DMLEvent{firstDML}, done: make(chan bool)}
	second := &writeBatch{events: []*event.DMLEvent{secondDML}, done: make(chan bool)}
	input := &storageReader{memory: buffer.memory, records: []*ack{file, later}}
	w := &writer{memory: memory, inFlight: []*writeBatch{first, second}}
	// A later input cannot release positions past an incomplete file.
	w.finishBatches()
	require.NoError(t, input.Confirm(t.Context()))
	require.Len(t, input.records, 2)
	secondDML.PostFlush()
	close(second.done)
	w.finishBatches()
	require.NoError(t, input.Confirm(t.Context()))
	require.EqualValues(t, 2, file.refs.Load())
	firstDML.PostFlush()
	close(first.done)
	w.finishBatches()
	require.NoError(t, input.Confirm(t.Context()))
	require.EqualValues(t, 1, file.refs.Load())
	require.Len(t, input.records, 2)
	// Both batches are durable, but the decoder may still register more rows.
	file.refs.Add(-1)
	w.finishBatches()
	require.NoError(t, input.Confirm(t.Context()))
	require.Empty(t, input.records)
	require.EqualValues(t, 2, memory.confirmed.Load())
	require.Zero(t, memory.bytes.Load())
}

func TestWatermarkConfirmationWaitsForWrites(t *testing.T) {
	memory := &memoryUsage{}
	buffer := &assembler{memory: memory}
	record, err := buffer.memory.newAck(t.Context(), 128)
	require.NoError(t, err)
	input := &storageReader{memory: buffer.memory, records: []*ack{record}}
	batch := &writeBatch{items: []*writeEvent{{dml: &event.DMLEvent{CommitTs: 10}}}, done: make(chan bool)}
	w := &writer{
		memory: memory, inFlight: []*writeBatch{batch}, progressChanged: true,
		pendingWatermarks: []*writeEvent{{watermark: 10, onFlush: func() {
			record.refs.Add(-1)
		}}},
	}
	w.finishBatches()
	require.NoError(t, input.Confirm(t.Context()))
	require.EqualValues(t, 1, record.refs.Load())
	require.Len(t, input.records, 1)
	close(batch.done)
	require.NoError(t, w.waitBatch(t.Context(), batch))
	require.NoError(t, input.Confirm(t.Context()))
	require.Empty(t, input.records)
	require.Empty(t, w.pendingWatermarks)
	require.Zero(t, memory.bytes.Load())
}

func TestReadyDMLFlushDoesNotNeedAnotherWatermark(t *testing.T) {
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"),
		Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("v"), Offset: 0, State: timodel.StatePublic, FieldType: *types.NewFieldType(mysql.TypeLong)}},
	})
	dml := event.NewDMLEvent(common.NewDispatcherID(), 1, 0, 10, table)
	rows := chunk.NewChunkWithCapacity(table.GetFieldSlice(), 1)
	rows.AppendRow(chunk.MutRowFromValues(int64(1)).ToRow())
	dml.SetRows(rows)
	dml.RowTypes = []common.RowType{common.RowTypeInsert}
	dml.Length = 1
	memory := &memoryUsage{}
	buffer := &assembler{memory: memory}
	record, err := buffer.memory.newAck(t.Context(), 128)
	require.NoError(t, err)
	require.NoError(t, buffer.queueDML(t.Context(), dml, []*ack{record}, nil))
	record.refs.Add(-1)
	result := buffer.nextReady(10)
	input := &storageReader{memory: buffer.memory, records: []*ack{record}}
	downstream := mock.NewMockSink(gomock.NewController(t))
	downstream.EXPECT().AddDMLEvent(dml).Do(func(dml *event.DMLEvent) { dml.PostFlush() })
	w := &writer{downstream: downstream, memory: memory, mutations: make(map[mutationKey]*writeBatch)}
	require.NoError(t, w.consume(t.Context(), result))
	require.Len(t, w.pendingDML, 1)
	require.NoError(t, w.flushDML(t.Context()))
	require.Empty(t, w.pendingDML)
	w.finishBatches()
	require.NoError(t, input.Confirm(t.Context()))
	require.Empty(t, input.records)
	require.Zero(t, memory.bytes.Load())
}

func TestStorageProgressDoesNotRejectUnreadRows(t *testing.T) {
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("t")})
	dml := &event.DMLEvent{PhysicalTableID: 1, CommitTs: 10, TableInfo: table}
	w := &writer{memory: &memoryUsage{}, mutations: make(map[mutationKey]*writeBatch)}
	require.NoError(t, w.consume(t.Context(), &writeEvent{watermark: 20, tableID: 1, hasWatermark: true}))
	w.finishBatches()
	filtered, err := w.filterRows(t.Context(), dml, &writeBatch{})
	require.NoError(t, err)
	require.Same(t, dml, filtered)
	// A true topic-wide complete watermark retains the MQ replay cutoff.
	require.NoError(t, w.consume(t.Context(), &writeEvent{watermark: 20, tableID: 0, hasWatermark: true}))
	w.finishBatches()
	filtered, err = w.filterRows(t.Context(), dml, &writeBatch{})
	require.NoError(t, err)
	require.Nil(t, filtered)
}

func TestReaderBudgetIncludesInFlightMemory(t *testing.T) {
	memory := &memoryUsage{}
	record, err := memory.newAck(t.Context(), 128)
	require.NoError(t, err)
	require.NoError(t, memory.reserve(t.Context(), maxMemoryBytes-128))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, memory.wait(ctx), context.Canceled)
	ctx, cancel = context.WithTimeout(t.Context(), 50*time.Millisecond)
	require.ErrorIs(t, memory.wait(ctx), context.DeadlineExceeded)
	cancel()
	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	done := make(chan error, 1)
	wg.Go(func() { done <- memory.wait(ctx) })
	memory.release(128)
	require.NoError(t, <-done)
	wg.Wait()
	memory.decoded(record, 128)
	memory.confirm(record)
	memory.release(maxMemoryBytes - 256)
	require.Zero(t, memory.used())
}

func TestOversizedInputCanComplete(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	memory := &memoryUsage{}
	record, err := memory.newAck(ctx, maxMemoryBytes+(64<<20))
	require.NoError(t, err)
	require.Greater(t, memory.used(), int64(maxMemoryBytes))
	require.NoError(t, memory.reserve(ctx, 1024))
	memory.decoded(record, 256)
	require.Zero(t, record.refs.Load())
	memory.confirm(record)
	memory.release(1024)
	require.Zero(t, memory.used())
	cancel()
	require.ErrorIs(t, memory.reserve(ctx, 1), context.Canceled)
}

func TestKafkaReaderPrioritizesBlockedPartitions(t *testing.T) {
	c := &kafkaReader{
		memory: &memoryUsage{},
		progress: map[int32]*kafkaProgress{
			0: {hasWatermark: true, watermark: 20},
			1: {hasWatermark: true, watermark: 10},
			2: {},
		},
		sequences: make(map[int32]uint64),
		polled: map[int32][]*kgo.Record{
			0: {{Partition: 0}}, 1: {{Partition: 1}}, 2: {{Partition: 2}},
		},
	}
	require.EqualValues(t, 2, c.nextPartition())
	c.progress[2].hasWatermark = true
	c.progress[2].watermark = 10
	c.sequences[1] = 1
	require.EqualValues(t, 2, c.nextPartition())
	c.sequences[2] = 2
	require.EqualValues(t, 1, c.nextPartition())
	c.progress[0].needsMoreInput = true
	require.EqualValues(t, 0, c.nextPartition())
	c.progress[0].needsMoreInput = false
	c.boundary = &readBoundary{}
	c.positions = map[int32]int64{0: 10, 1: 0, 2: 10}
	c.targets = map[int32]int64{0: 10, 1: 5, 2: 10}
	require.EqualValues(t, 1, c.nextPartition())
	require.NoError(t, c.memory.reserve(t.Context(), 3*128))
	c.boundaryDDL = 100
	c.positions[1] = 1
	c.progress[1].ddlTs = 99
	c.advanceBoundary()
	require.False(t, c.boundary.reached)
	// Only this job's local copy closes its partition. The post-DDL backlog
	// up to offset five need not be read to execute the DDL.
	c.progress[1].ddlTs = 100
	c.advanceBoundary()
	require.True(t, c.boundary.reached)
	require.EqualValues(t, 1, c.positions[1])
	require.Zero(t, c.memory.used())
}

func TestKafkaInputBoundaryIsFixed(t *testing.T) {
	const topic = "input-boundary"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, topic))
	t.Cleanup(cluster.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	t.Cleanup(producer.Close)
	require.NoError(t, producer.ProduceSync(ctx,
		&kgo.Record{Topic: topic, Partition: 0, Value: []byte("control")},
		&kgo.Record{Topic: topic, Partition: 1, Value: []byte("data")},
	).FirstErr())
	r, err := newKafkaReader(ctx, &url.URL{Scheme: "kafka", Host: cluster.ListenAddrs()[0], Path: topic}, "input-boundary", &memoryUsage{}, config.GetDefaultReplicaConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })
	boundary, err := r.capture(ctx, nil, 0)
	require.NoError(t, err)
	require.False(t, boundary.reached)
	require.Equal(t, map[int32]int64{0: 1, 1: 1, 2: 0}, r.targets)
	// Traffic after capture must not extend the old window. Idle partition two
	// contributes its empty boundary without requiring a watermark or a message.
	require.NoError(t, producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("later")}).FirstErr())
	for !boundary.reached {
		data, err := r.Read(ctx)
		require.NoError(t, err)
		r.memory.decoded(data.record, 128)
		r.advanceBoundary()
	}
	require.EqualValues(t, 1, r.positions[0])
	require.EqualValues(t, 1, r.positions[1])
	require.NoError(t, r.Confirm(ctx))
	data, err := r.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "later", string(data.value))
	r.memory.decoded(data.record, 128)
	require.NoError(t, r.Confirm(ctx))
	require.Zero(t, r.memory.used())
}

func TestInputBoundariesFenceDDLAndWatermark(t *testing.T) {
	tableA := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("a")})
	tableB := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 2, Name: ast.NewCIStr("b")})
	boundary := &readBoundary{}
	before := &event.DMLEvent{PhysicalTableID: 1, CommitTs: 9, TableInfo: tableA}
	after := &event.DMLEvent{PhysicalTableID: 1, CommitTs: 11, TableInfo: tableA}
	independent := &event.DMLEvent{PhysicalTableID: 2, CommitTs: 12, TableInfo: tableB}
	ddl := &event.DDLEvent{SchemaName: "test", TableName: "a", FinishedTs: 10}
	a := &assembler{
		pendingDML: []*writeEvent{
			{dml: before, boundary: boundary}, {dml: after, boundary: boundary}, {dml: independent, boundary: boundary},
		},
	}
	// The candidate's control boundary has not been read: an earlier DDL can
	// still arrive on another partition, so no candidate row is handed off.
	require.Nil(t, a.nextReady(0))
	ddlBoundary := &readBoundary{}
	a.pendingDDL = []*writeEvent{{ddl: ddl, boundary: ddlBoundary}}
	boundary.reached = true
	require.Same(t, before, a.nextReady(0).dml)
	require.Same(t, independent, a.nextReady(0).dml)
	require.Nil(t, a.nextReady(0))
	require.True(t, a.hasPendingThrough(10, 0))
	ddlBoundary.reached = true
	require.Same(t, ddl, a.nextReady(0).ddl)
	require.False(t, a.hasPendingThrough(10, 0))
	require.Same(t, after, a.nextReady(0).dml)
	// Rows read during catch-up need their own later control boundary.
	a.pendingDML = []*writeEvent{{dml: after}}
	require.Nil(t, a.nextReady(0))
	require.True(t, a.hasPendingThrough(11, 0))
}

func TestOrderedReaderKeepsInputOrderAndDDLBoundary(t *testing.T) {
	waiting := &writeEvent{dml: &event.DMLEvent{PhysicalTableID: 1, CommitTs: 10}, boundary: &readBoundary{}}
	later := &writeEvent{dml: &event.DMLEvent{PhysicalTableID: 1, CommitTs: 20}, boundary: &readBoundary{reached: true}}
	other := &writeEvent{dml: &event.DMLEvent{PhysicalTableID: 2, CommitTs: 15}, boundary: &readBoundary{reached: true}}
	a := &assembler{pendingDML: []*writeEvent{waiting, later, other}}
	require.Same(t, other, a.nextReady(0))
	require.Nil(t, a.nextReady(0))
	waiting.boundary.reached = true
	require.Same(t, waiting, a.nextReady(0))
	require.Same(t, later, a.nextReady(0))
	// An independent row cannot skip a pre-DDL row whose proof is unfinished.
	preBoundary := &readBoundary{}
	preDDL := &writeEvent{dml: &event.DMLEvent{PhysicalTableID: 1, CommitTs: 10}, boundary: preBoundary}
	control := &writeEvent{ddl: &event.DDLEvent{FinishedTs: 15}, boundary: &readBoundary{reached: true}}
	ordered := &assembler{pendingDML: []*writeEvent{preDDL, other}, pendingDDL: []*writeEvent{control}}
	preDDL.dml.TableInfo = common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("a")})
	other.dml.TableInfo = common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 2, Name: ast.NewCIStr("b")})
	control.ddl.SchemaName, control.ddl.TableName = "test", "a"
	require.Same(t, other, ordered.nextReady(0))
	require.Nil(t, ordered.nextReady(0))
	preBoundary.reached = true
	require.Same(t, preDDL, ordered.nextReady(0))
	require.Same(t, control, ordered.nextReady(0))

	first := &event.DMLEvent{CommitTs: 20}
	second := &event.DMLEvent{CommitTs: 10}
	buffer := &assembler{
		memory:     &memoryUsage{},
		pendingDML: []*writeEvent{{dml: first, boundary: &readBoundary{reached: true}}, {dml: second, boundary: &readBoundary{reached: true}}},
	}
	require.Same(t, first, buffer.nextReady(0).dml)
	require.Same(t, second, buffer.nextReady(0).dml)
	ddl := &event.DDLEvent{FinishedTs: 15}
	buffer.pendingDML = []*writeEvent{{dml: first, boundary: &readBoundary{reached: true}}, {dml: second, boundary: &readBoundary{reached: true}}}
	buffer.pendingDDL = []*writeEvent{{ddl: ddl}}
	require.Same(t, second, buffer.nextReady(0).dml)
	require.Nil(t, buffer.nextReady(0))
	buffer.pendingDDL = nil
	buffer.pendingDML[0].boundary = nil
	require.Nil(t, buffer.nextReady(0))
	buffer.pendingDML = nil
	firstDDL, secondDDL := &event.DDLEvent{FinishedTs: 20}, &event.DDLEvent{FinishedTs: 10}
	require.NoError(t, buffer.queueDDL(t.Context(), firstDDL, &ack{}))
	require.NoError(t, buffer.queueDDL(t.Context(), secondDDL, &ack{}))
	require.Same(t, firstDDL, buffer.nextReady(^uint64(0)).ddl)
	require.Same(t, secondDDL, buffer.nextReady(^uint64(0)).ddl)
}

func TestReaderDDLArrivalOrder(t *testing.T) {
	tableA := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("a")})
	tableB := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 2, Name: ast.NewCIStr("b")})
	beforeA, afterA := &event.DMLEvent{PhysicalTableID: 1, CommitTs: 280, TableInfo: tableA}, &event.DMLEvent{PhysicalTableID: 1, CommitTs: 310, TableInfo: tableA}
	beforeB, afterB := &event.DMLEvent{PhysicalTableID: 2, CommitTs: 180, TableInfo: tableB}, &event.DMLEvent{PhysicalTableID: 2, CommitTs: 220, TableInfo: tableB}
	buffer := &assembler{
		memory: &memoryUsage{},
		pendingDML: []*writeEvent{
			{dml: beforeB, boundary: &readBoundary{reached: true}},
			{dml: afterB, boundary: &readBoundary{reached: true}},
			{dml: beforeA, boundary: &readBoundary{reached: true}},
			{dml: afterA, boundary: &readBoundary{reached: true}},
		},
	}
	ddlA := &event.DDLEvent{SchemaName: "test", TableName: "a", FinishedTs: 300}
	ddlB := &event.DDLEvent{SchemaName: "test", TableName: "b", FinishedTs: 200}
	require.NoError(t, buffer.queueDDL(t.Context(), ddlA, &ack{}))
	require.NoError(t, buffer.queueDDL(t.Context(), ddlB, &ack{}))
	// Prioritize the head's associated rows and execute it immediately. Every
	// later DDL still fences its own post-DDL rows.
	require.Same(t, beforeA, buffer.nextReady(300).dml)
	require.Same(t, ddlA, buffer.nextReady(300).ddl)
	require.Same(t, beforeB, buffer.nextReady(300).dml)
	require.Same(t, ddlB, buffer.nextReady(300).ddl)
	require.Same(t, afterB, buffer.nextReady(300).dml)
	require.Same(t, afterA, buffer.nextReady(300).dml)
}

func TestDDLScope(t *testing.T) {
	tableA := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("a")})
	tableB := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 2, Name: ast.NewCIStr("b")})
	tableC := common.NewTableInfo4Decoder("other", &timodel.TableInfo{ID: 3, Name: ast.NewCIStr("c")})
	a := &event.DMLEvent{PhysicalTableID: 10, TableInfo: tableA}
	b := &event.DMLEvent{PhysicalTableID: 20, TableInfo: tableB}
	c := &event.DMLEvent{PhysicalTableID: 30, TableInfo: tableC}
	ddl := &event.DDLEvent{SchemaName: "test", TableName: "a"}
	require.True(t, ddlBlocksTable(ddl, a))
	require.True(t, ddlBlocksTable(ddl, &event.DMLEvent{PhysicalTableID: 11, TableInfo: tableA}))
	require.False(t, ddlBlocksTable(ddl, b))
	ddl = &event.DDLEvent{SchemaName: "test", BlockedTables: &event.InfluencedTables{InfluenceType: event.InfluenceTypeDB}}
	require.True(t, ddlBlocksTable(ddl, a))
	require.True(t, ddlBlocksTable(ddl, b))
	require.False(t, ddlBlocksTable(ddl, c))
	ddl = &event.DDLEvent{SchemaName: "test", TableName: "a", ExtraSchemaName: "other", ExtraTableName: "c", Type: byte(timodel.ActionRenameTable)}
	require.True(t, ddlBlocksTable(ddl, a))
	require.True(t, ddlBlocksTable(ddl, c))
	require.False(t, ddlBlocksTable(ddl, b))
	ddl = &event.DDLEvent{
		SchemaName: "test", TableName: "a", Type: byte(timodel.ActionExchangeTablePartition),
		BlockedTableNames: []event.SchemaTableName{{SchemaName: "test", TableName: "a"}, {SchemaName: "other", TableName: "c"}},
	}
	require.True(t, ddlBlocksTable(ddl, a))
	require.True(t, ddlBlocksTable(ddl, c))
	require.False(t, ddlBlocksTable(ddl, b))
	ddl = &event.DDLEvent{SchemaName: "test", BlockedTables: &event.InfluencedTables{InfluenceType: event.InfluenceTypeAll}}
	require.True(t, ddlBlocksTable(ddl, a))
	require.True(t, ddlBlocksTable(ddl, b))
	require.True(t, ddlBlocksTable(ddl, c))
	assembly := &assembler{memory: &memoryUsage{}}
	for _, query := range []string{
		"RENAME TABLE test.a TO other.c",
		"ALTER TABLE test.a EXCHANGE PARTITION p0 WITH TABLE other.c",
		"CREATE TABLE other.c LIKE test.a",
	} {
		action := timodel.ActionRenameTable
		if strings.HasPrefix(query, "ALTER") {
			action = timodel.ActionExchangeTablePartition
		} else if strings.HasPrefix(query, "CREATE") {
			action = timodel.ActionCreateTable
		}
		ddl = &event.DDLEvent{SchemaName: "test", TableName: "unseen", Type: byte(action), Query: query}
		require.NoError(t, setDDLTableNames(ddl))
		require.NoError(t, assembly.queueDDL(t.Context(), ddl, &ack{}))
		require.True(t, ddlBlocksTable(ddl, a))
		require.True(t, ddlBlocksTable(ddl, c))
		require.False(t, ddlBlocksTable(ddl, b))
	}
	input := &kafkaReader{progress: map[int32]*kafkaProgress{0: {}, 1: {}, 2: {}, 3: {}}}
	router, err := eventrouter.NewEventRouter(config.GetDefaultReplicaConfig().Sink, true, "test", false, false)
	require.NoError(t, err)
	input.router = router
	partitionID, _, err := input.router.GetPartitionGenerator("test", "a").GeneratePartitionIndexAndKey(nil, 4, tableA, 10)
	require.NoError(t, err)
	selected, err := input.ddlPartitions(&event.DDLEvent{SchemaName: "test", TableName: "a"})
	require.NoError(t, err)
	require.Equal(t, []int32{partitionID}, selected)
	selected, err = input.ddlPartitions(&event.DDLEvent{SchemaName: "test"})
	require.NoError(t, err)
	require.Nil(t, selected)
	sinkConfig := config.GetDefaultReplicaConfig().Sink
	sinkConfig.DispatchRules = []*config.DispatchRule{{Matcher: []string{"*.*"}, PartitionRule: "index-value"}}
	input.router, err = eventrouter.NewEventRouter(sinkConfig, true, "test", false, false)
	require.NoError(t, err)
	selected, err = input.ddlPartitions(&event.DDLEvent{SchemaName: "test", TableName: "a"})
	require.NoError(t, err)
	require.Nil(t, selected)
}

func TestDDLCancellationLeavesInputUnconfirmed(t *testing.T) {
	memory := &memoryUsage{}
	buffer := &assembler{memory: memory}
	record, err := buffer.memory.newAck(t.Context(), 128)
	require.NoError(t, err)
	input := &storageReader{memory: buffer.memory, records: []*ack{record}}
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("t")})
	batch := &writeBatch{items: []*writeEvent{{dml: &event.DMLEvent{CommitTs: 9, TableInfo: table}}}, done: make(chan bool)}
	ddl := &event.DDLEvent{SchemaName: "test", TableName: "t", Query: "alter table t add column v int", FinishedTs: 10}
	result := &writeEvent{ddl: ddl, onFlush: func() {
		record.refs.Add(-1)
	}}
	downstream := mock.NewMockSink(gomock.NewController(t))
	w := &writer{downstream: downstream, memory: memory, inFlight: []*writeBatch{batch}, inFlightEvents: len(batch.items)}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, w.consume(ctx, result), context.Canceled)
	require.EqualValues(t, 1, record.refs.Load())
	close(batch.done)
	require.ErrorIs(t, input.Confirm(ctx), context.Canceled)
	require.Len(t, input.records, 1)
	require.EqualValues(t, 128, memory.bytes.Load())
}

func TestConsumerCancellationDuringStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	upstreamURI := &url.URL{Scheme: "file", Path: t.TempDir(), RawQuery: "protocol=csv"}
	options := &options{upstreamURI: upstreamURI.String(), downstreamURI: "blackhole://", timezone: "UTC", logLevel: "error"}
	var wg sync.WaitGroup
	done := make(chan error, 1)
	wg.Go(func() { done <- start(ctx, options) })
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop after cancellation")
	}
	wg.Wait()
	require.ErrorIs(t, context.Cause(ctx), context.Canceled)
	require.False(t, errors.Is(context.Cause(ctx), context.DeadlineExceeded))
}
