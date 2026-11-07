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

package source

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/ticdc/cmd/consumer/budget"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func consumerTestSchema(version uint64) *cloudstorage.SchemaFile {
	return &cloudstorage.SchemaFile{
		Schema: "test", Table: "t", Version: 1, TableVersion: version,
		Query: "create table test.t (id bigint primary key)", Type: byte(model.ActionCreateTable),
		Columns:      []cloudstorage.TableCol{{Name: "id", Tp: "BIGINT", IsPK: "true", Nullable: "false"}},
		TotalColumns: 1,
	}
}

func TestKafkaReaderPolling(t *testing.T) {
	const topic = "consumer-polling"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	defer producer.Close()
	var records []*kgo.Record
	for partition := range int32(2) {
		for i := range readBatchRows + 1 {
			records = append(records, &kgo.Record{Topic: topic, Partition: partition, Key: []byte(fmt.Sprint(i)), Value: []byte("value")})
		}
	}
	require.NoError(t, producer.ProduceSync(ctx, records...).FirstErr())
	upstream := &url.URL{Scheme: "kafka", Host: strings.Join(cluster.ListenAddrs(), ","), Path: "/" + topic}
	memory := budget.New()
	reader := newKafkaReader(ctx, Options{UpstreamURI: upstream, ConsumerID: "consumer-polling", ReplicaConfig: config.GetDefaultReplicaConfig(), CodecConfig: codecCommon.NewConfig(config.ProtocolOpen), Budget: memory})
	defer reader.Close()

	// A full conductor may pause all partitions; dependency reads can resume them.
	memory.ForceAcquire(memory.Limit())
	reader.ContinueReading(false)
	require.ElementsMatch(t, []int32{0, 1}, reader.client.PauseFetchPartitions(nil)[topic])
	pausedCtx, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stop()
	_, err = reader.Read(pausedCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	reader.Advance(Progress{
		Target:   100,
		Retarget: true,
		Proven:   []int32{0},
		Watermarks: []StreamWatermark{
			{Stream: 0},
			{Stream: 1},
		},
	})
	require.ElementsMatch(t, []int32{0, 1}, reader.client.PauseFetchPartitions(nil)[topic])
	firstBatch := make(chan []*Input, 1)
	readError := make(chan error, 1)
	go func() {
		inputs, err := reader.Read(ctx)
		firstBatch <- inputs
		readError <- err
	}()
	select {
	case err := <-readError:
		t.Fatalf("poll returned while all partitions were paused: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	reader.ContinueReading(true)
	require.Equal(t, []int32{0}, reader.client.PauseFetchPartitions(nil)[topic])
	require.NoError(t, <-readError)
	pending := <-firstBatch

	var last *Input
	for _, partition := range []int32{1, 0} {
		if partition == 0 {
			reader.Advance(Progress{Target: 0, Retarget: true})
			require.Empty(t, reader.client.PauseFetchPartitions(nil))
		}
		var offset int64
		for offset < readBatchRows+1 {
			inputs := pending
			pending = nil
			if len(inputs) == 0 {
				inputs, err = reader.Read(ctx)
				require.NoError(t, err)
			}
			require.NotEmpty(t, inputs)
			require.LessOrEqual(t, len(inputs), readBatchRows)
			for _, input := range inputs {
				require.Equal(t, partition, input.Stream)
				require.Equal(t, offset, input.Offset)
				require.Equal(t, fmt.Sprint(offset), string(input.Key))
				require.Equal(t, "value", string(input.Value))
				memory.Release(input.Bytes)
				offset++
				last = input
			}
		}
	}
	require.LessOrEqual(t, reader.client.CommittedOffsets()[topic][0].Offset, int64(0), "polling must not commit source positions")
	require.NoError(t, last.Ack(ctx))
	require.Equal(t, int64(readBatchRows+1), reader.client.CommittedOffsets()[topic][0].Offset)
	memory.Release(memory.Limit())
	require.Zero(t, memory.Used())

	canceledCtx, cancelCause := context.WithCancelCause(ctx)
	cancelCause(context.DeadlineExceeded)
	_, err = reader.Read(canceledCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestKafkaInputValidation(t *testing.T) {
	reader := &kafkaReader{
		protocol:        config.ProtocolOpen,
		maxMessageBytes: 100,
		maxBatchSize:    2,
		partitions:      map[int32]*kafkaPartition{0: {}, 1: {}},
	}
	origin := Origin{Bytes: 100}
	require.PanicsWithValue(t, "Kafka message batch size limit exceeded", func() {
		reader.CheckInput(origin, 3)
	})
	origin.Bytes = 101
	require.PanicsWithValue(t, "Kafka message byte size limit exceeded", func() {
		reader.CheckInput(origin, 2)
	})
}

func TestKafkaPartitionRouting(t *testing.T) {
	cfg := config.GetDefaultReplicaConfig()
	cfg.Sink.DispatchRules = []*config.DispatchRule{{Matcher: []string{"test.by_ts"}, PartitionRule: "ts"}}
	router, err := eventrouter.NewEventRouter(cfg.Sink, true, "t", false, false)
	require.NoError(t, err)
	client, err := kgo.NewClient()
	require.NoError(t, err)
	defer client.Close()
	memory := budget.New()
	memory.ForceAcquire(memory.Limit())
	reader := &kafkaReader{
		router:     router,
		client:     client,
		topic:      "t",
		memory:     memory,
		continuing: true,
		partitions: map[int32]*kafkaPartition{0: {}, 1: {}},
	}
	name := event.SchemaTableName{SchemaName: "test", TableName: "t"}
	for _, test := range []struct {
		name        string
		tables      []event.SchemaTableName
		paused      []int32
		controlOnly bool
	}{
		{name: "table", tables: []event.SchemaTableName{name, name}, paused: []int32{0}},
		{name: "global"},
		{name: "control", tables: []event.SchemaTableName{}, paused: []int32{1}, controlOnly: true},
		{name: "schema", tables: []event.SchemaTableName{{SchemaName: "test"}}},
		{name: "row dependent", tables: []event.SchemaTableName{name, {SchemaName: "test", TableName: "by_ts"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			progress := Progress{
				Scope:      test.tables,
				Target:     100,
				Retarget:   true,
				Watermarks: []StreamWatermark{{Stream: 0}, {Stream: 1}},
			}
			require.False(t, reader.Advance(progress))
			require.ElementsMatch(t, test.paused, client.PauseFetchPartitions(nil)[reader.topic])
			progress.Retarget = false
			progress.Watermarks[0].Timestamp = 100
			require.Equal(t, test.controlOnly, reader.Advance(progress))
			progress.Watermarks[1].Timestamp = 100
			require.True(t, reader.Advance(progress))
		})
	}

	table := consumerTestSchema(1).TableInfo()
	dml := &event.DMLEvent{
		TableInfo: table,
		CommitTs:  10,
		RowTypes:  []common.RowType{common.RowTypeInsert},
		Rows:      chunk.NewChunkWithCapacity(table.GetFieldSlice(), 1),
	}
	dml.Rows.AppendInt64(0, 1)
	reader.CheckDML(dml, 1, 42)
	_, ok := dml.GetNextRow()
	require.True(t, ok, "partition validation must rewind the event")
	require.PanicsWithValue(t, "Kafka DML was dispatched to the wrong partition", func() {
		reader.CheckDML(dml, 0, 42)
	})
	_, ok = dml.GetNextRow()
	require.True(t, ok, "failed partition validation must also rewind the event")
}
