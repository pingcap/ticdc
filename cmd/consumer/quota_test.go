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
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestQuotaFollowsCapturedDataAndRows(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	memory := newMemoryUsage()
	cfg := codecCommon.NewConfig(config.ProtocolCsv)
	cfg.Delimiter, cfg.Terminator, cfg.NullString = ",", "\n", "\\N"
	cfg.IncludeCommitTs = true
	p := newInputParser(ctx, &decodeConfig{codec: cfg, format: rowFormat}, nil, config.GetDefaultReplicaConfig(), nil, nil)
	p.memory = memory
	column := &timodel.ColumnInfo{
		ID: 1, Name: ast.NewCIStr("id"), State: timodel.StatePublic,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
	}
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID: 1, Name: ast.NewCIStr("t"), Columns: []*timodel.ColumnInfo{column},
	})
	raw := []byte("I,t,test,150,1\nI,t,test,160,2\n")
	memory.reserve(int64(len(raw)))
	r := &storageReader{memory: memory}
	var tasks []*decodeTask
	var capturedBytes int64
	for work, err := range r.parseFile(ctx, p, &storageFile{contents: raw, schema: &readSchema{table: table}, tableID: 1}) {
		require.NoError(t, err)
		row := work.(*decodeTask)
		require.Positive(t, row.bytes)
		capturedBytes += row.bytes
		tasks = append(tasks, row)
	}
	require.Len(t, tasks, 2)
	require.Equal(t, capturedBytes, memory.used()) // The entire raw input has been released.

	d := newDecoderGroup(memory, make(chan struct{}, 1))
	stopped := make(chan error, 1)
	go func() { stopped <- d.decodeRows(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.ErrorIs(t, <-stopped, context.Canceled)
	})
	c := &conductor{memory: memory, watermark: 200, decoder: d}
	for _, row := range tasks {
		require.NoError(t, c.accept(ctx, row))
	}
	var rowBytes int64
	for _, row := range tasks {
		<-row.done
		require.Nil(t, row.message)
		rowBytes += row.bytes
	}
	require.Equal(t, rowBytes, memory.used()) // Only converted chunks remain charged.

	downstream := mock.NewMockSink(gomock.NewController(t))
	var submitted []*event.DMLEvent
	downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(row *event.DMLEvent) {
		submitted = append(submitted, row)
	}).Times(2)
	w := &writer{downstream: downstream, memory: memory}
	require.NoError(t, c.flush(ctx, w))
	require.Equal(t, rowBytes, memory.used())
	require.Equal(t, uint64(149), w.checkpoint)
	submitted[0].PostFlush()
	require.Equal(t, tasks[1].bytes, memory.used()) // Each row releases quota at its own flush.
	require.NoError(t, c.flush(ctx, w))
	require.Equal(t, uint64(159), w.checkpoint)
	submitted[1].PostFlush()
	require.Zero(t, memory.used())
	require.NoError(t, c.flush(ctx, w))
	require.Equal(t, uint64(200), w.checkpoint)
}

func TestKafkaConfirmsCheckpointCoveredPrefix(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	const topic, group = "quota", "quota-consumer"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	defer cluster.Close()
	upstream, err := url.Parse("kafka://" + strings.Join(cluster.ListenAddrs(), ",") + "/" + topic + "?protocol=canal-json")
	require.NoError(t, err)
	memory := newMemoryUsage()
	r := newKafkaReader(ctx, upstream, group, memory, config.GetDefaultReplicaConfig(), make(chan struct{}, 1))
	defer r.Close()
	cfg := codecCommon.NewConfig(config.ProtocolCanalJSON)
	cfg.EnableTiDBExtension = true
	p := newInputParser(ctx, &decodeConfig{codec: cfg, format: messageFormat}, nil, config.GetDefaultReplicaConfig(), nil, nil)
	p.memory = memory
	for index, ts := range []uint64{100, 200, 150} {
		// Distinct tables may arrive at different timestamps in one partition.
		raw := []byte(fmt.Sprintf(`{"database":"test","table":"t%d","type":"INSERT","pkNames":["id"],"mysqlType":{"id":"bigint"},"data":[{"id":"1"}],"_tidb":{"commitTs":%d}}`, index, ts))
		memory.reserve(int64(len(raw)))
		for work, err := range r.parse(ctx, p, kafkaInput{message: &kgo.Record{Topic: topic, Partition: 0, Offset: int64(index + 10), Value: raw}}) {
			require.NoError(t, err)
			memory.release(work.(*decodeTask).bytes) // This test discards captured work before conversion.
		}
	}
	require.Equal(t, 3*kafkaRecordBytes, memory.used()) // Only source-position metadata remains.
	require.Equal(t, []kafkaRecord{{through: 100, offset: 10}, {through: 200, offset: 11}, {through: 200, offset: 12}}, r.partitions[0].records)
	require.NoError(t, r.Confirm(ctx, 0))
	require.Len(t, r.partitions[0].records, 3)
	require.NoError(t, r.Confirm(ctx, 100))
	require.Len(t, r.partitions[0].records, 2)
	committed, err := kadm.NewClient(r.client).FetchOffsets(ctx, group)
	require.NoError(t, err)
	require.NoError(t, committed[topic][0].Err)
	require.Equal(t, int64(11), committed[topic][0].At)
	require.NoError(t, r.Confirm(ctx, 150))
	require.Len(t, r.partitions[0].records, 2) // The later 150 cannot pass the preceding 200.
	failedCtx, fail := context.WithCancel(ctx)
	fail()
	require.Error(t, r.Confirm(failedCtx, 200))
	require.Len(t, r.partitions[0].records, 2)
	require.NoError(t, r.Confirm(ctx, 200))
	require.Empty(t, r.partitions[0].records)
	require.Zero(t, memory.used())
	committed, err = kadm.NewClient(r.client).FetchOffsets(ctx, group)
	require.NoError(t, err)
	require.Equal(t, int64(13), committed[topic][0].At)
}
