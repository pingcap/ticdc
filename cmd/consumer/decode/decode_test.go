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
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/ticdc/cmd/consumer/budget"
	"github.com/pingcap/ticdc/cmd/consumer/source"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/avro"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/schemamanager"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func consumerTestSchema(version uint64) *cloudstorage.SchemaFile {
	return &cloudstorage.SchemaFile{
		Schema: "test", Table: "t", Version: 1, TableVersion: version,
		Query: "create table test.t (id bigint primary key)", Type: byte(model.ActionCreateTable),
		Columns:      []cloudstorage.TableCol{{Name: "id", Tp: "BIGINT", IsPK: "true", Nullable: "false"}},
		TotalColumns: 1,
	}
}

func TestGroupOrderedOutput(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cfg := common.NewConfig(config.ProtocolCsv)
	cfg.Delimiter, cfg.Quote, cfg.Terminator, cfg.NullString = ",", "\"", "\n", "\\N"
	cfg.IncludeCommitTs = true
	memory := budget.New()
	group := NewGroup(ctx, Options{CodecConfig: cfg, ReplicaConfig: config.GetDefaultReplicaConfig(), UpstreamURI: &url.URL{}, Budget: memory})
	defer group.Close()
	stopped := make(chan error, 1)
	go func() { stopped <- group.Run(ctx) }()
	defer func() {
		cancel()
		require.ErrorIs(t, <-stopped, context.Canceled)
	}()

	schema := consumerTestSchema(10)
	var contents strings.Builder
	for i := range eventBatchSize + 1 {
		fmt.Fprintf(&contents, "\"I\",\"t\",\"test\",%d,%d\n", 20+i, i)
	}
	input := &source.Input{File: &source.File{Schema: schema, TableID: 42}, Value: []byte(contents.String()), Bytes: int64(contents.Len())}
	memory.ForceAcquire(input.Bytes)
	require.NoError(t, group.AddEvents(ctx, []*source.Input{{File: &source.File{Schema: schema}}, input, {Watermark: 2000}}))
	var rows, ends int
	for ends < 3 {
		select {
		case <-ctx.Done():
			t.Fatal(context.Cause(ctx))
		case batch := <-group.Output():
			for _, work := range batch {
				switch value := work.Event.(type) {
				case *event.DDLEvent:
					require.Zero(t, rows)
					require.Equal(t, schema.Query, value.Query)
				case *event.DMLEvent:
					row := value
					require.Equal(t, uint64(20+rows), row.CommitTs)
					require.Equal(t, int64(42), row.PhysicalTableID)
					require.Equal(t, uint64(10), row.TableInfo.UpdateTS)
					require.Equal(t, int64(rows), row.Rows.GetRow(0).GetInt64(0))
					rows++
					memory.Release(value.Rows.MemoryUsage())
					row.PostFlush()
				case *event.ResolvedEvent:
					require.Equal(t, eventBatchSize+1, rows)
					require.Equal(t, uint64(2000), value.ResolvedTs)
				default:
					ends++
				}
			}
		}
	}
	require.Equal(t, eventBatchSize+1, rows)
	require.Zero(t, memory.Used())
}

// The first registry lookup is held until a second message from the same
// partition reaches another decoder. This proves raw-message decoding overlaps.
func TestGroupDecodesOnePartitionConcurrently(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	const schema = `{"type":"record","namespace":"default.test","name":"t","fields":[{"name":"id","type":{"type":"long","connect.parameters":{"tidb_type":"BIGINT"}}}]}`
	response, err := json.Marshal(map[string]string{"schema": schema})
	require.NoError(t, err)
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			_, _ = w.Write([]byte("{}"))
			return
		case "/schemas/ids/1":
			close(firstStarted)
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return
			}
		case "/schemas/ids/2":
			close(secondStarted)
		default:
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(response)
	}))
	t.Cleanup(server.Close)
	cfg := common.NewConfig(config.ProtocolAvro)
	cfg.AvroConfluentSchemaRegistry = server.URL
	memory := budget.New()
	group := NewGroup(ctx, Options{CodecConfig: cfg, ReplicaConfig: config.GetDefaultReplicaConfig(), Streams: []source.Stream{{ID: 0, Topic: "t"}}, UpstreamURI: &url.URL{}, Budget: memory})
	defer group.Close()
	require.Len(t, group.decoders, decoderConcurrency)
	stopped := make(chan error, 1)
	go func() { stopped <- group.Run(ctx) }()
	defer func() {
		cancel()
		require.ErrorIs(t, <-stopped, context.Canceled)
	}()
	encoder, err := avro.GenCodec(schema)
	require.NoError(t, err)
	for i := 1; i <= 2; i++ {
		key, err := schemamanager.BuildConfluentWireHeader(i)
		require.NoError(t, err)
		key, err = encoder.BinaryFromNative(key, map[string]any{"id": int64(i)})
		require.NoError(t, err)
		input := &source.Input{Stream: 0, Offset: int64(i), Key: key, Bytes: int64(len(key))}
		memory.ForceAcquire(input.Bytes)
		require.NoError(t, group.AddEvents(ctx, []*source.Input{input}))
		started := firstStarted
		if i == 2 {
			started = secondStarted
		}
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("same-partition messages did not decode concurrently")
		}
	}
	select {
	case <-group.Output():
		t.Fatal("later message overtook the blocked first message")
	default:
	}

	close(releaseFirst)
	var rows, ends int
	for ends < 2 {
		select {
		case <-ctx.Done():
			t.Fatal(context.Cause(ctx))
		case batch := <-group.Output():
			for _, result := range batch {
				if result.Event == nil {
					ends++
					continue
				}
				row := result.Event.(*event.DMLEvent)
				rows++
				require.Equal(t, int64(rows), result.Origin.Offset)
				require.Equal(t, int64(rows), row.Rows.GetRow(0).GetInt64(0))
				memory.Release(row.Rows.MemoryUsage())
				row.PostFlush()
			}
		}
	}
	require.Equal(t, 2, rows)
	require.Zero(t, memory.Used())
}

func TestGroupStopsOnDecodeError(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	memory := budget.New()
	cfg := common.NewConfig(config.ProtocolOpen)
	group := NewGroup(ctx, Options{CodecConfig: cfg, ReplicaConfig: config.GetDefaultReplicaConfig(), UpstreamURI: &url.URL{}, Budget: memory})
	defer group.Close()
	stopped := make(chan error, 1)
	go func() { stopped <- group.Run(ctx) }()
	// Storage only supports CSV and Canal JSON. The decode failure must stop
	// decoding even with no output consumer.
	require.NoError(t, group.AddEvents(ctx, []*source.Input{{File: &source.File{Schema: consumerTestSchema(10)}, Value: []byte("row")}}))
	select {
	case err := <-stopped:
		require.ErrorContains(t, err, "storage consumer protocol is unsupported")
	case <-time.After(10 * time.Second):
		t.Fatal("decoder did not stop")
	}
}

func TestGroupStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	memory := budget.New()
	cfg := common.NewConfig(config.ProtocolOpen)
	group := NewGroup(ctx, Options{CodecConfig: cfg, ReplicaConfig: config.GetDefaultReplicaConfig(), UpstreamURI: &url.URL{}, Budget: memory})
	defer group.Close()
	stopped := make(chan error, 1)
	go func() { stopped <- group.Run(ctx) }()
	// Fill output and stop consuming it, then cancel the group.
	require.NoError(t, group.AddEvents(ctx, []*source.Input{{Watermark: 10}, {Watermark: 20}}))
	select {
	case <-group.Output():
	case <-time.After(10 * time.Second):
		t.Fatal("decoder did not publish output")
	}
	cancel()
	select {
	case err := <-stopped:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("decoder did not stop")
	}
}

func TestGroupSharesSchemaAndPreservesOrigin(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	memory := budget.New()
	group := NewGroup(ctx, Options{
		CodecConfig: common.NewConfig(config.ProtocolSimple), ReplicaConfig: config.GetDefaultReplicaConfig(),
		Streams: []source.Stream{{ID: 0, Topic: "t"}, {ID: 1, Topic: "t"}}, UpstreamURI: &url.URL{}, Budget: memory,
	})
	defer group.Close()
	stopped := make(chan error, 1)
	go func() { stopped <- group.Run(ctx) }()
	defer func() {
		cancel()
		require.ErrorIs(t, <-stopped, context.Canceled)
	}()
	inputs := []*source.Input{
		{Stream: 0, Value: []byte(`{"version":1,"database":"test","table":"t","tableID":2,"type":"INSERT","commitTs":90,"schemaVersion":10,"data":{"id":"7"}}`)},
		{Stream: 1, Value: []byte(`{"version":1,"type":"WATERMARK","commitTs":999}`)},
		{Stream: 1, Value: []byte(`{"version":1,"type":"BOOTSTRAP","commitTs":10,"tableSchema":{"schema":"test","table":"t","tableID":1,"version":10,"columns":[{"name":"id","dataType":{"mysqlType":"bigint","charset":"binary","collate":"binary","length":20}}]}}`)},
		{Stream: 0, Value: []byte(`{"version":1,"type":"WATERMARK","commitTs":1000}`)},
	}
	rowsBeforeSchema := make([]*source.Input, decoderConcurrency+1)
	for i := range rowsBeforeSchema {
		rowsBeforeSchema[i] = &source.Input{Stream: 0, Offset: int64(i), Value: inputs[0].Value}
	}
	inputs = append(rowsBeforeSchema, inputs[1:]...)
	inputCount := len(inputs)
	for _, input := range inputs {
		input.Bytes = int64(len(input.Value))
		memory.ForceAcquire(input.Bytes)
	}
	first := inputs[0]
	require.NoError(t, group.AddEvents(ctx, inputs[:1]))
	select {
	case <-group.Output():
		t.Fatal("input without schema must remain pending")
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, group.AddEvents(ctx, inputs[1:]))
	var ends, rows int
	var watermarks []uint64
	for ends < inputCount {
		select {
		case <-ctx.Done():
			t.Fatal(context.Cause(ctx))
		case batch := <-group.Output():
			for _, work := range batch {
				switch value := work.Event.(type) {
				case *event.DMLEvent:
					require.Equal(t, rows, ends, "row must retain its input position while waiting for schema")
					require.Equal(t, int64(rows), work.Origin.Offset)
					require.Zero(t, work.Origin.Stream)
					if rows == 0 {
						require.Equal(t, first.Offset, work.Origin.Offset)
						require.Equal(t, first.Stream, work.Origin.Stream)
					}
					require.Equal(t, uint64(10), value.TableInfo.GetUpdateTS())
					require.Equal(t, uint64(90), value.CommitTs)
					require.Equal(t, int64(2), value.PhysicalTableID)
					require.Equal(t, int64(7), value.Rows.GetRow(0).GetInt64(0))
					memory.Release(value.Rows.MemoryUsage())
					value.PostFlush()
					rows++
				case *event.ResolvedEvent:
					watermarks = append(watermarks, value.ResolvedTs)
				case *event.DDLEvent:
					t.Fatal("bootstrap must not produce a downstream DDL")
				default:
					ends++
				}
			}
		}
	}
	require.Equal(t, decoderConcurrency+1, rows)
	require.Equal(t, []uint64{999, 1000}, watermarks)
	require.Zero(t, memory.Used())
}
