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
	"encoding/json/v2"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/ticdc/cmd/consumer/budget"
	"github.com/pingcap/ticdc/cmd/consumer/conductor"
	"github.com/pingcap/ticdc/cmd/consumer/decode"
	"github.com/pingcap/ticdc/cmd/consumer/source"
	"github.com/pingcap/ticdc/cmd/consumer/writer"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
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

func TestStorageConductorSchemaProgress(t *testing.T) {
	root := t.TempDir()
	for _, version := range []uint64{10, 30} {
		schema := consumerTestSchema(version)
		if version == 30 {
			schema.Query, schema.Type = "alter table test.t modify id bigint", byte(model.ActionModifyColumn)
		}
		data, err := json.Marshal(schema)
		require.NoError(t, err)
		writeConsumerTestFile(t, root, schema.Path(false, 0), data)
		schemaKey := cloudstorage.SchemaPathKey{Schema: "test", Table: "t", TableVersion: version}
		key := cloudstorage.DMLPathKey{SchemaPathKey: schemaKey}
		index := cloudstorage.FileIndex{Idx: 1}
		path := key.GenerateDMLFilePath(&index, ".csv", 6)
		writeConsumerTestFile(t, root, path, []byte(fmt.Sprintf("\"I\",\"t\",\"test\",%d,1\n", version+10)))
		writeConsumerTestFile(t, root, key.GenerateIndexFilePath(index.FileIndexKey), []byte(filepath.Base(path)+"\n"))
	}
	writeConsumerTestFile(t, root, "metadata", []byte(`{"checkpoint-ts":50}`))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	upstream := &url.URL{Scheme: "file", Path: root, RawQuery: "protocol=csv"}
	replica := config.GetDefaultReplicaConfig()
	replica.Sink.DateSeparator = new(config.DateSeparatorNone)
	replica.Sink.FileIndexWidth = new(6)
	codec := common.NewConfig(config.ProtocolCsv)
	codec.Delimiter, codec.Quote, codec.Terminator, codec.NullString = ",", "\"", "\n", "\\N"
	codec.IncludeCommitTs = true
	memory := budget.New()
	reader := source.Open(ctx, source.Options{
		UpstreamURI:   upstream,
		ConsumerID:    "test",
		ReplicaConfig: replica,
		CodecConfig:   codec,
		Budget:        memory,
	})
	decoder := decode.NewGroup(ctx, decode.Options{
		CodecConfig:   codec,
		ReplicaConfig: replica,
		Streams:       reader.Streams(),
		UpstreamURI:   upstream,
		Budget:        memory,
	})
	scheduler := conductor.New(memory, reader)
	c := &consumer{
		reader:    reader,
		decoder:   decoder,
		conductor: scheduler,
		writer: writer.New(ctx, writer.Options{
			DownstreamURI: "blackhole://",
			ReplicaConfig: replica,
			CodecConfig:   codec,
			Budget:        memory,
		}),
	}
	stopped := make(chan error, 1)
	go func() { stopped <- c.start(ctx) }()
	require.Eventually(t, func() bool { return scheduler.Checkpoint() == 50 }, 10*time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-stopped:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("consumer did not stop")
	}
	require.Zero(t, memory.Used())
}

func writeConsumerTestFile(t *testing.T, root, path string, data []byte) {
	t.Helper()
	path = filepath.Join(root, path)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, data, 0o600))
}
