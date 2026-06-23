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
// See the License for the specific language governing permissions and
// limitations under the License.

package debezium

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/avro"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestDebeziumConfluentAvroEncodeRowEvent(t *testing.T) {
	ctx := context.Background()
	_, err := avro.SetupEncoderAndSchemaRegistry4Testing(
		ctx,
		common.NewConfig(config.ProtocolAvro),
	)
	require.NoError(t, err)
	defer avro.TeardownEncoderAndSchemaRegistry4Testing()

	helper := NewSQLTestHelper(t, "foo", `
		create table foo(
			id int primary key,
			name varchar(16),
			v bigint null
		)`)
	defer helper.Close()

	dmls := helper.helper.DML2Event("test", "foo", "insert into foo values (1, 'alice', null)")
	row, ok := dmls.GetNextRow()
	require.True(t, ok)

	cfg := common.NewConfig(config.ProtocolDebezium)
	cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
	cfg.DebeziumDisableSchema = true
	cfg.TimeZone = time.UTC

	encoder, err := NewAvroBatchEncoder(ctx, cfg, "dbserver1")
	require.NoError(t, err)
	require.NoError(t, encoder.AppendRowChangedEvent(ctx, "dbserver1.test.foo", &commonEvent.RowEvent{
		TableInfo:      helper.tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}))

	messages := encoder.Build()
	require.Len(t, messages, 1)
	require.Equal(t, byte(0), messages[0].Key[0])
	require.Equal(t, byte(0), messages[0].Value[0])

	key := decodeConfluentAvroForTest(t, messages[0].Key)
	require.Equal(t, int32(1), key["id"])

	value := decodeConfluentAvroForTest(t, messages[0].Value)
	require.Equal(t, "c", value["op"])
	require.Nil(t, value["before"])

	afterUnion, ok := value["after"].(map[string]any)
	require.True(t, ok)
	after, ok := afterUnion["dbserver1.test.foo.Value"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, int32(1), after["id"])
	name, ok := after["name"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "alice", name["string"])
	require.Nil(t, after["v"])

	source, ok := value["source"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "test", source["db"])
	table, ok := source["table"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "foo", table["string"])
	require.Equal(t, "dbserver1", source["name"])
}

func TestDebeziumConfluentAvroDecodeRowEvent(t *testing.T) {
	ctx := context.Background()
	_, err := avro.SetupEncoderAndSchemaRegistry4Testing(
		ctx,
		common.NewConfig(config.ProtocolAvro),
	)
	require.NoError(t, err)
	defer avro.TeardownEncoderAndSchemaRegistry4Testing()

	helper := NewSQLTestHelper(t, "foo", `
		create table foo(
			id int primary key,
			name varchar(16),
			v bigint null
		)`)
	defer helper.Close()

	dmls := helper.helper.DML2Event("test", "foo", "insert into foo values (1, 'alice', null)")
	row, ok := dmls.GetNextRow()
	require.True(t, ok)

	cfg := common.NewConfig(config.ProtocolDebezium)
	cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
	cfg.EnableTiDBExtension = true
	cfg.TimeZone = time.UTC

	encoder, err := NewAvroBatchEncoder(ctx, cfg, "dbserver1")
	require.NoError(t, err)
	require.NoError(t, encoder.AppendRowChangedEvent(ctx, "dbserver1.test.foo", &commonEvent.RowEvent{
		TableInfo:      helper.tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}))

	messages := encoder.Build()
	require.Len(t, messages, 1)

	decoder, err := NewAvroDecoder(ctx, cfg, 0, nil)
	require.NoError(t, err)
	decoder.AddKeyValue(messages[0].Key, messages[0].Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	decoded := decoder.NextDMLEvent()
	require.Equal(t, "test", decoded.TableInfo.GetSchemaName())
	require.Equal(t, "foo", decoded.TableInfo.GetTableName())

	change, ok := decoded.GetNextRow()
	require.True(t, ok)
	common.CompareRow(t, row, helper.tableInfo, change, decoded.TableInfo)
}

func TestDebeziumConfluentAvroDecodeDDLEvent(t *testing.T) {
	ctx := context.Background()
	_, err := avro.SetupEncoderAndSchemaRegistry4Testing(
		ctx,
		common.NewConfig(config.ProtocolAvro),
	)
	require.NoError(t, err)
	defer avro.TeardownEncoderAndSchemaRegistry4Testing()

	cfg := common.NewConfig(config.ProtocolDebezium)
	cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
	cfg.EnableTiDBExtension = true
	cfg.TimeZone = time.UTC

	encoder, err := NewAvroBatchEncoder(ctx, cfg, "dbserver1")
	require.NoError(t, err)

	routedDDL := common.NewRoutedDDLEvent4Test()
	message, err := encoder.EncodeDDLEvent(routedDDL)
	require.NoError(t, err)
	require.NotNil(t, message)

	decoder, err := NewAvroDecoder(ctx, cfg, 0, nil)
	require.NoError(t, err)
	decoder.AddKeyValue(message.Key, message.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, messageType)

	decoded := decoder.NextDDLEvent()
	require.Equal(t, "target_db", decoded.SchemaName)
	require.Equal(t, "target_table", decoded.TableName)
	require.Equal(t, routedDDL.Query, decoded.Query)
}

func TestDebeziumConfluentAvroDecodeSchemaDDLEvent(t *testing.T) {
	ctx := context.Background()
	_, err := avro.SetupEncoderAndSchemaRegistry4Testing(
		ctx,
		common.NewConfig(config.ProtocolAvro),
	)
	require.NoError(t, err)
	defer avro.TeardownEncoderAndSchemaRegistry4Testing()

	cfg := common.NewConfig(config.ProtocolDebezium)
	cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
	cfg.EnableTiDBExtension = true
	cfg.TimeZone = time.UTC

	encoder, err := NewAvroBatchEncoder(ctx, cfg, "dbserver1")
	require.NoError(t, err)

	ddl := &commonEvent.DDLEvent{
		Version:    commonEvent.DDLEventVersion1,
		Type:       byte(timodel.ActionCreateSchema),
		SchemaName: "test",
		Query:      "CREATE DATABASE `test`",
		FinishedTs: 100,
	}
	message, err := encoder.EncodeDDLEvent(ddl)
	require.NoError(t, err)
	require.NotNil(t, message)

	decoder, err := NewAvroDecoder(ctx, cfg, 0, nil)
	require.NoError(t, err)
	decoder.AddKeyValue(message.Key, message.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, messageType)

	decoded := decoder.NextDDLEvent()
	require.Equal(t, "test", decoded.SchemaName)
	require.Empty(t, decoded.TableName)
	require.Equal(t, ddl.Query, decoded.Query)
}

func decodeConfluentAvroForTest(t *testing.T, data []byte) map[string]any {
	t.Helper()

	require.GreaterOrEqual(t, len(data), 5)
	require.Equal(t, byte(0), data[0])
	schemaID := int(binary.BigEndian.Uint32(data[1:5]))
	binaryData := data[5:]

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:8081/schemas/ids/%d", schemaID))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var schemaResp struct {
		Schema string `json:"schema"`
	}
	require.NoError(t, json.Unmarshal(body, &schemaResp))

	codec, err := avro.GenCodec(schemaResp.Schema)
	require.NoError(t, err)

	native, _, err := codec.NativeFromBinary(binaryData)
	require.NoError(t, err)

	result, ok := native.(map[string]any)
	require.True(t, ok)
	return result
}
