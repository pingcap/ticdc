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
			bin varbinary(16),
			price decimal(10, 4),
			ubig bigint unsigned,
			v bigint null
		)`)
	defer helper.Close()

	dmls := helper.helper.DML2Event("test", "foo",
		"insert into foo values (1, 'alice', x'010203', 12.3400, 18446744073709551615, null)")
	row, ok := dmls.GetNextRow()
	require.True(t, ok)

	cfg := common.NewConfig(config.ProtocolDebeziumAvro)
	cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
	cfg.AvroBigintUnsignedHandlingMode = common.BigintUnsignedHandlingModeString
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
	require.Equal(t, int32(1), unwrapAvroUnionForTest(t, key["id"], "int"))

	value := decodeConfluentAvroForTest(t, messages[0].Value)
	require.Equal(t, "c", value["op"])
	require.Nil(t, value["before"])
	require.NotContains(t, value, "transaction")
	require.IsType(t, int64(0), value["ts_ms"])

	afterUnion, ok := value["after"].(map[string]any)
	require.True(t, ok)
	after, ok := afterUnion["dbserver1.test.foo"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, int32(1), unwrapAvroUnionForTest(t, after["id"], "int"))
	require.Equal(t, "alice", unwrapAvroUnionForTest(t, after["name"], "string"))
	require.Equal(t, []byte{1, 2, 3}, unwrapAvroUnionForTest(t, after["bin"], "bytes"))
	require.Equal(t, "18446744073709551615", unwrapAvroUnionForTest(t, after["ubig"], "string"))
	require.Nil(t, after["v"])

	source, ok := value["source"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "test", source["db"])
	require.Equal(t, "foo", source["table"])
	require.Nil(t, source["snapshot"])
	require.Nil(t, source["thread"])
	require.Equal(t, "dbserver1", source["name"])

	valueSchema := decodeConfluentAvroSchemaForTest(t, messages[0].Value)
	require.Contains(t, valueSchema, `"name":"fooEnvelope"`)
	require.Contains(t, valueSchema, `"name":"foo"`)
	require.Contains(t, valueSchema, `"name":"Source"`)
	require.Contains(t, valueSchema, `"logicalType":"decimal"`)
	require.NotContains(t, valueSchema, `"field":"transaction"`)
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
			bin varbinary(16),
			price decimal(10, 4),
			ubig bigint unsigned,
			v bigint null
		)`)
	defer helper.Close()

	dmls := helper.helper.DML2Event("test", "foo",
		"insert into foo values (1, 'alice', x'010203', 12.3400, 18446744073709551615, null)")
	row, ok := dmls.GetNextRow()
	require.True(t, ok)

	cfg := common.NewConfig(config.ProtocolDebeziumAvro)
	cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
	cfg.AvroBigintUnsignedHandlingMode = common.BigintUnsignedHandlingModeString
	cfg.EnableTiDBExtension = true
	cfg.TimeZone = time.UTC

	commitTs := uint64(123)
	encoder, err := NewAvroBatchEncoder(ctx, cfg, "dbserver1")
	require.NoError(t, err)
	require.NoError(t, encoder.AppendRowChangedEvent(ctx, "dbserver1.test.foo", &commonEvent.RowEvent{
		TableInfo:      helper.tableInfo,
		CommitTs:       commitTs,
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
	require.Equal(t, commitTs, decoded.CommitTs)
	require.Equal(t, "test", decoded.TableInfo.GetSchemaName())
	require.Equal(t, "foo", decoded.TableInfo.GetTableName())

	change, ok := decoded.GetNextRow()
	require.True(t, ok)
	common.CompareRow(t, row, helper.tableInfo, change, decoded.TableInfo)
}

func TestDebeziumConfluentAvroDecodeAccountDMLEvents(t *testing.T) {
	ctx := context.Background()
	_, err := avro.SetupEncoderAndSchemaRegistry4Testing(
		ctx,
		common.NewConfig(config.ProtocolAvro),
	)
	require.NoError(t, err)
	defer avro.TeardownEncoderAndSchemaRegistry4Testing()

	helper := NewSQLTestHelper(t, "tp_account", `
		create table tp_account(
			id int primary key,
			account_id int not null
		)`)
	defer helper.Close()

	insertDML := helper.helper.DML2Event("test", "tp_account",
		"insert into tp_account values (12, 34)")
	updateDML, _ := helper.helper.DML2UpdateEvent("test", "tp_account",
		"insert into tp_account values (13, 34)",
		"update tp_account set account_id = 35 where id = 13")
	deleteDML := helper.helper.DML2DeleteEvent("test", "tp_account",
		"insert into tp_account values (14, 34)",
		"delete from tp_account where id = 14")

	cfg := common.NewConfig(config.ProtocolDebeziumAvro)
	cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
	cfg.EnableTiDBExtension = true
	cfg.TimeZone = time.UTC

	encoder, err := NewAvroBatchEncoder(ctx, cfg, "dbserver1")
	require.NoError(t, err)

	rows := make([]commonEvent.RowChange, 0, 3)
	for _, dml := range []*commonEvent.DMLEvent{insertDML, updateDML, deleteDML} {
		row, ok := dml.GetNextRow()
		if !ok {
			continue
		}
		rows = append(rows, row)
		require.NoError(t, encoder.AppendRowChangedEvent(ctx, "dbserver1.test.tp_account", &commonEvent.RowEvent{
			TableInfo:      helper.tableInfo,
			CommitTs:       1,
			Event:          row,
			ColumnSelector: columnselector.NewDefaultColumnSelector(),
			Callback:       func() {},
		}))
	}
	require.Len(t, rows, 3)

	messages := encoder.Build()
	require.Len(t, messages, 3)
	for idx, message := range messages {
		decoder, err := NewAvroDecoder(ctx, cfg, 0, nil)
		require.NoError(t, err)
		decoder.AddKeyValue(message.Key, message.Value)

		messageType, hasNext := decoder.HasNext()
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeRow, messageType)

		decoded := decoder.NextDMLEvent()
		require.Equal(t, "test", decoded.TableInfo.GetSchemaName())
		require.Equal(t, "tp_account", decoded.TableInfo.GetTableName())

		change, ok := decoded.GetNextRow()
		require.True(t, ok)
		common.CompareRow(t, rows[idx], helper.tableInfo, change, decoded.TableInfo)
	}
}

func TestDebeziumConfluentAvroDecodeShortNamedUnionBranch(t *testing.T) {
	valueSchema := map[string]any{
		"type":      "record",
		"name":      "Value",
		"namespace": "dbserver1.test.tp_account",
		"fields": []any{
			map[string]any{
				"name":                      "id",
				"type":                      "int",
				debeziumAvroConnectFieldKey: "id",
			},
			map[string]any{
				"name":                      "account_id",
				"type":                      "int",
				debeziumAvroConnectFieldKey: "account_id",
			},
		},
	}
	namedSchemas := map[string]any{
		"dbserver1.test.tp_account.Value": valueSchema,
	}

	payload, err := avroNativeToConnectPayload(
		[]any{"null", "dbserver1.test.tp_account.Value"},
		map[string]any{
			"Value": map[string]any{
				"id":         int32(12),
				"account_id": int32(34),
			},
		},
		namedSchemas,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"id":         int32(12),
		"account_id": int32(34),
	}, payload)
}

func TestDebeziumConfluentAvroDecodeFullNamedWrapperForShortUnionBranch(t *testing.T) {
	valueSchema := map[string]any{
		"type":      "record",
		"name":      "Value",
		"namespace": "default.test.tp_account",
		"fields": []any{
			map[string]any{
				"name":                      "id",
				"type":                      "int",
				debeziumAvroConnectFieldKey: "id",
			},
			map[string]any{
				"name":                      "account_id",
				"type":                      "int",
				debeziumAvroConnectFieldKey: "account_id",
			},
		},
	}
	envelopeSchema := map[string]any{
		"type":      "record",
		"name":      "Envelope",
		"namespace": "default.test.tp_account",
		"fields": []any{
			map[string]any{
				"name": "after",
				"type": []any{"null", "Value"},
			},
		},
	}
	namedSchemas := map[string]any{}
	collectAvroNamedSchemas(valueSchema, namedSchemas)
	collectAvroNamedSchemas(envelopeSchema, namedSchemas)

	payload, err := avroNativeToConnectPayload(
		[]any{"null", "Value"},
		map[string]any{
			"default.test.tp_account.Value": map[string]any{
				"id":         int32(12),
				"account_id": int32(34),
			},
		},
		namedSchemas,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"id":         int32(12),
		"account_id": int32(34),
	}, payload)
}

func TestDebeziumConfluentAvroDecodeSingleFieldUnionRecord(t *testing.T) {
	valueSchema := map[string]any{
		"type":      "record",
		"name":      "Value",
		"namespace": "dbserver1.test.only_pk",
		"fields": []any{
			map[string]any{
				"name":                      "id",
				"type":                      "int",
				debeziumAvroConnectFieldKey: "id",
			},
		},
	}
	namedSchemas := map[string]any{
		"dbserver1.test.only_pk.Value": valueSchema,
	}

	payload, err := avroNativeToConnectPayload(
		[]any{"null", "dbserver1.test.only_pk.Value"},
		map[string]any{
			"id": int32(12),
		},
		namedSchemas,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"id": int32(12),
	}, payload)
}

func TestDebeziumConfluentAvroDecodeMissingRecordField(t *testing.T) {
	valueSchema := map[string]any{
		"type":      "record",
		"name":      "Value",
		"namespace": "dbserver1.test.tp_account",
		"fields": []any{
			map[string]any{
				"name":                      "id",
				"type":                      "int",
				debeziumAvroConnectFieldKey: "id",
			},
			map[string]any{
				"name":                      "account_id",
				"type":                      "int",
				debeziumAvroConnectFieldKey: "account_id",
			},
		},
	}

	_, err := avroNativeToConnectPayload(
		valueSchema,
		map[string]any{
			"id": int32(12),
		},
		nil,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "avro record payload is missing field account_id")
}

func TestDebeziumConfluentAvroDecodeMissingOptionalRecordField(t *testing.T) {
	valueSchema := map[string]any{
		"type":      "record",
		"name":      "Table",
		"namespace": "io.debezium.connector.schema",
		"fields": []any{
			map[string]any{
				"name":    "defaultCharsetName",
				"type":    []any{"null", "string"},
				"default": nil,
			},
			map[string]any{
				"name": "columns",
				"type": map[string]any{
					"type":  "array",
					"items": "string",
				},
			},
		},
	}

	payload, err := avroNativeToConnectPayload(
		valueSchema,
		map[string]any{
			"columns": []any{"id"},
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"defaultCharsetName": nil,
		"columns":            []any{"id"},
	}, payload)
}

func TestDebeziumConfluentAvroDecodeMissingArrayRecordField(t *testing.T) {
	valueSchema := map[string]any{
		"type":      "record",
		"name":      "Table",
		"namespace": "io.debezium.connector.schema",
		"fields": []any{
			map[string]any{
				"name": "columns",
				"type": map[string]any{
					"type":  "array",
					"items": "string",
				},
			},
		},
	}

	payload, err := avroNativeToConnectPayload(
		valueSchema,
		map[string]any{},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"columns": []any{},
	}, payload)
}

func TestDebeziumConfluentAvroDoesNotEncodeDDLEvent(t *testing.T) {
	ctx := context.Background()
	_, err := avro.SetupEncoderAndSchemaRegistry4Testing(
		ctx,
		common.NewConfig(config.ProtocolAvro),
	)
	require.NoError(t, err)
	defer avro.TeardownEncoderAndSchemaRegistry4Testing()

	cfg := common.NewConfig(config.ProtocolDebeziumAvro)
	cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
	cfg.EnableTiDBExtension = true
	cfg.TimeZone = time.UTC

	encoder, err := NewAvroBatchEncoder(ctx, cfg, "dbserver1")
	require.NoError(t, err)

	routedDDL := common.NewRoutedDDLEvent4Test()
	message, err := encoder.EncodeDDLEvent(routedDDL)
	require.NoError(t, err)
	require.Nil(t, message)
}

func TestDebeziumConfluentAvroDoesNotEncodeCheckpointEvent(t *testing.T) {
	ctx := context.Background()
	_, err := avro.SetupEncoderAndSchemaRegistry4Testing(
		ctx,
		common.NewConfig(config.ProtocolAvro),
	)
	require.NoError(t, err)
	defer avro.TeardownEncoderAndSchemaRegistry4Testing()

	cfg := common.NewConfig(config.ProtocolDebeziumAvro)
	cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
	cfg.EnableTiDBExtension = true
	cfg.AvroEnableWatermark = true
	cfg.TimeZone = time.UTC

	encoder, err := NewAvroBatchEncoder(ctx, cfg, "dbserver1")
	require.NoError(t, err)

	message, err := encoder.EncodeCheckpointEvent(100)
	require.NoError(t, err)
	require.Nil(t, message)
}

func decodeConfluentAvroForTest(t *testing.T, data []byte) map[string]any {
	t.Helper()

	schema, binaryData := decodeConfluentAvroEnvelopeForTest(t, data)
	codec, err := avro.GenCodec(schema)
	require.NoError(t, err)

	native, _, err := codec.NativeFromBinary(binaryData)
	require.NoError(t, err)

	result, ok := native.(map[string]any)
	require.True(t, ok)
	return result
}

func decodeConfluentAvroSchemaForTest(t *testing.T, data []byte) string {
	t.Helper()

	schema, _ := decodeConfluentAvroEnvelopeForTest(t, data)
	return schema
}

func decodeConfluentAvroEnvelopeForTest(t *testing.T, data []byte) (string, []byte) {
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

	return schemaResp.Schema, binaryData
}

func unwrapAvroUnionForTest(t *testing.T, value any, branch string) any {
	t.Helper()

	union, ok := value.(map[string]any)
	require.True(t, ok)
	result, ok := union[branch]
	require.True(t, ok)
	return result
}
