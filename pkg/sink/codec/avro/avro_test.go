// Copyright 2025 PingCAP, Inc.
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

package avro

import (
	"context"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/uuid"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	parserTypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func newAvroRowEventForTest(
	tableInfo *commonType.TableInfo,
	commitTs uint64,
	preRow chunk.Row,
	row chunk.Row,
) *commonEvent.RowEvent {
	return &commonEvent.RowEvent{
		PhysicalTableID: tableInfo.TableName.TableID,
		StartTs:         commitTs,
		CommitTs:        commitTs,
		TableInfo:       tableInfo,
		Event: commonEvent.RowChange{
			PreRow: preRow,
			Row:    row,
		},
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}
}

func newAvroTableInfoForTest() *commonType.TableInfo {
	idFieldType := parserTypes.NewFieldType(mysql.TypeLong)
	idFieldType.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	ageFieldType := parserTypes.NewFieldType(mysql.TypeLong)

	return commonType.WrapTableInfo("test", &timodel.TableInfo{
		ID:       20,
		Name:     ast.NewCIStr("person"),
		UpdateTS: 100,
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				FieldType: *idFieldType,
				State:     timodel.StatePublic,
				Offset:    0,
			},
			{
				ID:        2,
				Name:      ast.NewCIStr("age"),
				FieldType: *ageFieldType,
				State:     timodel.StatePublic,
				Offset:    1,
			},
		},
	})
}

func TestAvroEncode4EnableChecksum(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	codecConfig.EnableRowChecksum = true
	codecConfig.AvroDecimalHandlingMode = "string"
	codecConfig.AvroBigintUnsignedHandlingMode = "string"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	defer TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	_, event, _, _ := common.NewLargeEvent4Test(t)
	topic := "default"
	bin, err := encoder.encodeValue(ctx, "default", event)
	require.NoError(t, err)

	cid, data, err := extractConfluentSchemaIDAndBinaryData(bin)
	require.NoError(t, err)

	avroValueCodec, err := encoder.schemaM.Lookup(ctx, topic, schemaID{confluentSchemaID: cid})
	require.NoError(t, err)

	res, _, err := avroValueCodec.NativeFromBinary(data)
	require.NoError(t, err)
	require.NotNil(t, res)

	m, ok := res.(map[string]interface{})
	require.True(t, ok)

	_, found := m[tidbRowLevelChecksum]
	require.True(t, found)

	_, found = m[tidbCorrupted]
	require.True(t, found)

	_, found = m[tidbChecksumVersion]
	require.True(t, found)
}

func TestAvroEncodeDeleteChecksum(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	codecConfig.EnableRowChecksum = true
	codecConfig.AvroIncludeBeforeValue = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	defer TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	tableInfo := newAvroTableInfoForTest()
	event := newAvroRowEventForTest(
		tableInfo,
		1024,
		chunk.MutRowFromValues(int64(1), int64(18)).ToRow(),
		chunk.Row{},
	)
	event.Checksum = &integrity.Checksum{
		Current:  11,
		Previous: 22,
	}

	topic := "default"
	bin, err := encoder.encodeValue(ctx, topic, event)
	require.NoError(t, err)

	cid, data, err := extractConfluentSchemaIDAndBinaryData(bin)
	require.NoError(t, err)

	avroValueCodec, err := encoder.schemaM.Lookup(ctx, topic, schemaID{confluentSchemaID: cid})
	require.NoError(t, err)

	res, _, err := avroValueCodec.NativeFromBinary(data)
	require.NoError(t, err)
	m, ok := res.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, deleteOperation, m[tidbOp])
	require.Equal(t, "22", m[tidbRowLevelChecksum])
}

func TestAvroEncode(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	defer TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	_, event, _, _ := common.NewLargeEvent4Test(t)
	topic := "default"
	bin, err := encoder.encodeKey(ctx, topic, event)
	require.NoError(t, err)

	cid, data, err := extractConfluentSchemaIDAndBinaryData(bin)
	require.NoError(t, err)

	avroKeyCodec, err := encoder.schemaM.Lookup(ctx, topic, schemaID{confluentSchemaID: cid})
	require.NoError(t, err)

	res, _, err := avroKeyCodec.NativeFromBinary(data)
	require.NoError(t, err)
	require.NotNil(t, res)
	for k := range res.(map[string]interface{}) {
		if k == "_tidb_commit_ts" || k == "_tidb_op" || k == "_tidb_commit_physical_time" {
			require.Fail(t, "key shall not include extension fields")
		}
	}
	require.Equal(t, int32(127), res.(map[string]interface{})["tu1"])

	bin, err = encoder.encodeValue(ctx, topic, event)
	require.NoError(t, err)

	cid, data, err = extractConfluentSchemaIDAndBinaryData(bin)
	require.NoError(t, err)

	avroValueCodec, err := encoder.schemaM.Lookup(ctx, topic, schemaID{confluentSchemaID: cid})
	require.NoError(t, err)

	res, _, err = avroValueCodec.NativeFromBinary(data)
	require.NoError(t, err)
	require.NotNil(t, res)

	for k, v := range res.(map[string]interface{}) {
		if k == "_tidb_op" {
			require.Equal(t, "c", v.(string))
		}
		if k == "float" {
			require.Equal(t, float32(3.14), v)
		}
	}
}

func TestAvroEncodeIncludeBeforeValue(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	codecConfig.AvroIncludeBeforeValue = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	defer TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	tableInfo := newAvroTableInfoForTest()
	beforeRow := chunk.MutRowFromValues(int64(1), int64(18)).ToRow()
	afterRow := chunk.MutRowFromValues(int64(1), int64(20)).ToRow()

	testCases := []struct {
		name      string
		event     *commonEvent.RowEvent
		op        string
		hasBefore bool
	}{
		{
			name:  "insert",
			event: newAvroRowEventForTest(tableInfo, 1024, chunk.Row{}, beforeRow),
			op:    insertOperation,
		},
		{
			name:      "update",
			event:     newAvroRowEventForTest(tableInfo, 1025, beforeRow, afterRow),
			op:        updateOperation,
			hasBefore: true,
		},
		{
			name:      "delete",
			event:     newAvroRowEventForTest(tableInfo, 1026, afterRow, chunk.Row{}),
			op:        deleteOperation,
			hasBefore: true,
		},
	}

	topic := "default"
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bin, err := encoder.encodeValue(ctx, topic, tc.event)
			require.NoError(t, err)
			require.NotNil(t, bin)

			cid, data, err := extractConfluentSchemaIDAndBinaryData(bin)
			require.NoError(t, err)

			avroValueCodec, err := encoder.schemaM.Lookup(ctx, topic, schemaID{confluentSchemaID: cid})
			require.NoError(t, err)

			res, _, err := avroValueCodec.NativeFromBinary(data)
			require.NoError(t, err)
			require.NotNil(t, res)

			m, ok := res.(map[string]interface{})
			require.True(t, ok)
			require.Equal(t, int64(tc.event.CommitTs), m[tidbCommitTs])
			require.Equal(t, tc.op, m[tidbOp])
			if tc.hasBefore {
				require.NotNil(t, m[ticdcBefore])
			} else {
				require.Nil(t, m[ticdcBefore])
			}

			key, err := encoder.encodeKey(ctx, topic, tc.event)
			require.NoError(t, err)
			decoder := NewDecoder(codecConfig, 0, encoder.schemaM, topic, nil)
			decoder.AddKeyValue(key, bin)

			messageType, exist := decoder.HasNext()
			require.True(t, exist)
			require.Equal(t, common.MessageTypeRow, messageType)

			decoded := decoder.NextDMLEvent()
			require.NotNil(t, decoded)
			require.Equal(t, tc.event.CommitTs, decoded.CommitTs)

			decodedRow, ok := decoded.GetNextRow()
			require.True(t, ok)
			common.CompareRow(t, tc.event.Event, tc.event.TableInfo, decodedRow, decoded.TableInfo)
		})
	}
}

func TestAvroEncodeDeleteEventUsesPreRowForKey(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true

	ctx := t.Context()

	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	defer TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	_, _, _, event := common.NewLargeEvent4Test(t)
	topic := "avro-delete-test-topic"
	require.NoError(t, encoder.AppendRowChangedEvent(ctx, topic, event))

	messages := encoder.Build()
	require.Len(t, messages, 1)
	require.NotEmpty(t, messages[0].Key)
	require.Nil(t, messages[0].Value)

	cid, data, err := extractConfluentSchemaIDAndBinaryData(messages[0].Key)
	require.NoError(t, err)

	avroKeyCodec, err := encoder.schemaM.Lookup(ctx,
		topicName2SchemaSubjects(topic, keySchemaSuffix),
		schemaID{confluentSchemaID: cid})
	require.NoError(t, err)

	res, _, err := avroKeyCodec.NativeFromBinary(data)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, int32(127), res.(map[string]any)["tu1"])
}

func TestAvroEncodeDeleteEventWithWatermarkCarriesCommitTs(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	codecConfig.AvroEnableWatermark = true

	ctx := t.Context()

	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	defer TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	_, _, _, event := common.NewLargeEvent4Test(t)
	topic := "avro-delete-with-watermark-test-topic"
	require.NoError(t, encoder.AppendRowChangedEvent(ctx, topic, event))

	messages := encoder.Build()
	require.Len(t, messages, 1)
	require.NotEmpty(t, messages[0].Key)
	require.NotEmpty(t, messages[0].Value)
	require.Equal(t, deleteByte, messages[0].Value[0])

	decoder := NewDecoder(codecConfig, 0, encoder.schemaM, topic, nil)
	decoder.AddKeyValue(messages[0].Key, messages[0].Value)

	messageType, exists := decoder.HasNext()
	require.True(t, exists)
	require.Equal(t, common.MessageTypeRow, messageType)

	decoded := decoder.NextDMLEvent()
	require.NotNil(t, decoded)
	require.Equal(t, event.CommitTs, decoded.GetCommitTs())
	require.Len(t, decoded.RowTypes, 1)
	require.Equal(t, commonType.RowTypeDelete, decoded.RowTypes[0])

	_, exists = decoder.HasNext()
	require.False(t, exists)
}

func TestAvroEnvelope(t *testing.T) {
	t.Parallel()
	cManager := &confluentSchemaManager{}
	gManager := &glueSchemaManager{}
	avroCodec, err := GenCodec(`
       {
         "type": "record",
         "name": "testdb.avroenvelope",
         "fields" : [
           {"name": "id", "type": "int", "default": 0}
         ]
       }`)

	require.NoError(t, err)

	testNativeData := make(map[string]interface{})
	testNativeData["id"] = 7

	bin, err := avroCodec.BinaryFromNative(nil, testNativeData)
	require.NoError(t, err)

	// test confluent schema message
	header, err := cManager.getMsgHeader(8)
	require.NoError(t, err)
	res := avroEncodeResult{
		data:   bin,
		header: header,
	}

	evlp, err := res.toEnvelope()
	require.NoError(t, err)
	require.Equal(t, header, evlp[0:5])

	parsed, _, err := avroCodec.NativeFromBinary(evlp[5:])
	require.NoError(t, err)
	require.NotNil(t, parsed)

	id, exists := parsed.(map[string]interface{})["id"]
	require.True(t, exists)
	require.Equal(t, int32(7), id)

	// test glue schema message
	uuidGenerator := uuid.NewGenerator()
	uuidS := uuidGenerator.NewString()
	header, err = gManager.getMsgHeader(uuidS)
	require.NoError(t, err)
	res = avroEncodeResult{
		data:   bin,
		header: header,
	}
	evlp, err = res.toEnvelope()
	require.NoError(t, err)
	require.Equal(t, header, evlp[0:18])

	parsed, _, err = avroCodec.NativeFromBinary(evlp[18:])
	require.NoError(t, err)
	require.NotNil(t, parsed)
	id, exists = parsed.(map[string]interface{})["id"]
	require.True(t, exists)
	require.Equal(t, int32(7), id)
}

func TestStringNull(t *testing.T) {
	_, err := goavro.NewCodecWithOptions(`{
		"type": "record",
		"name": "test",
		"fields":
		  [
			{
			 "type": [
				 "string",
				 "null"
			 ],
			 "default": "null",
			 "name": "field"
			}
		   ]
	  }`, &goavro.CodecOption{EnableStringNull: true})
	require.Error(t, err)
	_, err = goavro.NewCodecWithOptions(`{
		"type": "record",
		"name": "test",
		"fields":
		  [
			{
			 "type": [
				 "string",
				 "null"
			 ],
			 "default": "null",
			 "name": "field"
			}
		   ]
	  }`, &goavro.CodecOption{EnableStringNull: false})
	require.NoError(t, err)
}
