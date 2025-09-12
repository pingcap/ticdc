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
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/uuid"
	"github.com/stretchr/testify/require"
)

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
