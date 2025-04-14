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

	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestDMLEventE2E(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, event, _, _ := common.NewLargeEvent4Test(t)

	for _, decimalHandling := range []string{"precise", "string"} {
		for _, unsignedBigintHandling := range []string{"long", "string"} {
			codecConfig.AvroDecimalHandlingMode = decimalHandling
			codecConfig.AvroBigintUnsignedHandlingMode = unsignedBigintHandling

			encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
			require.NoError(t, err)
			require.NotNil(t, encoder)

			topic := "avro-test-topic"
			err = encoder.AppendRowChangedEvent(ctx, topic, event)
			require.NoError(t, err)

			messages := encoder.Build()
			require.Len(t, messages, 1)
			message := messages[0]

			schemaM, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
			require.NoError(t, err)

			decoder := NewDecoder(codecConfig, schemaM, topic, nil)
			err = decoder.AddKeyValue(message.Key, message.Value)
			require.NoError(t, err)

			messageType, exist, err := decoder.HasNext()
			require.NoError(t, err)
			require.True(t, exist)
			require.Equal(t, common.MessageTypeRow, messageType)

			decodedEvent, err := decoder.NextRowChangedEvent()
			require.NoError(t, err)
			require.NotNil(t, decodedEvent)
			require.NotZero(t, decodedEvent.GetTableID())

			TeardownEncoderAndSchemaRegistry4Testing()
		}
	}
}

func TestDDLEventE2E(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	codecConfig.AvroEnableWatermark = true

	encoder, err := NewAvroEncoder(context.Background(), codecConfig)
	require.NoError(t, err)

	ddl, _, _, _ := common.NewLargeEvent4Test(t)
	message, err := encoder.EncodeDDLEvent(ddl)
	require.NoError(t, err)
	require.NotNil(t, message)

	topic := "test-topic"
	decoder := NewDecoder(codecConfig, nil, topic, nil)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, exist, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, exist)
	require.Equal(t, common.MessageTypeDDL, messageType)

	decodedEvent, err := decoder.NextDDLEvent()
	require.NoError(t, err)
	require.NotNil(t, decodedEvent)
	require.Equal(t, ddl.GetCommitTs(), decodedEvent.GetCommitTs())
	require.Equal(t, timodel.ActionCreateTable, decodedEvent.Type)
	require.NotEmpty(t, decodedEvent.Query)
	require.NotEmpty(t, decodedEvent.TableInfo.TableName.Schema)
	require.NotEmpty(t, decodedEvent.TableInfo.TableName.Table)
}

func TestResolvedE2E(t *testing.T) {
	t.Parallel()

	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true
	codecConfig.AvroEnableWatermark = true

	encoder, err := NewAvroEncoder(context.Background(), codecConfig)
	require.NoError(t, err)

	resolvedTs := uint64(1591943372224)
	message, err := encoder.EncodeCheckpointEvent(resolvedTs)
	require.NoError(t, err)
	require.NotNil(t, message)

	topic := "test-topic"
	decoder := NewDecoder(codecConfig, nil, topic, nil)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, exist, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, exist)
	require.Equal(t, common.MessageTypeResolved, messageType)

	obtained, err := decoder.NextResolvedEvent()
	require.NoError(t, err)
	require.Equal(t, resolvedTs, obtained)
}

func TestArvoAppendRowChangedEventWithCallback(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolAvro)
	codecConfig.EnableTiDBExtension = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	encoder, err := SetupEncoderAndSchemaRegistry4Testing(ctx, codecConfig)
	defer TeardownEncoderAndSchemaRegistry4Testing()
	require.NoError(t, err)
	require.NotNil(t, encoder)

	// Empty build makes sure that the callback build logic not broken.
	msgs := encoder.Build()
	require.Len(t, msgs, 0, "no message should be built and no panic")

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(col1 varchar(255) primary key)`
	helper.DDL2Event(sql)

	sql = `insert into test.t values ('aa')`
	event := helper.DML2Event("test", "t", sql)

	row, ok := event.GetNextRow()
	require.True(t, ok)
	expected := 0
	count := 0
	for i := 0; i < 5; i++ {
		expected += i
		bit := i
		err := encoder.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
			TableInfo:      event.TableInfo,
			Event:          row,
			CommitTs:       event.CommitTs,
			ColumnSelector: columnselector.NewDefaultColumnSelector(),
			Callback: func() {
				count += bit
			},
		})
		require.NoError(t, err)

		msgs = encoder.Build()
		require.Len(t, msgs, 1, "one message should be built")

		msgs[0].Callback()
		require.Equal(t, expected, count, "expected one callback be called")
	}
}
