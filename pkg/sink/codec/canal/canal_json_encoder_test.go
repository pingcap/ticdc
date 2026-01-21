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

package canal

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/compression"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func dml2rowEvent(t *testing.T, dml *commonEvent.DMLEvent) *commonEvent.RowEvent {
	row, ok := dml.GetNextRow()
	dml.Rewind()
	require.True(t, ok)
	return &commonEvent.RowEvent{
		TableInfo:      dml.TableInfo,
		CommitTs:       dml.CommitTs,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	}
}

func TestDMLE2E(t *testing.T) {
	createTableDDLEvent, insertEvent, updateEvent, deleteEvent := common.NewLargeEvent4Test(t)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	for _, enableTiDBExtension := range []bool{false, true} {
		codecConfig.EnableTiDBExtension = enableTiDBExtension
		encIface, err := NewJSONRowEventEncoder(ctx, codecConfig)
		require.NoError(t, err)

		encoder := encIface.(*JSONRowEventEncoder)

		decoder, err := NewDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)

		message, err := encoder.EncodeDDLEvent(createTableDDLEvent)
		require.NoError(t, err)

		decoder.AddKeyValue(message.Key, message.Value)

		messageType, hasNext := decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, messageType, common.MessageTypeDDL)

		decodedDDL := decoder.NextDDLEvent()
		require.NoError(t, err)
		if enableTiDBExtension {
			require.Equal(t, createTableDDLEvent.GetCommitTs(), decodedDDL.GetCommitTs())
		}
		require.Equal(t, createTableDDLEvent.Query, decodedDDL.Query)

		err = encoder.AppendRowChangedEvent(ctx, "", insertEvent)
		require.NoError(t, err)

		message = encoder.Build()[0]
		decoder.AddKeyValue(message.Key, message.Value)

		messageType, hasNext = decoder.HasNext()
		require.True(t, hasNext)
		require.Equal(t, messageType, common.MessageTypeRow)

		decodedEvent := dml2rowEvent(t, decoder.NextDMLEvent())
		require.True(t, decodedEvent.IsInsert())
		if enableTiDBExtension {
			require.Equal(t, insertEvent.CommitTs, decodedEvent.CommitTs)
		}
		require.Equal(t, insertEvent.TableInfo.GetSchemaName(), decodedEvent.TableInfo.GetSchemaName())
		require.Equal(t, insertEvent.TableInfo.GetTableName(), decodedEvent.TableInfo.GetTableName())

		common.CompareRow(t, insertEvent.Event, insertEvent.TableInfo, decodedEvent.Event, decodedEvent.TableInfo)

		err = encoder.AppendRowChangedEvent(ctx, "", updateEvent)
		require.NoError(t, err)

		message = encoder.Build()[0]

		decoder.AddKeyValue(message.Key, message.Value)

		messageType, hasNext = decoder.HasNext()
		require.True(t, hasNext)
		require.EqualValues(t, messageType, common.MessageTypeRow)

		decodedEvent = dml2rowEvent(t, decoder.NextDMLEvent())
		require.True(t, decodedEvent.IsUpdate())

		err = encoder.AppendRowChangedEvent(ctx, "", deleteEvent)
		require.NoError(t, err)

		message = encoder.Build()[0]
		decoder.AddKeyValue(message.Key, message.Value)

		messageType, hasNext = decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.EqualValues(t, messageType, common.MessageTypeRow)

		decodedEvent = dml2rowEvent(t, decoder.NextDMLEvent())
		require.NoError(t, err)
		require.True(t, decodedEvent.IsDelete())
	}
}

func TestCanalJSONCompressionE2E(t *testing.T) {
	_, insertEvent, _, _ := common.NewLargeEvent4Test(t)

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compression.LZ4

	ctx := context.Background()
	encIface, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := encIface.(*JSONRowEventEncoder)

	// encode normal row changed event
	rc := insertEvent.Event
	err = encoder.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
		TableInfo:      insertEvent.TableInfo,
		CommitTs:       insertEvent.CommitTs,
		Event:          rc,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	})
	require.NoError(t, err)

	message := encoder.Build()[0]

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	decoder.AddKeyValue(message.Key, message.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, messageType, common.MessageTypeRow)

	decodedEvent := decoder.NextDMLEvent()
	require.Equal(t, decodedEvent.CommitTs, insertEvent.CommitTs)
	require.Equal(t, decodedEvent.TableInfo.GetSchemaName(), insertEvent.TableInfo.GetSchemaName())
	require.Equal(t, decodedEvent.TableInfo.GetTableName(), insertEvent.TableInfo.GetTableName())

	// encode DDL event
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	sql := `create table test.person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))`
	ddlEvent := helper.DDL2Event(sql)

	message, err = encoder.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)

	decoder.AddKeyValue(message.Key, message.Value)

	messageType, hasNext = decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, messageType, common.MessageTypeDDL)

	decodedDDL := decoder.NextDDLEvent()
	require.NoError(t, err)

	require.Equal(t, decodedDDL.Query, ddlEvent.Query)
	require.Equal(t, decodedDDL.GetCommitTs(), ddlEvent.GetCommitTs())
	require.Equal(t, decodedDDL.SchemaName, ddlEvent.SchemaName)
	require.Equal(t, decodedDDL.TableName, ddlEvent.TableName)

	// encode checkpoint event
	waterMark := uint64(2333)
	message, err = encoder.EncodeCheckpointEvent(waterMark)
	require.NoError(t, err)

	decoder.AddKeyValue(message.Key, message.Value)

	messageType, hasNext = decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, messageType, common.MessageTypeResolved)

	decodedWatermark := decoder.NextResolvedEvent()
	require.Equal(t, decodedWatermark, waterMark)
}

func TestCanalJSONClaimCheckE2E(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionClaimCheck
	codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compression.Snappy
	codecConfig.LargeMessageHandle.ClaimCheckStorageURI = "file:///tmp/canal-json-claim-check"
	codecConfig.MaxMessageBytes = 500
	ctx := context.Background()

	for _, rawValue := range []bool{false, true} {
		codecConfig.LargeMessageHandle.ClaimCheckRawValue = rawValue

		encIface, err := NewJSONRowEventEncoder(ctx, codecConfig)
		require.NoError(t, err)
		encoder := encIface.(*JSONRowEventEncoder)

		_, insertEvent, _, _ := common.NewLargeEvent4Test(t)
		rc := insertEvent.Event
		err = encoder.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
			TableInfo:      insertEvent.TableInfo,
			CommitTs:       insertEvent.CommitTs,
			Event:          rc,
			ColumnSelector: columnselector.NewDefaultColumnSelector(),
		})
		require.NoError(t, err)

		// this is a large message, should be delivered to the external storage.
		claimCheckLocationMessage := encoder.Build()[0]

		decoder, err := NewDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)

		decoder.AddKeyValue(claimCheckLocationMessage.Key, claimCheckLocationMessage.Value)

		messageType, ok := decoder.HasNext()
		require.Equal(t, messageType, common.MessageTypeRow)
		require.True(t, ok)

		decodedLargeEvent := decoder.NextDMLEvent()

		require.Equal(t, insertEvent.CommitTs, decodedLargeEvent.CommitTs)
		require.Equal(t, insertEvent.TableInfo.GetSchemaName(), decodedLargeEvent.TableInfo.GetSchemaName())
		require.Equal(t, insertEvent.TableInfo.GetTableName(), decodedLargeEvent.TableInfo.GetTableName())

		change, ok := decodedLargeEvent.GetNextRow()
		require.True(t, ok)
		common.CompareRow(t, insertEvent.Event, insertEvent.TableInfo, change, decodedLargeEvent.TableInfo)
	}
}

func TestNewCanalJSONMessageHandleKeyOnly4LargeMessage(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionHandleKeyOnly
	codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compression.LZ4
	codecConfig.MaxMessageBytes = 500

	ctx := context.Background()

	encIface, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := encIface.(*JSONRowEventEncoder)

	_, insertEvent, _, _ := common.NewLargeEvent4Test(t)
	err = encoder.AppendRowChangedEvent(context.Background(), "", insertEvent)
	require.NoError(t, err)

	message := encoder.Build()[0]

	dec, err := NewDecoder(context.Background(), codecConfig, &sql.DB{})
	require.NoError(t, err)

	dec.AddKeyValue(message.Key, message.Value)

	messageType, ok := dec.HasNext()
	require.Equal(t, messageType, common.MessageTypeRow)
	require.True(t, ok)

	handleKeyOnlyMessage := dec.(*decoder).msg.(*canalJSONMessageWithTiDBExtension)
	require.True(t, handleKeyOnlyMessage.Extensions.OnlyHandleKey)
	for _, col := range insertEvent.TableInfo.GetColumns() {
		if col == nil {
			continue
		}
		colName := insertEvent.TableInfo.ForceGetColumnName(col.ID)
		if insertEvent.TableInfo.IsHandleKey(col.ID) {
			require.Contains(t, handleKeyOnlyMessage.Data[0], colName)
			require.Contains(t, handleKeyOnlyMessage.SQLType, colName)
			require.Contains(t, handleKeyOnlyMessage.MySQLType, colName)
		} else {
			require.NotContains(t, handleKeyOnlyMessage.Data[0], colName)
			require.NotContains(t, handleKeyOnlyMessage.SQLType, colName)
			require.NotContains(t, handleKeyOnlyMessage.MySQLType, colName)
		}
	}
}

func TestNewCanalJSONMessageFromDDL(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	ctx := context.Background()

	encIface, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := encIface.(*JSONRowEventEncoder)

	sql := `create table test.person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))`
	ddlEvent := helper.DDL2Event(sql)

	message := encoder.newJSONMessageForDDL(ddlEvent)
	require.NotNil(t, message)

	msg, ok := message.(*JSONMessage)
	require.True(t, ok)
	require.Equal(t, convertToCanalTs(ddlEvent.GetCommitTs()), msg.ExecutionTime)
	require.True(t, msg.IsDDL)
	require.Equal(t, "test", msg.Schema)
	require.Equal(t, "person", msg.Table)
	require.Equal(t, ddlEvent.Query, msg.Query)
	require.Equal(t, "CREATE", msg.EventType)

	codecConfig.EnableTiDBExtension = true
	encIface, err = NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	encoder = encIface.(*JSONRowEventEncoder)
	message = encoder.newJSONMessageForDDL(ddlEvent)
	require.NotNil(t, message)

	withExtension, ok := message.(*canalJSONMessageWithTiDBExtension)
	require.True(t, ok)

	require.NotNil(t, withExtension.Extensions)
	require.Equal(t, ddlEvent.GetCommitTs(), withExtension.Extensions.CommitTs)
}

func TestBatching(t *testing.T) {
	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	encIface, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := encIface.(*JSONRowEventEncoder)
	require.NotNil(t, encoder)

	_, _, updateEvent, _ := common.NewLargeEvent4Test(t)
	updateCase := *updateEvent
	for i := 1; i <= 1000; i++ {
		ts := uint64(i)
		updateCase.CommitTs = ts
		err := encoder.AppendRowChangedEvent(context.Background(), "", &updateCase)
		require.NoError(t, err)

		if i%100 == 0 {
			msgs := encoder.Build()
			require.NotNil(t, msgs)
			require.Len(t, msgs, 100)

			for j := range msgs {
				require.Equal(t, 1, msgs[j].GetRowsCount())

				var msg JSONMessage
				err := json.Unmarshal(msgs[j].Value, &msg)
				require.NoError(t, err)
				require.Equal(t, "UPDATE", msg.EventType)
			}
		}
	}

	require.Len(t, encoder.messages, 0)
}

func TestEncodeCheckpointEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	var watermark uint64 = 2333
	for _, enable := range []bool{false, true} {
		codecConfig := common.NewConfig(config.ProtocolCanalJSON)
		codecConfig.EnableTiDBExtension = enable

		encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
		require.NoError(t, err)

		msg, err := encoder.EncodeCheckpointEvent(watermark)
		require.NoError(t, err)

		if !enable {
			require.Nil(t, msg)
			continue
		}

		require.NotNil(t, msg)

		ctx := context.Background()
		decoder, err := NewDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)

		decoder.AddKeyValue(msg.Key, msg.Value)

		ty, hasNext := decoder.HasNext()
		if enable {
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeResolved, ty)
			consumed := decoder.NextResolvedEvent()
			require.Equal(t, watermark, consumed)
		} else {
			require.False(t, hasNext)
			require.Equal(t, common.MessageTypeUnknown, ty)
		}

		ty, hasNext = decoder.HasNext()
		require.False(t, hasNext)
		require.Equal(t, common.MessageTypeUnknown, ty)
	}
}

func TestCheckpointEventValueMarshal(t *testing.T) {
	t.Parallel()

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true

	ctx := context.Background()

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	var watermark uint64 = 1024
	msg, err := encoder.EncodeCheckpointEvent(watermark)
	require.NoError(t, err)
	require.NotNil(t, msg)

	// Unmarshal from the data we have encoded.
	jsonMsg := canalJSONMessageWithTiDBExtension{
		&JSONMessage{},
		&tidbExtension{},
	}
	err = json.Unmarshal(msg.Value, &jsonMsg)
	require.NoError(t, err)
	require.Equal(t, watermark, jsonMsg.Extensions.WatermarkTs)
	require.Equal(t, tidbWaterMarkType, jsonMsg.EventType)
	require.Equal(t, "", jsonMsg.Schema)
	require.Equal(t, "", jsonMsg.Table)
	require.Equal(t, "", jsonMsg.Query)
	require.False(t, jsonMsg.IsDDL)
	require.EqualValues(t, 0, jsonMsg.ExecutionTime)
	require.Nil(t, jsonMsg.Data)
	require.Nil(t, jsonMsg.Old)
	require.Nil(t, jsonMsg.SQLType)
	require.Nil(t, jsonMsg.MySQLType)
}

func TestDDLEventWithExtension(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)
	require.NotNil(t, encoder)

	sql := `create table test.person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))`
	ddlEvent := helper.DDL2Event(sql)

	message, err := encoder.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	decoder.AddKeyValue(message.Key, message.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, messageType, common.MessageTypeDDL)

	decodedDDL := decoder.NextDDLEvent()
	require.NoError(t, err)
	require.Equal(t, ddlEvent.Query, decodedDDL.Query)
	require.Equal(t, ddlEvent.GetCommitTs(), decodedDDL.GetCommitTs())
	require.Equal(t, ddlEvent.SchemaName, decodedDDL.SchemaName)
	require.Equal(t, ddlEvent.TableName, decodedDDL.TableName)
}

func TestCanalJSONAppendRowChangedEventWithCallback(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a varchar(255) primary key)`
	_ = helper.DDL2Event(sql)

	sql = `insert into test.t values ("aa")`
	dml := helper.DML2Event("test", "t", sql)
	rc, ok := dml.GetNextRow()
	dml.Rewind()
	require.True(t, ok)

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	ctx := context.Background()

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	count := 0
	tests := []struct {
		row      commonEvent.RowChange
		callback func()
	}{
		{
			row: rc,
			callback: func() {
				count += 1
			},
		},
		{
			row: rc,
			callback: func() {
				count += 2
			},
		},
		{
			row: rc,
			callback: func() {
				count += 3
			},
		},
		{
			row: rc,
			callback: func() {
				count += 4
			},
		},
		{
			row: rc,
			callback: func() {
				count += 5
			},
		},
	}

	// Empty build makes sure that the callback build logic not broken.
	msgs := encoder.Build()
	require.Len(t, msgs, 0, "no message should be built and no panic")

	// Append the events.
	for _, test := range tests {
		err := encoder.AppendRowChangedEvent(context.Background(), "", &commonEvent.RowEvent{
			TableInfo:      dml.TableInfo,
			CommitTs:       dml.CommitTs,
			Event:          test.row,
			ColumnSelector: columnselector.NewDefaultColumnSelector(),
			Callback:       test.callback,
		})
		require.NoError(t, err)
	}
	require.Equal(t, 0, count, "nothing should be called")

	msgs = encoder.Build()
	require.Len(t, msgs, 5, "expected 5 messages")
	msgs[0].Callback()
	require.Equal(t, 1, count, "expected one callback be called")
	msgs[1].Callback()
	require.Equal(t, 3, count, "expected one callback be called")
	msgs[2].Callback()
	require.Equal(t, 6, count, "expected one callback be called")
	msgs[3].Callback()
	require.Equal(t, 10, count, "expected one callback be called")
	msgs[4].Callback()
	require.Equal(t, 15, count, "expected one callback be called")
}

func TestMaxMessageBytes(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a varchar(255) primary key)`
	_ = helper.DDL2Event(sql)

	sql = `insert into test.t values ("aa")`
	dml := helper.DML2Event("test", "t", sql)
	rc, ok := dml.GetNextRow()
	dml.Rewind()
	require.True(t, ok)

	ctx := context.Background()
	topic := ""

	// the test message length is smaller than max-message-bytes
	maxMessageBytes := 300
	codecConfig := common.NewConfig(config.ProtocolCanalJSON).WithMaxMessageBytes(maxMessageBytes)

	encIface, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := encIface.(*JSONRowEventEncoder)

	err = encoder.AppendRowChangedEvent(ctx, topic, &commonEvent.RowEvent{
		TableInfo:      dml.TableInfo,
		CommitTs:       dml.CommitTs,
		Event:          rc,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	})
	require.NoError(t, err)

	// the test message length is larger than max-message-bytes
	codecConfig = codecConfig.WithMaxMessageBytes(100)

	encIface, err = NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	encoder = encIface.(*JSONRowEventEncoder)
	err = encoder.AppendRowChangedEvent(ctx, topic, &commonEvent.RowEvent{
		TableInfo:      dml.TableInfo,
		CommitTs:       dml.CommitTs,
		Event:          rc,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	})
	require.Error(t, err, cerror.ErrMessageTooLarge)
}

func TestCanalJSONContentCompatibleE2E(t *testing.T) {
	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	codecConfig.ContentCompatible = true
	codecConfig.OnlyOutputUpdatedColumns = true

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	_, insertEvent, updateEvent, deleteEvent := common.NewLargeEvent4Test(t)
	events := []*commonEvent.RowEvent{insertEvent, updateEvent, deleteEvent}

	for _, event := range events {
		err = encoder.AppendRowChangedEvent(ctx, "", event)
		require.NoError(t, err)

		message := encoder.Build()[0]

		decoder.AddKeyValue(message.Key, message.Value)

		messageType, hasNext := decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, messageType, common.MessageTypeRow)

		decodedEvent := decoder.NextDMLEvent()
		require.NoError(t, err)
		require.Equal(t, decodedEvent.CommitTs, event.CommitTs)
		require.Equal(t, decodedEvent.TableInfo.GetSchemaName(), event.TableInfo.GetSchemaName())
		require.Equal(t, decodedEvent.TableInfo.GetTableName(), event.TableInfo.GetTableName())

		change, ok := decodedEvent.GetNextRow()
		require.True(t, ok)
		common.CompareRow(t, event.Event, event.TableInfo, change, decodedEvent.TableInfo)
	}
}

func TestE2EPartitionTableByHash(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	createTableDDLEvent := helper.DDL2Event(`CREATE TABLE t (a INT,PRIMARY KEY(a)) PARTITION BY HASH (a) PARTITIONS 5`)
	require.NotNil(t, createTableDDLEvent)
	insertEvent := helper.DML2Event4PartitionTable("test", "t", "p0", `insert into t values (5)`)
	require.NotNil(t, insertEvent)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	message, err := encoder.EncodeDDLEvent(createTableDDLEvent)
	require.NoError(t, err)

	decoder.AddKeyValue(message.Key, message.Value)

	tp, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, tp)

	decodedDDL := decoder.NextDDLEvent()
	require.NotNil(t, decodedDDL)

	rc, ok := insertEvent.GetNextRow()
	insertEvent.Rewind()
	require.True(t, ok)
	err = encoder.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
		TableInfo:      insertEvent.TableInfo,
		CommitTs:       insertEvent.CommitTs,
		Event:          rc,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	})
	require.NoError(t, err)
	message = encoder.Build()[0]

	decoder.AddKeyValue(message.Key, message.Value)
	tp, hasNext = decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, tp)

	decodedEvent := decoder.NextDMLEvent()
	require.NotZero(t, decodedEvent.GetTableID())
}

func TestE2EPartitionTableByRange(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	createTableDDLEvent := helper.DDL2Event(`create table t (id int primary key, a int) PARTITION BY RANGE ( id ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (21))`)
	require.NotNil(t, createTableDDLEvent)

	insertEvent := helper.DML2Event4PartitionTable("test", "t", "p1", `insert into t (id) values (6)`)
	require.NotNil(t, insertEvent)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	message, err := encoder.EncodeDDLEvent(createTableDDLEvent)
	require.NoError(t, err)

	decoder.AddKeyValue(message.Key, message.Value)

	tp, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, tp)

	decodedDDL := decoder.NextDDLEvent()
	require.NotNil(t, decodedDDL)

	rc, ok := insertEvent.GetNextRow()
	insertEvent.Rewind()
	require.True(t, ok)
	err = encoder.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
		TableInfo:      insertEvent.TableInfo,
		CommitTs:       insertEvent.CommitTs,
		Event:          rc,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
	})
	require.NoError(t, err)
	message = encoder.Build()[0]

	decoder.AddKeyValue(message.Key, message.Value)
	tp, hasNext = decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, tp)

	decodedEvent := decoder.NextDMLEvent()
	require.NotZero(t, decodedEvent.GetTableID())
}

func TestE2EPartitionTable(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	createPartitionTableDDL := helper.DDL2Event(`create table test.t(a int primary key, b int) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than MAXVALUE)`)
	require.NotNil(t, createPartitionTableDDL)

	insertEvent := helper.DML2Event4PartitionTable("test", "t", "p0", `insert into test.t values (1, 1)`)
	require.NotNil(t, insertEvent)
	insertEvent1 := helper.DML2Event4PartitionTable("test", "t", "p1", `insert into test.t values (11, 11)`)
	require.NotNil(t, insertEvent1)

	insertEvent2 := helper.DML2Event4PartitionTable("test", "t", "p2", `insert into test.t values (21, 21)`)
	require.NotNil(t, insertEvent2)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	for _, enableTiDBExtension := range []bool{false, true} {
		codecConfig.EnableTiDBExtension = enableTiDBExtension

		encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
		require.NoError(t, err)

		decoder, err := NewDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)

		message, err := encoder.EncodeDDLEvent(createPartitionTableDDL)
		require.NoError(t, err)

		decoder.AddKeyValue(message.Key, message.Value)

		tp, hasNext := decoder.HasNext()
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeDDL, tp)

		decodedDDL := decoder.NextDDLEvent()
		require.NotNil(t, decodedDDL)

		rc, ok := insertEvent.GetNextRow()
		insertEvent.Rewind()
		require.True(t, ok)
		err = encoder.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
			TableInfo:      insertEvent.TableInfo,
			CommitTs:       insertEvent.CommitTs,
			Event:          rc,
			ColumnSelector: columnselector.NewDefaultColumnSelector(),
		})
		require.NoError(t, err)
		message = encoder.Build()[0]

		decoder.AddKeyValue(message.Key, message.Value)
		tp, hasNext = decoder.HasNext()
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeRow, tp)

		decodedEvent := decoder.NextDMLEvent()
		require.NotZero(t, decodedEvent.GetTableID())

		rc, ok = insertEvent1.GetNextRow()
		insertEvent1.Rewind()
		require.True(t, ok)
		err = encoder.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
			TableInfo:      insertEvent1.TableInfo,
			CommitTs:       insertEvent1.CommitTs,
			Event:          rc,
			ColumnSelector: columnselector.NewDefaultColumnSelector(),
		})
		require.NoError(t, err)
		message = encoder.Build()[0]

		decoder.AddKeyValue(message.Key, message.Value)
		tp, hasNext = decoder.HasNext()
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeRow, tp)

		decodedEvent = decoder.NextDMLEvent()
		require.NotZero(t, decodedEvent.GetTableID())

		rc, ok = insertEvent2.GetNextRow()
		insertEvent2.Rewind()
		require.True(t, ok)
		err = encoder.AppendRowChangedEvent(ctx, "", &commonEvent.RowEvent{
			TableInfo:      insertEvent2.TableInfo,
			CommitTs:       insertEvent2.CommitTs,
			Event:          rc,
			ColumnSelector: columnselector.NewDefaultColumnSelector(),
		})
		require.NoError(t, err)
		message = encoder.Build()[0]

		decoder.AddKeyValue(message.Key, message.Value)
		tp, hasNext = decoder.HasNext()
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeRow, tp)

		decodedEvent = decoder.NextDMLEvent()

		require.NotZero(t, decodedEvent.GetTableID())
	}
}
