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

package open

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// normal message 1
// normal message multiple to merge
// large message (with handle / claim)

func readByteToUint(b []byte) uint64 {
	var value uint64
	log.Info("read byte", zap.Any("b", b[:]))
	buf := bytes.NewReader(b[:])
	err := binary.Read(buf, binary.BigEndian, &value)
	if err != nil {
		log.Error("Error reading int64 from byte slice:", zap.Any("error", err))
		return 0
	}
	return value
}

func TestEncodeCheckpoint(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolOpen)
	ctx := context.Background()
	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	m, err := encoder.EncodeCheckpointEvent(12345678)
	require.NoError(t, err)

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, messageType, common.MessageTypeResolved)

	checkpoint, err := decoder.NextResolvedEvent()
	require.NoError(t, err)

	require.Equal(t, 12345678, checkpoint)
}

func TestCreateTableDDL(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)
	require.NotNil(t, job)

	ddlEvent := &commonEvent.DDLEvent{
		Query:      job.Query,
		Type:       byte(job.Type),
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 1,
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen)

	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	message, err := encoder.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, messageType)

	obtained, err := decoder.NextDDLEvent()
	require.NoError(t, err)
	require.Equal(t, ddlEvent.Query, obtained.Query)
	require.Equal(t, ddlEvent.Type, obtained.Type)
	require.Equal(t, ddlEvent.SchemaName, obtained.SchemaName)
	require.Equal(t, ddlEvent.TableName, obtained.TableName)
	require.Equal(t, ddlEvent.FinishedTs, obtained.FinishedTs)
}

func TestEncoderOneMessage(t *testing.T) {
	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen)
	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)

	tableInfo := helper.GetTableInfo(job)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 123)`)
	require.NotNil(t, dmlEvent)
	insertRow, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	count := 0

	insertRowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       dmlEvent.GetCommitTs(),
		Event:          insertRow,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() { count += 1 },
	}

	err = encoder.AppendRowChangedEvent(ctx, "", insertRowEvent)
	require.NoError(t, err)

	messages := encoder.Build()

	require.Equal(t, 1, len(messages))
	require.Equal(t, 1, messages[0].GetRowsCount())

	message := messages[0]
	message.Callback()
	require.Equal(t, 1, count)

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(messages[0].Key, messages[0].Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, messageType, common.MessageTypeRow)

	decoded, err := decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok := decoded.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, insertRowEvent.Event, insertRowEvent.TableInfo, change, decoded.TableInfo)
}

func TestEncoderMultipleMessage(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)
	tableInfo := helper.GetTableInfo(job)

	dmlEvent := helper.DML2Event("test", "t",
		`insert into test.t values (1, 123)`,
		`insert into test.t values (2, 223)`,
		`insert into test.t values (3, 333)`)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(400)
	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	insertEvents := make([]*commonEvent.RowEvent, 0, 3)
	columnSelector := columnselector.NewDefaultColumnSelector()
	count := 0
	for {
		insertRow, ok := dmlEvent.GetNextRow()
		if !ok {
			break
		}

		insertRowEvent := &commonEvent.RowEvent{
			TableInfo:      tableInfo,
			CommitTs:       dmlEvent.GetCommitTs(),
			Event:          insertRow,
			ColumnSelector: columnSelector,
			Callback:       func() { count += 1 },
		}
		insertEvents = append(insertEvents, insertRowEvent)

		err = encoder.AppendRowChangedEvent(ctx, "", insertRowEvent)
		require.NoError(t, err)
	}
	messages := encoder.Build()

	require.Equal(t, 2, len(messages))
	require.Equal(t, 2, messages[0].GetRowsCount())
	require.Equal(t, 1, messages[1].GetRowsCount())

	for _, message := range messages {
		message.Callback()
	}

	require.Equal(t, 3, count)

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(messages[0].Key, messages[0].Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, messageType, common.MessageTypeRow)

	decoded, err := decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok := decoded.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, insertEvents[0].Event, insertEvents[0].TableInfo, change, decoded.TableInfo)

	messageType, hasNext, err = decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, messageType, common.MessageTypeRow)

	decoded, err = decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok = decoded.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, insertEvents[1].Event, insertEvents[1].TableInfo, change, decoded.TableInfo)

	err = decoder.AddKeyValue(messages[1].Key, messages[1].Value)
	require.NoError(t, err)

	messageType, hasNext, err = decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, messageType, common.MessageTypeRow)

	decoded, err = decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok = decoded.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, insertEvents[2].Event, insertEvents[2].TableInfo, change, decoded.TableInfo)
}

func TestMessageTooLarge(t *testing.T) {
	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(100)
	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)

	tableInfo := helper.GetTableInfo(job)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 123)`)
	require.NotNil(t, dmlEvent)
	insertRow, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	count := 0
	insertRowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       dmlEvent.GetCommitTs(),
		Event:          insertRow,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() { count += 1 },
	}

	err = encoder.AppendRowChangedEvent(ctx, "", insertRowEvent)
	require.ErrorIs(t, err, errors.ErrMessageTooLarge)
}

func TestLargeMessageWithHandleKeyOnly(t *testing.T) {
	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(150)
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionHandleKeyOnly
	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)

	tableInfo := helper.GetTableInfo(job)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 123)`)
	require.NotNil(t, dmlEvent)
	insertRow, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	insertRowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       dmlEvent.GetCommitTs(),
		Event:          insertRow,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	err = encoder.AppendRowChangedEvent(ctx, "", insertRowEvent)
	require.NoError(t, err)

	messages := encoder.Build()

	require.Equal(t, 1, len(messages))
	require.Equal(t, 1, messages[0].GetRowsCount())

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	message := messages[0]
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, messageType, common.MessageTypeRow)

	decoded, err := decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok := decoded.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, insertRowEvent.Event, insertRowEvent.TableInfo, change, decoded.TableInfo)
}

func TestLargeMessageWithoutHandle(t *testing.T) {
	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(150)
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionHandleKeyOnly
	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint, b int)`)

	tableInfo := helper.GetTableInfo(job)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1, 123)`)
	require.NotNil(t, dmlEvent)
	insertRow, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	insertRowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          insertRow,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	err = encoder.AppendRowChangedEvent(ctx, "", insertRowEvent)
	require.ErrorIs(t, err, errors.ErrOpenProtocolCodecInvalidData)
}
