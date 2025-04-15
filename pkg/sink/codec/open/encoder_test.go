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
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestEncodeFlag(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	createTableDDL := `create table t(
    	a int primary key,
    	b int not null,
    	c int,
    	d int unsigned,
    	e blob,
    	unique key idx(b, c),
    	key idx2(c, d)
    )`
	job := helper.DDL2Job(createTableDDL)
	tableInfo := helper.GetTableInfo(job)

	dmlEvent := helper.DML2Event("test", "t",
		`insert into t values (1, 2, 3, 4, "0x010201")`)
	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	columnSelector := columnselector.NewDefaultColumnSelector()

	insertEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       dmlEvent.GetCommitTs(),
		Event:          row,
		ColumnSelector: columnSelector,
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen)

	enc, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = enc.AppendRowChangedEvent(ctx, "", insertEvent)
	require.NoError(t, err)

	messages := enc.Build()
	require.Len(t, messages, 1)
	require.NotEmpty(t, messages[0])

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(messages[0].Key, messages[0].Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	decoded, err := decoder.NextDMLEvent()
	require.NoError(t, err)

	change, ok := decoded.GetNextRow()
	require.True(t, ok)
	common.CompareRow(t, insertEvent.Event, insertEvent.TableInfo, change, decoded.TableInfo)
}

func TestIntegerTypes(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	createTableDDL := `create table test.t(
		id int primary key auto_increment,
 		a tinyint, b tinyint unsigned,
 		c smallint, d smallint unsigned,
 		e mediumint, f mediumint unsigned,
 		g int, h int unsigned,
 		i bigint, j bigint unsigned)`

	job := helper.DDL2Job(createTableDDL)
	tableInfo := helper.GetTableInfo(job)

	sql := `insert into test.t values(
		1,
		-128, 0,
		-32768, 0,
		-8388608, 0,
		-2147483648, 0,
		-9223372036854775808, 0)`
	minValues := helper.DML2Event("test", "t", sql)
	minRow, ok := minValues.GetNextRow()
	require.True(t, ok)

	columnSelector := columnselector.NewDefaultColumnSelector()

	minValueEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       minValues.GetCommitTs(),
		Event:          minRow,
		ColumnSelector: columnSelector,
		Callback:       func() {},
	}

	sql = `insert into test.t values (
		2,
		127, 255,
		32767, 65535,
		8388607, 16777215,
		2147483647, 4294967295,
	9223372036854775807, 18446744073709551615)`
	maxValues := helper.DML2Event("test", "t", sql)
	maxRow, ok := maxValues.GetNextRow()

	maxValueEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       maxValues.GetCommitTs(),
		Event:          maxRow,
		ColumnSelector: columnSelector,
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen)
	for _, event := range []*commonEvent.RowEvent{minValueEvent, maxValueEvent} {
		encoder, err := NewBatchEncoder(ctx, codecConfig)
		require.NoError(t, err)

		err = encoder.AppendRowChangedEvent(ctx, "", event)
		require.NoError(t, err)

		messages := encoder.Build()
		require.Len(t, messages, 1)

		decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)

		err = decoder.AddKeyValue(messages[0].Key, messages[0].Value)
		require.NoError(t, err)

		messageType, hasNext, err := decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeRow, messageType)

		decoded, err := decoder.NextDMLEvent()
		require.NoError(t, err)

		require.Equal(t, event.CommitTs, decoded.GetCommitTs())

		change, ok := decoded.GetNextRow()
		require.True(t, ok)

		common.CompareRow(t, event.Event, event.TableInfo, change, decoded.TableInfo)
	}
}

func TestFloatTypes(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(
    	id int primary key auto_increment,
	    a float, b float(10, 3), c float(10), 
	    d double, e double(20, 3))`)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a,b,c,d,e) values (1.23, 4.56, 7.89, 10.11, 12.13)`)
	require.NotNil(t, dmlEvent)
	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	tableInfo := helper.GetTableInfo(job)
	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen)

	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event, err := decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok := event.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
}

func TestTimeTypes(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(id int primary key auto_increment, a timestamp, b datetime, c date, d time)`)

	dmlEvent := helper.DML2Event("test", "t",
		`insert into test.t(a,b,c,d) values ("2020-01-01 12:00:00", "2020-01-01 12:00:00", "2020-01-01", "12:00:00")`)
	require.NotNil(t, dmlEvent)
	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	tableInfo := helper.GetTableInfo(job)
	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen)

	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event, err := decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok := event.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
}

func TestStringTypes(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(
    	id int primary key auto_increment, a char(10) , b varchar(10), c binary(10), d varbinary(10))`)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a,b,c,d) values ("char","varchar","binary","varbinary")`)
	require.NotNil(t, dmlEvent)
	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	tableInfo := helper.GetTableInfo(job)
	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen)

	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event, err := decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok := event.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
}

func TestBlobTypes(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(
    	id int primary key auto_increment,
		a tinyblob, b blob, c mediumblob, d longblob)`)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a,b,c,d) values (0x010201,0x010202,0x010203,0x010204)`)
	require.NotNil(t, dmlEvent)
	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	tableInfo := helper.GetTableInfo(job)
	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen)

	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event, err := decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok := event.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
}

func TestTextTypes(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(
    	id int primary key auto_increment,
		a tinytext, b text, c mediumtext, d longtext)`)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a,b,c,d) values ("tinytext","text","mediumtext","longtext")`)
	require.NotNil(t, dmlEvent)
	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	tableInfo := helper.GetTableInfo(job)
	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen)

	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event, err := decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok := event.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
}

func TestOtherTypes(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(
    	id int primary key auto_increment, 
    	a bool, b bool, c year,
		d bit(10), e json, 
		f decimal(10,2), 
		g enum('a','b','c'), h set('a','b','c'))`)
	tableInfo := helper.GetTableInfo(job)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a, b, c, d, e, f, g, h) values (
   		true, false, 2000, 
	    0b0101010101, '{"key1": "value1"}', 
	    153.123, 
	    'a', 'a,b')`)

	require.NotNil(t, dmlEvent)
	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen)
	codecConfig.ContentCompatible = true

	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event, err := decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok := event.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
}

func TestEncodeCheckpoint(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolOpen)
	ctx := context.Background()
	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	checkpoint := uint64(12345678)
	m, err := encoder.EncodeCheckpointEvent(checkpoint)
	require.NoError(t, err)

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, messageType, common.MessageTypeResolved)

	obtained, err := decoder.NextResolvedEvent()
	require.NoError(t, err)

	require.Equal(t, checkpoint, obtained)
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
	require.Equal(t, count, 0)
}

func TestLargeMessageWithHandleEnableHandleKeyOnly(t *testing.T) {
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

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(168)
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionHandleKeyOnly
	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

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

	require.Len(t, decoded.TableInfo.GetColumns(), 1)
	require.Equal(t, "a", decoded.TableInfo.GetColumns()[0].Name.O)

	originColID := insertRowEvent.TableInfo.ForceGetColumnIDByName("a")
	originColOffset := insertRowEvent.TableInfo.MustGetColumnOffsetByID(originColID)
	expected := insertRowEvent.GetRows().GetDatumRow(insertRowEvent.TableInfo.GetFieldSlice())[originColOffset]

	obtained := change.Row.GetDatumRow(decoded.TableInfo.GetFieldSlice())
	require.Equal(t, expected, obtained[0])
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

func TestDMLEventWithColumnSelector(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b tinyint)`)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a) values (1)`)
	require.NotNil(t, dmlEvent)
	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	tableInfo := helper.GetTableInfo(job)

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.ColumnSelectors = []*config.ColumnSelector{
		{
			Matcher: []string{"test.*"},
			Columns: []string{"a"},
		},
	}
	selectors, err := columnselector.NewColumnSelectors(replicaConfig.Sink)
	require.NoError(t, err)

	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: selectors.GetSelector("test", "t"),
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen)

	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event, err := decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok := event.GetNextRow()
	require.True(t, ok)

	require.Len(t, event.TableInfo.GetColumns(), 1)
	require.Equal(t, "a", event.TableInfo.GetColumns()[0].Name.O)

	origin := rowEvent.GetRows().GetDatumRow(rowEvent.TableInfo.GetFieldSlice())

	originColID := rowEvent.TableInfo.ForceGetColumnIDByName("a")
	originColOffset := rowEvent.TableInfo.MustGetColumnOffsetByID(originColID)
	expected := origin[originColOffset]

	obtained := change.Row.GetDatumRow(event.TableInfo.GetFieldSlice())
	require.Equal(t, expected, obtained[0])
}

// Including insert / update / delete
func TestDMLEvent(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)
	tableInfo := helper.GetTableInfo(job)

	// Insert
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

	// Update
	dmlEvent = helper.DML2Event("test", "t", `update test.t set b = 456 where a = 1`)
	require.NotNil(t, dmlEvent)
	updateRow, ok := dmlEvent.GetNextRow()
	updateRow.PreRow = insertRow.Row
	require.True(t, ok)

	updateRowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       dmlEvent.GetCommitTs(),
		Event:          updateRow,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	deleteRow := updateRow
	deleteRow.PreRow = updateRow.Row
	deleteRow.Row = chunk.Row{}
	require.True(t, ok)

	deleteEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       dmlEvent.GetCommitTs(),
		Event:          deleteRow,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen)

	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)
	for _, origin := range []*commonEvent.RowEvent{
		insertRowEvent,
		updateRowEvent,
		deleteEvent,
	} {
		err = encoder.AppendRowChangedEvent(ctx, "", origin)
		require.NoError(t, err)

		m := encoder.Build()[0]

		err = decoder.AddKeyValue(m.Key, m.Value)
		require.NoError(t, err)

		messageType, hasNext, err := decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeRow, messageType)

		decoded, err := decoder.NextDMLEvent()
		require.NoError(t, err)
		change, ok := decoded.GetNextRow()
		require.True(t, ok)

		common.CompareRow(t, origin.Event, origin.TableInfo, change, decoded.TableInfo)
	}
}

func TestOnlyOutputUpdatedEvent(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int, c decimal(10,2), d json, e char(10), f binary(10), g blob)`)
	event := helper.DML2Event("test", "t", `insert into test.t values (1, 123, 123.12, '{"key1": "value1"}',"Alice",0x0102030405060708090A,0x4944330300000000)`)
	eventNew := helper.DML2Event("test", "t", `update test.t set b = 456,c = 456.45 where a = 1`)
	tableInfo := helper.GetTableInfo(job)

	preRow, _ := event.GetNextRow()
	row, _ := eventNew.GetNextRow()
	row.PreRow = preRow.Row

	updateRowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen)
	codecConfig.OnlyOutputUpdatedColumns = true

	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", updateRowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	decoded, err := decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok := decoded.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, updateRowEvent.Event, updateRowEvent.TableInfo, change, decoded.TableInfo)
}

func TestHandleOnlyEvent(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)

	tableInfo := helper.GetTableInfo(job)
	// Insert
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

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen)

	encoder, err := NewBatchEncoder(ctx, codecConfig)
	require.NoError(t, err)

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", insertRowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	err = decoder.AddKeyValue(m.Key, m.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	decoded, err := decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok := decoded.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, insertRowEvent.Event, insertRowEvent.TableInfo, change, decoded.TableInfo)
}
