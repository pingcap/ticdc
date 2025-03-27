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
	"testing"

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

// TODO: claim check

func CompareRow(
	t *testing.T,
	origin commonEvent.RowChange,
	originTableInfo *commonType.TableInfo,
	obtained commonEvent.RowChange,
	obtainedTableInfo *commonType.TableInfo,
) {
	if !origin.Row.IsEmpty() {
		a := origin.Row.GetDatumRow(originTableInfo.GetFieldSlice())
		b := obtained.Row.GetDatumRow(obtainedTableInfo.GetFieldSlice())
		require.Equal(t, len(a), len(b))
		for idx, col := range originTableInfo.GetColumns() {
			colID := obtainedTableInfo.ForceGetColumnIDByName(col.Name.O)
			offset := obtainedTableInfo.MustGetColumnOffsetByID(colID)
			require.Equal(t, a[idx], b[offset])
		}
	}

	if !origin.PreRow.IsEmpty() {
		a := origin.PreRow.GetDatumRow(originTableInfo.GetFieldSlice())
		b := obtained.PreRow.GetDatumRow(obtainedTableInfo.GetFieldSlice())
		require.Equal(t, len(a), len(b))
		for idx, col := range originTableInfo.GetColumns() {
			colID := obtainedTableInfo.ForceGetColumnIDByName(col.Name.O)
			offset := obtainedTableInfo.MustGetColumnOffsetByID(colID)
			require.Equal(t, a[idx], b[offset])
		}
	}
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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.ContentCompatible = true
	for _, enableTiDBExtension := range []bool{true, false} {
		for _, event := range []*commonEvent.RowEvent{minValueEvent, maxValueEvent} {
			codecConfig.EnableTiDBExtension = enableTiDBExtension
			encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
			require.NoError(t, err)

			err = encoder.AppendRowChangedEvent(ctx, "", event)
			require.NoError(t, err)

			messages := encoder.Build()
			require.Len(t, messages, 1)

			decoder, err := NewCanalJSONDecoder(ctx, codecConfig, nil)
			require.NoError(t, err)

			err = decoder.AddKeyValue(messages[0].Key, messages[0].Value)
			require.NoError(t, err)

			messageType, hasNext, err := decoder.HasNext()
			require.NoError(t, err)
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeRow, messageType)

			decoded, err := decoder.NextDMLEvent()
			require.NoError(t, err)

			if enableTiDBExtension {
				require.Equal(t, event.CommitTs, decoded.GetCommitTs())
			}

			change, ok := decoded.GetNextRow()
			require.True(t, ok)

			CompareRow(t, event.Event, event.TableInfo, change, decoded.TableInfo)
		}
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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewCanalJSONDecoder(ctx, codecConfig, nil)
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

	CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewCanalJSONDecoder(ctx, codecConfig, nil)
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

	CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewCanalJSONDecoder(ctx, codecConfig, nil)
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

	CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewCanalJSONDecoder(ctx, codecConfig, nil)
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

	CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewCanalJSONDecoder(ctx, codecConfig, nil)
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

	CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.ContentCompatible = true

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewCanalJSONDecoder(ctx, codecConfig, nil)
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

	CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewCanalJSONDecoder(ctx, codecConfig, nil)
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

func TestDMLMultiplePK(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(a tinyint, c int, b tinyint, PRIMARY KEY (a, b))`)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values (1,2,3)`)
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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.ContentCompatible = true

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewCanalJSONDecoder(ctx, codecConfig, nil)
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

	CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
}

func TestDMLMessageTooLarge(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(
			a tinyint primary key, b tinyint,
			c bool, d bool,
			e smallint, f smallint,
			g int, h int,
			i float, j float,
			k double, l double,
			m timestamp, n timestamp,
			o bigint, p bigint,
			q mediumint, r mediumint,
			s date, t date,
			u time, v time,
			w datetime, x datetime,
			y year, z year,
			aa varchar(10), ab varchar(10),
			ac varbinary(10), ad varbinary(10),
			ae bit(10), af bit(10),
			ag json, ah json,
			ai decimal(10,2), aj decimal(10,2),
			ak enum('a','b','c'), al enum('a','b','c'),
			am set('a','b','c'), an set('a','b','c'),
			ao tinytext, ap tinytext,
			aq tinyblob, ar tinyblob,
			as1 mediumtext, at mediumtext,
			au mediumblob, av mediumblob,
			aw longtext, ax longtext,
			ay longblob, az longblob,
			ba text, bb text,
			bc blob, bd blob,
			be char(10), bf char(10),
			bg binary(10), bh binary(10))`)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(
			a,c,e,g,i,k,m,o,q,s,u,w,y,aa,ac,ae,ag,ai,ak,am,ao,aq,as1,au,aw,ay,ba,bc,be,bg) values (
				1, true, -1, 123, 153.123,153.123,
				"1973-12-30 15:30:00",123,123,"2000-01-01","23:59:59",
				"2015-12-20 23:58:58",1970,"测试",0x0102030405060708090A,81,
				'{"key1": "value1"}', 129012.12, 'a', 'b', "5rWL6K+VdGV4dA==",
				0x89504E470D0A1A0A,"5rWL6K+VdGV4dA==",0x4944330300000000,
				"5rWL6K+VdGV4dA==",0x504B0304140000000800,"5rWL6K+VdGV4dA==",
				0x255044462D312E34,"Alice",0x0102030405060708090A)`)
	require.NotNil(t, dmlEvent)
	tableInfo := helper.GetTableInfo(job)

	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig = codecConfig.WithMaxMessageBytes(300)
	codecConfig.EnableTiDBExtension = true
	encoder, err := NewJSONRowEventEncoder(context.Background(), codecConfig)
	require.NoError(t, err)
	err = encoder.AppendRowChangedEvent(context.Background(), "", rowEvent)
	require.ErrorIs(t, err, errors.ErrMessageTooLarge)
}

func TestMessageLargeHandleKeyOnly(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(
			a tinyint primary key, b tinyint,
			c bool, d bool,
			e smallint, f smallint,
			g int, h int,
			i float, j float,
			k double, l double,
			m timestamp, n timestamp,
			o bigint, p bigint,
			q mediumint, r mediumint,
			s date, t date,
			u time, v time,
			w datetime, x datetime,
			y year, z year,
			aa varchar(10), ab varchar(10),
			ac varbinary(10), ad varbinary(10),
			ae bit(10), af bit(10),
			ag json, ah json,
			ai decimal(10,2), aj decimal(10,2),
			ak enum('a','b','c'), al enum('a','b','c'),
			am set('a','b','c'), an set('a','b','c'),
			ao tinytext, ap tinytext,
			aq tinyblob, ar tinyblob,
			as1 mediumtext, at mediumtext,
			au mediumblob, av mediumblob,
			aw longtext, ax longtext,
			ay longblob, az longblob,
			ba text, bb text,
			bc blob, bd blob,
			be char(10), bf char(10),
			bg binary(10), bh binary(10))`)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(
			a,c,e,g,i,k,m,o,q,s,u,w,y,aa,ac,ae,ag,ai,ak,am,ao,aq,as1,au,aw,ay,ba,bc,be,bg) values (
				1, true, -1, 123, 153.123,153.123,
				"1973-12-30 15:30:00",123,123,"2000-01-01","23:59:59",
				"2015-12-20 23:58:58",1970,"测试",0x0102030405060708090A,81,
				'{"key1": "value1"}', 129012.12, 'a', 'b', "5rWL6K+VdGV4dA==",
				0x89504E470D0A1A0A,"5rWL6K+VdGV4dA==",0x4944330300000000,
				"5rWL6K+VdGV4dA==",0x504B0304140000000800,"5rWL6K+VdGV4dA==",
				0x255044462D312E34,"Alice",0x0102030405060708090A)`)
	require.NotNil(t, dmlEvent)
	tableInfo := helper.GetTableInfo(job)

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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON).WithMaxMessageBytes(300)
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionHandleKeyOnly
	codecConfig.EnableTiDBExtension = true

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]
	require.NotNil(t, m.Callback)

	decoder, err := NewCanalJSONDecoder(ctx, codecConfig, nil)
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

func TestDMLTypeEvent(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b tinyint)`)
	tableInfo := helper.GetTableInfo(job)

	insert := helper.DML2Event("test", "t", `insert into test.t(a,b) values (1,3)`)
	require.NotNil(t, insert)
	insertRow, ok := insert.GetNextRow()
	require.True(t, ok)

	columnSelector := columnselector.NewDefaultColumnSelector()
	insertEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       insert.GetCommitTs(),
		Event:          insertRow,
		ColumnSelector: columnSelector,
		Callback:       func() {},
	}

	update := helper.DML2Event("test", "t", `update test.t set b = 2 where a = 1`)
	require.NotNil(t, update)
	updateRow, ok := update.GetNextRow()
	require.True(t, ok)
	updateRow.PreRow = insertRow.Row

	updateEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       update.GetCommitTs(),
		Event:          updateRow,
		ColumnSelector: columnSelector,
		Callback:       func() {},
	}

	// delete
	deleteRow := updateRow
	deleteRow.PreRow = updateRow.Row
	deleteRow.Row = chunk.Row{}

	deleteEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       update.GetCommitTs(),
		Event:          deleteRow,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	decoder, err := NewCanalJSONDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	for _, event := range []*commonEvent.RowEvent{
		insertEvent,
		deleteEvent,
		updateEvent,
	} {

		err = encoder.AppendRowChangedEvent(ctx, "", event)
		require.NoError(t, err)

		message := encoder.Build()[0]

		err = decoder.AddKeyValue(message.Key, message.Value)
		require.NoError(t, err)

		messageType, hasNext, err := decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeRow, messageType)

		decoded, err := decoder.NextDMLEvent()
		require.NoError(t, err)
		change, ok := decoded.GetNextRow()
		require.True(t, ok)

		CompareRow(t, event.Event, event.TableInfo, change, decoded.TableInfo)
	}

	// update with only updated columns
	codecConfig.OnlyOutputUpdatedColumns = true
	encoder, err = NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", updateEvent)
	require.NoError(t, err)

	message := encoder.Build()[0]

	decoder, err = NewCanalJSONDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	decoded, err := decoder.NextDMLEvent()
	require.NoError(t, err)
	change, ok := decoded.GetNextRow()
	require.True(t, ok)

	CompareRow(t, updateEvent.Event, updateEvent.TableInfo, change, decoded.TableInfo)
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

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	ctx := context.Background()

	for _, enableTiDBExtension := range []bool{false, true} {
		codecConfig.EnableTiDBExtension = enableTiDBExtension
		encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
		require.NoError(t, err)

		message, err := encoder.EncodeDDLEvent(ddlEvent)
		require.NoError(t, err)

		decoder, err := NewCanalJSONDecoder(ctx, codecConfig, nil)
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
		if enableTiDBExtension {
			require.Equal(t, ddlEvent.FinishedTs, obtained.FinishedTs)
		}
	}
}

// checkpointTs
func TestCheckpointTs(t *testing.T) {
	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	watermark := uint64(179394)
	message, err := encoder.EncodeCheckpointEvent(watermark)
	require.NoError(t, err)
	require.Nil(t, message)

	// with extension
	codecConfig.EnableTiDBExtension = true
	encoder, err = NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)
	message, err = encoder.EncodeCheckpointEvent(watermark)
	require.NoError(t, err)

	decoder, err := NewCanalJSONDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeResolved, messageType)

	obtained, err := decoder.NextResolvedEvent()
	require.NoError(t, err)
	require.Equal(t, watermark, obtained)
}
