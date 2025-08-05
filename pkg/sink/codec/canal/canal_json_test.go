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
	"encoding/json"
	"testing"

	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestIntegerContentCompatible(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	createTableSQL := `	create table tp_int
	(
		id          int auto_increment,
		c_tinyint   tinyint   null,
		c_smallint  smallint  null,
		c_mediumint mediumint null,
		c_int       int       null,
		c_bigint    bigint    null,
		constraint pk
	primary key (id)
	)`
	_ = helper.DDL2Event(createTableSQL)

	insertSQL := `insert into tp_int() values ()`
	insertDMLEvent := helper.DML2Event("test", "tp_int", insertSQL)

	insertRow, ok := insertDMLEvent.GetNextRow()
	require.True(t, ok)

	insertRowEvent := &commonEvent.RowEvent{
		TableInfo:      insertDMLEvent.TableInfo,
		CommitTs:       insertDMLEvent.GetCommitTs(),
		Event:          insertRow,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	codecConfig.ContentCompatible = true
	codecConfig.OnlyOutputUpdatedColumns = true

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", insertRowEvent)
	require.NoError(t, err)

	messages := encoder.Build()
	require.Len(t, messages, 1)

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	decoder.AddKeyValue(messages[0].Key, messages[0].Value)
	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	decodedInsert := decoder.NextDMLEvent()
	require.NotNil(t, decodedInsert)
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
	codecConfig.OnlyOutputUpdatedColumns = true
	for _, enableTiDBExtension := range []bool{true, false} {
		for _, event := range []*commonEvent.RowEvent{minValueEvent, maxValueEvent} {
			codecConfig.EnableTiDBExtension = enableTiDBExtension
			encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
			require.NoError(t, err)

			err = encoder.AppendRowChangedEvent(ctx, "", event)
			require.NoError(t, err)

			messages := encoder.Build()
			require.Len(t, messages, 1)

			decoder, err := NewDecoder(ctx, codecConfig, nil)
			require.NoError(t, err)

			decoder.AddKeyValue(messages[0].Key, messages[0].Value)

			messageType, hasNext := decoder.HasNext()
			require.True(t, hasNext)
			require.Equal(t, common.MessageTypeRow, messageType)

			decoded := decoder.NextDMLEvent()
			if enableTiDBExtension {
				require.Equal(t, event.CommitTs, decoded.GetCommitTs())
			}

			require.NotZero(t, decoded.TableInfo.GetPreInsertSQL())
			require.NotZero(t, decoded.TableInfo.GetPreUpdateSQL())
			require.NotZero(t, decoded.TableInfo.GetPreReplaceSQL())

			change, ok := decoded.GetNextRow()
			require.True(t, ok)

			common.CompareRow(t, event.Event, event.TableInfo, change, decoded.TableInfo)
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

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	decoder.AddKeyValue(m.Key, m.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event := decoder.NextDMLEvent()
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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	decoder.AddKeyValue(m.Key, m.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event := decoder.NextDMLEvent()
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

	// dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a,b,c,d) values ("char","varchar","binary","varbinary")`)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t() values (1, "", "", "", "")`)
	require.NotNil(t, dmlEvent)
	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)

	tableInfo := helper.GetTableInfo(job)
	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       dmlEvent.GetCommitTs(),
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

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	decoder.AddKeyValue(m.Key, m.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event := decoder.NextDMLEvent()
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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	decoder.AddKeyValue(m.Key, m.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event := decoder.NextDMLEvent()
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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	decoder.AddKeyValue(m.Key, m.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event := decoder.NextDMLEvent()
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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.ContentCompatible = true

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	decoder.AddKeyValue(m.Key, m.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event := decoder.NextDMLEvent()
	change, ok := event.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
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
	selectors, err := columnselector.New(replicaConfig.Sink)
	require.NoError(t, err)

	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: selectors.Get("test", "t"),
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", rowEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	decoder.AddKeyValue(m.Key, m.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event := decoder.NextDMLEvent()
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

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	decoder.AddKeyValue(m.Key, m.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event := decoder.NextDMLEvent()
	change, ok := event.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, event.TableInfo)
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

func TestLargeMessageClaimCheck(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	_ = helper.DDL2Event(`create table t (
		id          int primary key auto_increment,
	
		c_tinyint   tinyint   null,
		c_smallint  smallint  null,
		c_mediumint mediumint null,
		c_int       int       null,
		c_bigint    bigint    null,
	
		c_unsigned_tinyint   tinyint   unsigned null,
		c_unsigned_smallint  smallint  unsigned null,
		c_unsigned_mediumint mediumint unsigned null,
		c_unsigned_int       int       unsigned null,
		c_unsigned_bigint    bigint    unsigned null,
	
		c_float   float   null,
		c_double  double  null,
		c_decimal decimal null,
		c_decimal_2 decimal(10, 4) null,
	
		c_unsigned_float     float unsigned   null,
		c_unsigned_double    double unsigned  null,
		c_unsigned_decimal   decimal unsigned null,
		c_unsigned_decimal_2 decimal(10, 4) unsigned null,
	
		c_date      date      null,
		c_datetime  datetime  null,
		c_timestamp timestamp null,
		c_time      time      null,
		c_year      year      null,
	
		c_tinytext   tinytext      null,
		c_text       text          null,
		c_mediumtext mediumtext    null,
		c_longtext   longtext      null,
	
		c_tinyblob   tinyblob      null,
		c_blob       blob          null,
		c_mediumblob mediumblob    null,
		c_longblob   longblob      null,
	
		c_char       char(16)      null,
		c_varchar    varchar(16)   null,
		c_binary     binary(16)    null,
		c_varbinary  varbinary(16) null,
	
		c_enum enum ('a','b','c') null,
		c_set  set ('a','b','c')  null,
		c_bit  bit(64)            null,
		c_json json               null
	);`)

	dmlEvent := helper.DML2Event("test", "t", `insert into t values (
		1,
		1, 2, 3, 4, 5,
		1, 2, 3, 4, 5,
		2020.0202, 2020.0303, 2020.0404, 2021.1208,
		3.1415, 2.7182, 8000, 179394.233,
		'2020-02-20', '2020-02-20 02:20:20', '2020-02-20 02:20:20', '02:20:20', '2020',
		'89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A', '89504E470D0A1A0A',
		x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
		'89504E470D0A1A0A', '89504E470D0A1A0A', x'89504E470D0A1A0A', x'89504E470D0A1A0A',
		'b', 'b,c', b'1000001', '{
			"key1": "value1",
			"key2": "value2",
			"key3": "123"
		}'
	);`)

	row, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	insertEvent := &commonEvent.RowEvent{
		TableInfo:      dmlEvent.TableInfo,
		CommitTs:       1,
		Event:          row,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON).WithMaxMessageBytes(1000)
	codecConfig.EnableTiDBExtension = true
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionClaimCheck
	codecConfig.LargeMessageHandle.LargeMessageHandleCompression = "snappy"
	codecConfig.LargeMessageHandle.ClaimCheckStorageURI = "file:///tmp/canal-json-claim-check"

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", insertEvent)
	require.NoError(t, err)

	m := encoder.Build()[0]
	require.NotNil(t, m.Callback)

	dec, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	dec.AddKeyValue(m.Key, m.Value)

	messageType, hasNext := dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	decodedInsert := dec.NextDMLEvent()
	require.NotNil(t, decodedInsert)

	change, ok := decodedInsert.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, insertEvent.Event, insertEvent.TableInfo, change, decodedInsert.TableInfo)
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

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	decoder.AddKeyValue(m.Key, m.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event := decoder.NextDMLEvent()
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

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	for _, event := range []*commonEvent.RowEvent{
		insertEvent,
		deleteEvent,
		updateEvent,
	} {

		err = encoder.AppendRowChangedEvent(ctx, "", event)
		require.NoError(t, err)

		message := encoder.Build()[0]

		decoder.AddKeyValue(message.Key, message.Value)

		messageType, hasNext := decoder.HasNext()
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeRow, messageType)

		decoded := decoder.NextDMLEvent()
		change, ok := decoded.GetNextRow()
		require.True(t, ok)

		common.CompareRow(t, event.Event, event.TableInfo, change, decoded.TableInfo)
	}

	// update with only updated columns
	codecConfig.OnlyOutputUpdatedColumns = true
	encoder, err = NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, "", updateEvent)
	require.NoError(t, err)

	message := encoder.Build()[0]

	decoder, err = NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	decoder.AddKeyValue(message.Key, message.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	decoded := decoder.NextDMLEvent()
	change, ok := decoded.GetNextRow()
	require.True(t, ok)

	common.CompareRow(t, updateEvent.Event, updateEvent.TableInfo, change, decoded.TableInfo)
}

func TestDDLSequence(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	dec, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	createDB := helper.DDL2Event(`create database abc`)

	m, err := encoder.EncodeDDLEvent(createDB)
	require.NoError(t, err)

	dec.AddKeyValue(m.Key, m.Value)

	messageType, hasNext := dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, messageType)

	obtained := dec.NextDDLEvent()
	require.Equal(t, createDB.Query, obtained.Query)
	require.Equal(t, createDB.Type, obtained.Type)
	require.Equal(t, obtained.GetBlockedTables().InfluenceType, commonEvent.InfluenceTypeNormal)

	dropDB := helper.DDL2Event(`drop database abc`)

	m, err = encoder.EncodeDDLEvent(dropDB)
	require.NoError(t, err)

	dec.AddKeyValue(m.Key, m.Value)

	messageType, hasNext = dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, messageType)

	obtained = dec.NextDDLEvent()
	require.Equal(t, dropDB.Query, obtained.Query)
	require.Equal(t, dropDB.Type, obtained.Type)
	require.Equal(t, obtained.GetBlockedTables().InfluenceType, commonEvent.InfluenceTypeDB)

	helper.Tk().MustExec("use test")

	createTable := helper.DDL2Event(`create table t(a int primary key, b int)`)

	m, err = encoder.EncodeDDLEvent(createTable)
	require.NoError(t, err)

	dec.AddKeyValue(m.Key, m.Value)

	messageType, hasNext = dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, messageType)

	obtained = dec.NextDDLEvent()
	require.Equal(t, createTable.Query, obtained.Query)
	require.Equal(t, createTable.Type, obtained.Type)
	require.Equal(t, obtained.GetBlockedTables().InfluenceType, commonEvent.InfluenceTypeNormal)

	insert := helper.DML2Event("test", "t", `insert into test.t(a,b) values (1,1)`)
	require.NotNil(t, insert)
	insertRow, ok := insert.GetNextRow()
	require.True(t, ok)

	columnSelector := columnselector.NewDefaultColumnSelector()
	insertEvent := &commonEvent.RowEvent{
		TableInfo:      insert.TableInfo,
		CommitTs:       insert.GetCommitTs(),
		Event:          insertRow,
		ColumnSelector: columnSelector,
		Callback:       func() {},
	}

	err = encoder.AppendRowChangedEvent(ctx, "", insertEvent)
	require.NoError(t, err)

	m = encoder.Build()[0]

	dec.AddKeyValue(m.Key, m.Value)
	messageType, hasNext = dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	decodedInsert := dec.NextDMLEvent()
	require.NotZero(t, decodedInsert.GetTableID())

	addColumn := helper.DDL2Event(`alter table t add column c int`)

	m, err = encoder.EncodeDDLEvent(addColumn)
	require.NoError(t, err)

	dec.AddKeyValue(m.Key, m.Value)

	messageType, hasNext = dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, messageType)

	obtained = dec.NextDDLEvent()
	require.Equal(t, addColumn.Query, obtained.Query)
	require.Equal(t, addColumn.Type, obtained.Type)
	require.Equal(t, obtained.GetBlockedTables().InfluenceType, commonEvent.InfluenceTypeNormal)
	require.Equal(t, decodedInsert.GetTableID(), obtained.GetBlockedTables().TableIDs[0])

	dropTable := helper.DDL2Event(`drop table t`)

	m, err = encoder.EncodeDDLEvent(dropTable)
	require.NoError(t, err)

	dec.AddKeyValue(m.Key, m.Value)

	messageType, hasNext = dec.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, messageType)

	obtained = dec.NextDDLEvent()
	require.Equal(t, dropTable.Query, obtained.Query)
	require.Equal(t, dropTable.Type, obtained.Type)
	require.Equal(t, obtained.GetBlockedTables().InfluenceType, commonEvent.InfluenceTypeNormal)
}

func TestCreateTableDDL(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	ddlEvent := helper.DDL2Event(`create table test.t(a tinyint primary key, b int)`)

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	ctx := context.Background()

	for _, enableTiDBExtension := range []bool{false, true} {
		codecConfig.EnableTiDBExtension = enableTiDBExtension
		encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
		require.NoError(t, err)

		message, err := encoder.EncodeDDLEvent(ddlEvent)
		require.NoError(t, err)

		decoder, err := NewDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)

		decoder.AddKeyValue(message.Key, message.Value)

		messageType, hasNext := decoder.HasNext()
		require.True(t, hasNext)
		require.Equal(t, common.MessageTypeDDL, messageType)

		obtained := decoder.NextDDLEvent()
		require.Equal(t, ddlEvent.Query, obtained.Query)
		require.Equal(t, ddlEvent.Type, obtained.Type)
		require.Equal(t, obtained.GetBlockedTables().InfluenceType, commonEvent.InfluenceTypeNormal)
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

	decoder, err := NewDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	decoder.AddKeyValue(message.Key, message.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeResolved, messageType)

	obtained := decoder.NextResolvedEvent()
	require.Equal(t, watermark, obtained)
}

func TestRowKey(t *testing.T) {
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
	values := helper.DML2Event("test", "t", sql)
	row, ok := values.GetNextRow()
	require.True(t, ok)
	columnSelector := columnselector.NewDefaultColumnSelector()
	event := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       values.GetCommitTs(),
		Event:          row,
		ColumnSelector: columnSelector,
		Callback:       func() {},
	}

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.ContentCompatible = true
	codecConfig.OnlyOutputUpdatedColumns = true
	codecConfig.EnableTiDBExtension = true
	codecConfig.OutputRowKey = true
	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)
	err = encoder.AppendRowChangedEvent(ctx, "", event)
	require.NoError(t, err)
	messages := encoder.Build()
	require.Len(t, messages, 1)
	message := messages[0]

	var data map[string]json.RawMessage
	err = json.Unmarshal(message.Value, &data)
	require.NoError(t, err)

	var tidb_ext struct {
		CommitTs int64  `json:"commitTs"`
		Rowkey   string `json:"rowkey"`
	}
	err = json.Unmarshal(data["_tidb"], &tidb_ext)
	require.NoError(t, err)

	require.NotEqual(t, tidb_ext.CommitTs, 0)
	require.Equal(t, tidb_ext.Rowkey, "dIAAAAAAAABwX3KAAAAAAAAAAQ==")
}
