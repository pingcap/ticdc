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

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

// TODO: claim check

func CompareRow(t *testing.T, tableInfo *commonType.TableInfo, expected commonEvent.RowChange, actual commonEvent.RowChange) {
	fields := tableInfo.GetFieldSlice()

	if !expected.Row.IsEmpty() {
		a := expected.Row.GetDatumRow(fields)
		b := actual.Row.GetDatumRow(fields)
		require.Equal(t, len(a), len(b))
		for i := range fields {
			require.Equal(t, a[i], b[i])
		}
	}

	if !expected.PreRow.IsEmpty() {
		a := expected.PreRow.GetDatumRow(fields)
		b := expected.PreRow.GetDatumRow(fields)
		require.Equal(t, len(a), len(b))
		for i := range fields {
			require.Equal(t, a[i], b[i])
		}
	}
}

func TestBasicTypes(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(a tinyint primary key, bi varbinary(16))`)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a,bi) values (1, x'89504E470D0A1A0A')`)
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
	require.NotNil(t, change.Row)

	CompareRow(t, tableInfo, row, change)
}

func TestAllTypes(t *testing.T) {
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
		ac varbinary(16), ad varbinary(10),
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
		bg binary(10), bh binary(10),
		bi varbinary(16))`)

	dmlEvent := helper.DML2Event("test", "t", `insert into test.t(
		a,c,e,g,i,k,m,o,q,s,u,w,y,aa,ac,ae,ag,ai,ak,am,ao,aq,as1,au,aw,ay,ba,bc,be,bg, bi) values (
			1, true, -1, 123, 153.123,153.123,
			"1973-12-30 15:30:00",123,123,"2000-01-01","23:59:59",
			"2015-12-20 23:58:58",1970,"测试",0x0102030405060708090A,81,
			'{"key1": "value1"}', 129012.12, 'a', 'b', "5rWL6K+VdGV4dA==",
			0x89504E470D0A1A0A,"5rWL6K+VdGV4dA==",0x4944330300000000,
			"5rWL6K+VdGV4dA==",0x504B0304140000000800,"5rWL6K+VdGV4dA==",
			0x255044462D312E34,"Alice",0x0102030405060708090A, x'89504E470D0A1A0A')`)
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

	event, err := decoder.NextDMLEvent()
	require.NoError(t, err)

	change, ok := event.GetNextRow()
	require.True(t, ok)
	require.NotNil(t, change.Row)

	CompareRow(t, tableInfo, row, change)
}

func TestGeneralDMLEvent(t *testing.T) {
	// columnSelector
	{
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

		protocolConfig := common.NewConfig(config.ProtocolCanalJSON)
		value, err := newJSONMessageForDML(rowEvent, protocolConfig, false, "")
		require.NoError(t, err)

		var message JSONMessage

		err = json.Unmarshal(value, &message)
		require.NoError(t, err)

		require.Equal(t, int64(0), message.ID)
		require.Equal(t, "test", message.Schema)
		require.Equal(t, "t", message.Table)
		require.Equal(t, []string{"a"}, message.PKNames)
		require.Equal(t, false, message.IsDDL)
		require.Equal(t, "INSERT", message.EventType)
		require.Equal(t, "", message.Query)

		sqlValue, err := json.Marshal(message.SQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":-6}`, string(sqlValue))

		mysqlValue, err := json.Marshal(message.MySQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":"tinyint"}`, string(mysqlValue))

		oldValue, err := json.Marshal(message.Old)
		require.NoError(t, err)
		require.Equal(t, "null", string(oldValue))

		newValue, err := json.Marshal(message.Data)
		require.NoError(t, err)
		require.Equal(t, `[{"a":"1"}]`, string(newValue))
	}
	// EnableTiDBExtension
	{
		helper := commonEvent.NewEventTestHelper(t)
		defer helper.Close()

		helper.Tk().MustExec("use test")
		job := helper.DDL2Job(`create table test.t(a tinyint primary key, b tinyint)`)

		dmlEvent := helper.DML2Event("test", "t", `insert into test.t(a) values (1)`)
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

		protocolConfig := common.NewConfig(config.ProtocolCanalJSON)
		protocolConfig.EnableTiDBExtension = true
		value, err := newJSONMessageForDML(rowEvent, protocolConfig, false, "")
		require.NoError(t, err)

		var message canalJSONMessageWithTiDBExtension

		err = json.Unmarshal(value, &message)
		require.NoError(t, err)

		require.Equal(t, int64(0), message.ID)
		require.Equal(t, "test", message.Schema)
		require.Equal(t, "t", message.Table)
		require.Equal(t, []string{"a"}, message.PKNames)
		require.Equal(t, false, message.IsDDL)
		require.Equal(t, "INSERT", message.EventType)
		require.Equal(t, "", message.Query)

		sqlValue, err := json.Marshal(message.SQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":-6,"b":-6}`, string(sqlValue))

		mysqlValue, err := json.Marshal(message.MySQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":"tinyint","b":"tinyint"}`, string(mysqlValue))

		oldValue, err := json.Marshal(message.Old)
		require.NoError(t, err)
		require.Equal(t, "null", string(oldValue))

		newValue, err := json.Marshal(message.Data)
		require.NoError(t, err)
		require.Equal(t, `[{"a":"1","b":null}]`, string(newValue))

		require.Equal(t, uint64(1), message.Extensions.CommitTs)
		require.Equal(t, false, message.Extensions.OnlyHandleKey)
		require.Equal(t, "", message.Extensions.ClaimCheckLocation)
	}
	// multi pk
	{
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

		protocolConfig := common.NewConfig(config.ProtocolCanalJSON)
		value, err := newJSONMessageForDML(rowEvent, protocolConfig, false, "")
		require.NoError(t, err)

		var message JSONMessage

		err = json.Unmarshal(value, &message)
		require.NoError(t, err)

		require.Equal(t, []string{"a", "b"}, message.PKNames)

		sqlValue, err := json.Marshal(message.SQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":-6,"b":-6,"c":4}`, string(sqlValue))

		mysqlValue, err := json.Marshal(message.MySQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":"tinyint","b":"tinyint","c":"int"}`, string(mysqlValue))

		oldValue, err := json.Marshal(message.Old)
		require.NoError(t, err)
		require.Equal(t, "null", string(oldValue))

		newValue, err := json.Marshal(message.Data)
		require.NoError(t, err)
		require.Equal(t, `[{"a":"1","b":"3","c":"2"}]`, string(newValue))
	}
	// message large
	{
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

		protocolConfig := common.NewConfig(config.ProtocolCanalJSON)
		protocolConfig = protocolConfig.WithMaxMessageBytes(300)
		protocolConfig.EnableTiDBExtension = true
		encoder, err := NewJSONRowEventEncoder(context.Background(), protocolConfig)
		require.NoError(t, err)
		err = encoder.AppendRowChangedEvent(context.Background(), "", rowEvent)
		require.ErrorIs(t, err, errors.ErrMessageTooLarge)
	}
	// message large + handle only
	{
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

		protocolConfig := common.NewConfig(config.ProtocolCanalJSON)
		protocolConfig = protocolConfig.WithMaxMessageBytes(300)
		protocolConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionHandleKeyOnly
		protocolConfig.EnableTiDBExtension = true
		encoder, err := NewJSONRowEventEncoder(context.Background(), protocolConfig)
		require.NoError(t, err)
		err = encoder.AppendRowChangedEvent(context.Background(), "", rowEvent)
		require.NoError(t, err)

		messages := encoder.Build()
		require.Equal(t, 1, len(messages))
		require.NotNil(t, messages[0].Callback)

		value := messages[0].Value
		var message canalJSONMessageWithTiDBExtension

		err = json.Unmarshal(value, &message)
		require.NoError(t, err)

		sqlValue, err := json.Marshal(message.SQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":-6}`, string(sqlValue))

		mysqlValue, err := json.Marshal(message.MySQLType)
		require.NoError(t, err)
		require.Equal(t, `{"a":"tinyint"}`, string(mysqlValue))

		oldValue, err := json.Marshal(message.Old)
		require.NoError(t, err)
		require.Equal(t, "null", string(oldValue))

		newValue, err := json.Marshal(message.Data)
		require.NoError(t, err)
		require.Equal(t, `[{"a":"1"}]`, string(newValue))

		require.Equal(t, true, message.Extensions.OnlyHandleKey)
	}
}

func TestInsertEvent(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b tinyint)`)
	tableInfo := helper.GetTableInfo(job)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	encoder, err := NewJSONRowEventEncoder(ctx, codecConfig)
	require.NoError(t, err)

	insertEvent := helper.DML2Event("test", "t", `insert into test.t(a,b) values (1,3)`)
	require.NotNil(t, insertEvent)
	insertRow, ok := insertEvent.GetNextRow()
	require.True(t, ok)

	rowEvent := &commonEvent.RowEvent{
		TableInfo:      tableInfo,
		CommitTs:       1,
		Event:          insertRow,
		ColumnSelector: columnselector.NewDefaultColumnSelector(),
		Callback:       func() {},
	}

	err = encoder.AppendRowChangedEvent(context.Background(), "", rowEvent)
	require.NoError(t, err)

	messages := encoder.Build()
	require.Equal(t, 1, len(messages))

	decoder, err := NewCanalJSONDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(messages[0].Key, messages[0].Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	event, err := decoder.NextDMLEvent()
	require.NoError(t, err)
	require.NotNil(t, event)
	//var value JSONMessage
	//err = json.Unmarshal(messages[0].Value, &value)
	//require.NoError(t, err)
	//
	//require.Equal(t, int64(0), value.ID)
	//require.Equal(t, "test", value.Schema)
	//require.Equal(t, "t", value.Table)
	//require.Equal(t, []string{"a"}, value.PKNames)
	//require.Equal(t, false, value.IsDDL)
	//require.Equal(t, "INSERT", value.EventType)
	//require.Equal(t, "", value.Query)
	//
	//sqlValue, err := json.Marshal(value.SQLType)
	//require.NoError(t, err)
	//require.Equal(t, `{"a":-6,"b":-6}`, string(sqlValue))
	//
	//mysqlValue, err := json.Marshal(value.MySQLType)
	//require.NoError(t, err)
	//require.Equal(t, `{"a":"tinyint","b":"tinyint"}`, string(mysqlValue))
	//
	//oldValue, err := json.Marshal(value.Old)
	//require.NoError(t, err)
	//require.Equal(t, "null", string(oldValue))
	//
	//newValue, err := json.Marshal(value.Data)
	//require.NoError(t, err)
	//require.Equal(t, `[{"a":"1","b":"3"}]`, string(newValue))
	//

}

// insert / update(with only updated or not) / delete
//func TestDMLTypeEvent(t *testing.T) {
//
//	// update
//	dmlEvent = helper.DML2Event("test", "t", `update test.t set b = 2 where a = 1`)
//	require.NotNil(t, dmlEvent)
//	updateRow, ok := dmlEvent.GetNextRow()
//	require.True(t, ok)
//	updateRow.PreRow = insertRow.Row
//
//	updateRowEvent := &commonEvent.RowEvent{
//		TableInfo:      tableInfo,
//		CommitTs:       2,
//		Event:          updateRow,
//		ColumnSelector: columnselector.NewDefaultColumnSelector(),
//		Callback:       func() {},
//	}
//
//	err = encoder.AppendRowChangedEvent(context.Background(), "", updateRowEvent)
//	require.NoError(t, err)
//
//	messages = encoder.Build()
//	require.Equal(t, 1, len(messages))
//
//	err = json.Unmarshal(messages[0].Value, &value)
//	require.NoError(t, err)
//
//	require.Equal(t, int64(0), value.ID)
//	require.Equal(t, "test", value.Schema)
//	require.Equal(t, "t", value.Table)
//	require.Equal(t, []string{"a"}, value.PKNames)
//	require.Equal(t, false, value.IsDDL)
//	require.Equal(t, "UPDATE", value.EventType)
//	require.Equal(t, "", value.Query)
//
//	sqlValue, err = json.Marshal(value.SQLType)
//	require.NoError(t, err)
//	require.Equal(t, `{"a":-6,"b":-6}`, string(sqlValue))
//
//	mysqlValue, err = json.Marshal(value.MySQLType)
//	require.NoError(t, err)
//	require.Equal(t, `{"a":"tinyint","b":"tinyint"}`, string(mysqlValue))
//
//	oldValue, err = json.Marshal(value.Old)
//	require.NoError(t, err)
//	require.Equal(t, `[{"a":"1","b":"3"}]`, string(oldValue))
//
//	newValue, err = json.Marshal(value.Data)
//	require.NoError(t, err)
//	require.Equal(t, `[{"a":"1","b":"2"}]`, string(newValue))
//
//	// delete
//	deleteRow := updateRow
//	deleteRow.PreRow = updateRow.Row
//	deleteRow.Row = chunk.Row{}
//
//	deleteRowEvent := &commonEvent.RowEvent{
//		TableInfo:      tableInfo,
//		CommitTs:       3,
//		Event:          deleteRow,
//		ColumnSelector: columnselector.NewDefaultColumnSelector(),
//		Callback:       func() {},
//	}
//
//	err = encoder.AppendRowChangedEvent(context.Background(), "", deleteRowEvent)
//	require.NoError(t, err)
//
//	messages = encoder.Build()
//	require.Equal(t, 1, len(messages))
//
//	err = json.Unmarshal(messages[0].Value, &value)
//	require.NoError(t, err)
//
//	require.Equal(t, int64(0), value.ID)
//	require.Equal(t, "test", value.Schema)
//	require.Equal(t, "t", value.Table)
//	require.Equal(t, []string{"a"}, value.PKNames)
//	require.Equal(t, false, value.IsDDL)
//	require.Equal(t, "DELETE", value.EventType)
//	require.Equal(t, "", value.Query)
//
//	sqlValue, err = json.Marshal(value.SQLType)
//	require.NoError(t, err)
//	require.Equal(t, `{"a":-6,"b":-6}`, string(sqlValue))
//
//	mysqlValue, err = json.Marshal(value.MySQLType)
//	require.NoError(t, err)
//	require.Equal(t, `{"a":"tinyint","b":"tinyint"}`, string(mysqlValue))
//
//	oldValue, err = json.Marshal(value.Old)
//	require.NoError(t, err)
//	require.Equal(t, "null", string(oldValue))
//
//	newValue, err = json.Marshal(value.Data)
//	require.NoError(t, err)
//	require.Equal(t, `[{"a":"1","b":"2"}]`, string(newValue))
//
//	// update with only updated columns
//	protocolConfig.OnlyOutputUpdatedColumns = true
//	encoder, err = NewJSONRowEventEncoder(context.Background(), protocolConfig)
//	require.NoError(t, err)
//
//	err = encoder.AppendRowChangedEvent(context.Background(), "", updateRowEvent)
//	require.NoError(t, err)
//
//	messages = encoder.Build()
//	require.Equal(t, 1, len(messages))
//
//	err = json.Unmarshal(messages[0].Value, &value)
//	require.NoError(t, err)
//
//	require.Equal(t, int64(0), value.ID)
//	require.Equal(t, "test", value.Schema)
//	require.Equal(t, "t", value.Table)
//	require.Equal(t, []string{"a"}, value.PKNames)
//	require.Equal(t, false, value.IsDDL)
//	require.Equal(t, "UPDATE", value.EventType)
//	require.Equal(t, "", value.Query)
//
//	sqlValue, err = json.Marshal(value.SQLType)
//	require.NoError(t, err)
//	require.Equal(t, `{"a":-6,"b":-6}`, string(sqlValue))
//
//	mysqlValue, err = json.Marshal(value.MySQLType)
//	require.NoError(t, err)
//	require.Equal(t, `{"a":"tinyint","b":"tinyint"}`, string(mysqlValue))
//
//	oldValue, err = json.Marshal(value.Old)
//	require.NoError(t, err)
//	require.Equal(t, `[{"b":"3"}]`, string(oldValue))
//
//	newValue, err = json.Marshal(value.Data)
//	require.NoError(t, err)
//	require.Equal(t, `[{"a":"1","b":"2"}]`, string(newValue))
//}

func TestCreateTableDDL(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	job := helper.DDL2Job(`create table test.t(a tinyint primary key, b int)`)
	require.NotNil(t, job)

	ddlEvent := &commonEvent.DDLEvent{
		Query:      job.Query,
		Type:       job.Type,
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
