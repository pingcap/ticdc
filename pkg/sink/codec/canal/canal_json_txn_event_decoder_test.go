// Copyright 2022 PingCAP, Inc.
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
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestTxnDecoderDecodeFromTxnEncoder(t *testing.T) {
	t.Parallel()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	_ = helper.DDL2Event(`create table test.t(id int primary key, name varchar(32))`)
	dmlEvent := helper.DML2Event("test", "t", `insert into test.t values(1, "aa"),(2, "bb")`)
	require.NotNil(t, dmlEvent)

	for _, encodeEnable := range []bool{false, true} {
		encodeConfig := codecCommon.NewConfig(config.ProtocolCanalJSON)
		encodeConfig.EnableTiDBExtension = encodeEnable
		encodeConfig.Terminator = "\n"

		encoder := NewJSONTxnEventEncoder(encodeConfig)
		require.NoError(t, encoder.AppendTxnEvent(dmlEvent))
		messages := encoder.Build()
		require.Len(t, messages, 1)

		msg := messages[0]
		require.Equal(t, 2, msg.GetRowsCount())

		for _, decodeEnable := range []bool{false, true} {
			decodeConfig := codecCommon.NewConfig(config.ProtocolCanalJSON)
			decodeConfig.EnableTiDBExtension = decodeEnable
			decodeConfig.Terminator = "\n"

			decoder := NewTxnDecoder(decodeConfig)
			decoder.AddKeyValue(msg.Key, msg.Value)

			dmlEvent.Rewind()
			decodedCount := 0
			for {
				ty, hasNext := decoder.HasNext()
				if !hasNext {
					break
				}
				require.Equal(t, codecCommon.MessageTypeRow, ty)

				decoded := decoder.NextDMLEvent()
				require.NotNil(t, decoded)

				if encodeEnable && decodeEnable {
					require.Equal(t, dmlEvent.GetCommitTs(), decoded.GetCommitTs())
				} else {
					require.Equal(t, uint64(0), decoded.GetCommitTs())
				}
				require.Equal(t, dmlEvent.TableInfo.GetSchemaName(), decoded.TableInfo.GetSchemaName())
				require.Equal(t, dmlEvent.TableInfo.GetTableName(), decoded.TableInfo.GetTableName())

				originChange, ok := dmlEvent.GetNextRow()
				require.True(t, ok)
				decodedChange, ok := decoded.GetNextRow()
				require.True(t, ok)
				codecCommon.CompareRow(t, originChange, dmlEvent.TableInfo, decodedChange, decoded.TableInfo)

				_, ok = decoded.GetNextRow()
				require.False(t, ok)
				decodedCount++
			}
			require.Equal(t, 2, decodedCount)
		}
	}
}

func TestTxnDecoderWithTerminator(t *testing.T) {
	t.Parallel()

	encodedValue := `{"id":0,"database":"test","table":"employee","pkNames":["id"],"isDdl":false,"type":"INSERT","es":1668067205238,"ts":1668067206650,"sql":"","sqlType":{"FirstName":12,"HireDate":91,"LastName":12,"OfficeLocation":12,"id":4},"mysqlType":{"FirstName":"varchar","HireDate":"date","LastName":"varchar","OfficeLocation":"varchar","id":"int"},"data":[{"FirstName":"Bob","HireDate":"2014-06-04","LastName":"Smith","OfficeLocation":"New York","id":"101"}],"old":null}
{"id":0,"database":"test","table":"employee","pkNames":["id"],"isDdl":false,"type":"UPDATE","es":1668067229137,"ts":1668067230720,"sql":"","sqlType":{"FirstName":12,"HireDate":91,"LastName":12,"OfficeLocation":12,"id":4},"mysqlType":{"FirstName":"varchar","HireDate":"date","LastName":"varchar","OfficeLocation":"varchar","id":"int"},"data":[{"FirstName":"Bob","HireDate":"2015-10-08","LastName":"Smith","OfficeLocation":"Los Angeles","id":"101"}],"old":[{"FirstName":"Bob","HireDate":"2014-06-04","LastName":"Smith","OfficeLocation":"New York","id":"101"}]}
{"id":0,"database":"test","table":"employee","pkNames":["id"],"isDdl":false,"type":"DELETE","es":1668067230388,"ts":1668067231725,"sql":"","sqlType":{"FirstName":12,"HireDate":91,"LastName":12,"OfficeLocation":12,"id":4},"mysqlType":{"FirstName":"varchar","HireDate":"date","LastName":"varchar","OfficeLocation":"varchar","id":"int"},"data":[{"FirstName":"Bob","HireDate":"2015-10-08","LastName":"Smith","OfficeLocation":"Los Angeles","id":"101"}],"old":null}`
	codecConfig := codecCommon.NewConfig(config.ProtocolCanalJSON)
	codecConfig.Terminator = "\n"
	decoder := NewTxnDecoder(codecConfig)

	decoder.AddKeyValue(nil, []byte(encodedValue))

	cnt := 0
	for {
		tp, hasNext := decoder.HasNext()
		if !hasNext {
			break
		}
		require.Equal(t, codecCommon.MessageTypeRow, tp)
		cnt++
		event := decoder.NextDMLEvent()
		require.NotNil(t, event)
	}
	require.Equal(t, 3, cnt)
}
