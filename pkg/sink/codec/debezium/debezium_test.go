// Copyright 2024 PingCAP, Inc.
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

package debezium

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type SQLTestHelper struct {
	t *testing.T

	helper  *commonEvent.EventTestHelper
	mounter commonEvent.Mounter

	tableInfo *commonType.TableInfo
}

func NewSQLTestHelper(t *testing.T, tableName, initialCreateTableDDL string) *SQLTestHelper {
	helper := commonEvent.NewEventTestHelperWithTimeZone(t, time.UTC)
	helper.Tk().MustExec("set @@tidb_enable_clustered_index=1;")
	helper.Tk().MustExec("use test;")

	job := helper.DDL2Job(initialCreateTableDDL)
	require.NotNil(t, job)

	mounter := commonEvent.NewMounter(time.UTC, config.GetDefaultReplicaConfig().Integrity)

	tableInfo := helper.GetTableInfo(job)

	return &SQLTestHelper{
		t:         t,
		helper:    helper,
		mounter:   mounter,
		tableInfo: tableInfo,
	}
}

func (h *SQLTestHelper) Close() {
	h.helper.Close()
}

func (h *SQLTestHelper) MustExec(query string, args ...interface{}) {
	h.helper.Tk().MustExec(query, args...)
}

type debeziumSuite struct {
	suite.Suite
	disableSchema bool
}

func (s *debeziumSuite) requireDebeziumJSONEq(dbzOutput []byte, tiCDCOutput []byte) {
	var (
		ignoredRecordPaths = map[string]bool{
			`{map[string]any}["schema"]`:                             s.disableSchema,
			`{map[string]any}["payload"].(map[string]any)["source"]`: true,
			`{map[string]any}["payload"].(map[string]any)["ts_ms"]`:  true,
		}

		compareOpt = cmp.FilterPath(
			func(p cmp.Path) bool {
				path := p.GoString()
				_, shouldIgnore := ignoredRecordPaths[path]
				return shouldIgnore
			},
			cmp.Ignore(),
		)
	)

	var objDbzOutput map[string]any
	s.Require().Nil(json.Unmarshal(dbzOutput, &objDbzOutput), "Failed to unmarshal Debezium JSON")

	var objTiCDCOutput map[string]any
	s.Require().Nil(json.Unmarshal(tiCDCOutput, &objTiCDCOutput), "Failed to unmarshal TiCDC JSON")

	if diff := cmp.Diff(objDbzOutput, objTiCDCOutput, compareOpt); diff != "" {
		s.Failf("JSON is not equal", "Diff (-debezium, +ticdc):\n%s", diff)
	}
}

func TestEncodeRoutedDMLEventUsesTargetNames(t *testing.T) {
	cfg := common.NewConfig(config.ProtocolDebezium)
	cfg.EnableTiDBExtension = true
	cfg.TimeZone = time.UTC

	encoder := NewBatchEncoder(cfg, "dbserver1")
	rowEvent := common.NewRoutedRowEvent4Test()
	require.NoError(t, encoder.AppendRowChangedEvent(context.Background(), "", rowEvent))

	messages := encoder.Build()
	require.Len(t, messages, 1)

	decoder := NewDecoder(cfg, 0, nil)
	decoder.AddKeyValue(messages[0].Key, messages[0].Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeRow, messageType)

	decoded := decoder.NextDMLMessage().ToDMLEvent()
	require.Equal(t, "target_db", decoded.TableInfo.GetSchemaName())
	require.Equal(t, "target_table", decoded.TableInfo.GetTableName())

	change, ok := decoded.GetNextRow()
	require.True(t, ok)
	common.CompareRow(t, rowEvent.Event, rowEvent.TableInfo, change, decoded.TableInfo)
}

func TestDebeziumNumericStringHandling(t *testing.T) {
	const decimalValue = "12345678901234567890123456789012345.123456789012345678901234567890"
	helper := NewSQLTestHelper(t, "numeric_precision", `create table numeric_precision (
		id bigint unsigned primary key default 18446744073709551615,
		signed_value bigint default 9223372036854775807,
		amount decimal(65,30) default `+decimalValue+`,
		nullable_amount decimal(65,30)
	)`)
	defer helper.Close()

	insert := helper.helper.DML2Event("test", "numeric_precision", "insert into numeric_precision () values ()")
	update, _ := helper.helper.DML2UpdateEvent("test", "numeric_precision",
		"insert into numeric_precision (id) values (18446744073709551614)",
		"update numeric_precision set signed_value = -9223372036854775808, amount = -"+decimalValue+" where id = 18446744073709551614")
	deleted := helper.helper.DML2DeleteEvent("test", "numeric_precision",
		"insert into numeric_precision (id) values (18446744073709551613)",
		"delete from numeric_precision where id = 18446744073709551613")

	for _, event := range []*commonEvent.DMLEvent{insert, update, deleted} {
		row, ok := event.GetNextRow()
		require.True(t, ok)
		for _, disableSchema := range []bool{false, true} {
			cfg := common.NewConfig(config.ProtocolDebezium)
			cfg.EnableTiDBExtension = true
			cfg.TimeZone = time.UTC
			cfg.DebeziumDisableSchema = disableSchema
			cfg.DebeziumDecimalHandlingMode = common.DecimalHandlingModeString
			cfg.DebeziumBigintUnsignedHandlingMode = common.BigintUnsignedHandlingModeString
			encoder := NewBatchEncoder(cfg, "dbserver1")
			require.NoError(t, encoder.AppendRowChangedEvent(t.Context(), "", &commonEvent.RowEvent{
				TableInfo:      helper.tableInfo,
				CommitTs:       1,
				Event:          row,
				ColumnSelector: columnselector.NewDefaultColumnSelector(),
			}))
			messages := encoder.Build()
			require.Len(t, messages, 1)

			var key, value map[string]any
			dec := json.NewDecoder(bytes.NewReader(messages[0].Key))
			dec.UseNumber()
			require.NoError(t, dec.Decode(&key))
			dec = json.NewDecoder(bytes.NewReader(messages[0].Value))
			dec.UseNumber()
			require.NoError(t, dec.Decode(&value))
			payload := value["payload"].(map[string]any)
			for _, field := range []string{"before", "after"} {
				data, ok := payload[field].(map[string]any)
				if !ok {
					continue
				}
				expectedRow := row.Row
				if field == "before" {
					expectedRow = row.PreRow
				}
				require.Equal(t, strconv.FormatUint(expectedRow.GetUint64(0), 10), data["id"])
				require.Equal(t, key["payload"].(map[string]any)["id"], data["id"])
				require.Equal(t, json.Number(strconv.FormatInt(expectedRow.GetInt64(1), 10)), data["signed_value"])
				require.Equal(t, expectedRow.GetMyDecimal(2).String(), data["amount"])
				require.Nil(t, data["nullable_amount"])
			}
			if disableSchema {
				require.NotContains(t, key, "schema")
				require.NotContains(t, value, "schema")
				continue
			}

			keyField := schemaFieldsByName(t, key["schema"].(map[string]any), "id")
			require.Equal(t, "string", keyField["type"])
			require.Equal(t, "18446744073709551615", keyField["default"])
			afterSchema := schemaFieldsByName(t, value["schema"].(map[string]any), "after")
			amountField := schemaFieldsByName(t, afterSchema, "amount")
			require.Equal(t, "string", amountField["type"])
			require.Equal(t, decimalValue, amountField["default"])
			signedField := schemaFieldsByName(t, afterSchema, "signed_value")
			require.Equal(t, "int64", signedField["type"])
			require.Equal(t, json.Number("9223372036854775807"), signedField["default"])

			decoder := NewDecoder(cfg, 0, nil)
			decoder.AddKeyValue(messages[0].Key, messages[0].Value)
			decoded := decoder.NextDMLMessage().ToDMLEvent()
			change, ok := decoded.GetNextRow()
			require.True(t, ok)
			common.CompareRow(t, row, helper.tableInfo, change, decoded.TableInfo)
			decoded.PostFlush()
		}
	}
}

func TestEncodeRoutedDDLEventUsesTargetNames(t *testing.T) {
	cfg := common.NewConfig(config.ProtocolDebezium)
	cfg.EnableTiDBExtension = true
	cfg.TimeZone = time.UTC

	encoder := NewBatchEncoder(cfg, "dbserver1")
	routedDDL := common.NewRoutedDDLEvent4Test()
	message, err := encoder.EncodeDDLEvent(routedDDL)
	require.NoError(t, err)
	require.NotNil(t, message)

	decoder := NewDecoder(cfg, 0, nil)
	decoder.AddKeyValue(message.Key, message.Value)

	messageType, hasNext := decoder.HasNext()
	require.True(t, hasNext)
	require.Equal(t, common.MessageTypeDDL, messageType)

	decoded := decoder.NextDDLEvent()
	require.Equal(t, "target_db", decoded.SchemaName)
	require.Equal(t, "target_table", decoded.TableName)
	require.Equal(t, routedDDL.Query, decoded.Query)
}

func TestDebeziumSuiteEnableSchema(t *testing.T) {
	suite.Run(t, &debeziumSuite{
		disableSchema: false,
	})
}

func TestDebeziumSuiteDisableSchema(t *testing.T) {
	suite.Run(t, &debeziumSuite{
		disableSchema: true,
	})
}

func (s *debeziumSuite) TestDataTypes() {
	dataDDL, err := os.ReadFile("testdata/datatype.ddl.sql")
	s.Require().Nil(err)

	dataDML, err := os.ReadFile("testdata/datatype.dml.sql")
	s.Require().Nil(err)

	dataDbzOutput, err := os.ReadFile("testdata/datatype.dbz.json")
	s.Require().Nil(err)
	keyDbzOutput, err := os.ReadFile("testdata/datatype.dbz.key.json")
	s.Require().Nil(err)

	helper := NewSQLTestHelper(s.T(), "foo", string(dataDDL))

	helper.MustExec(`SET sql_mode='';`)
	helper.MustExec(`SET time_zone='UTC';`)
	dmls := helper.helper.DML2Event("test", "foo", string(dataDML))

	cfg := common.NewConfig(config.ProtocolDebezium)
	cfg.TimeZone = time.UTC
	cfg.DebeziumDisableSchema = s.disableSchema
	encoder := NewBatchEncoder(cfg, "dbserver1")
	for {
		row, ok := dmls.GetNextRow()
		if !ok {
			break
		}
		err := encoder.AppendRowChangedEvent(context.Background(), "", &commonEvent.RowEvent{
			TableInfo:      helper.tableInfo,
			CommitTs:       1,
			Event:          row,
			ColumnSelector: columnselector.NewDefaultColumnSelector(),
			Callback:       func() {},
		})
		s.Require().Nil(err)
	}

	messages := encoder.Build()
	s.Require().Len(messages, 1)
	s.requireDebeziumJSONEq(dataDbzOutput, messages[0].Value)
	s.requireDebeziumJSONEq(keyDbzOutput, messages[0].Key)
}
