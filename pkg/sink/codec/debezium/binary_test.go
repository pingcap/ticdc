// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package debezium

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestDebeziumBinaryHandling(t *testing.T) {
	helper := NewSQLTestHelper(t, "binary_modes", `create table binary_modes (
		id varbinary(4) primary key default 'ab',
		fixed_value binary(4),
		blob_value blob,
		empty_value varbinary(4),
		null_value varbinary(4),
		default_value varbinary(4) default 'ab',
		text_value varchar(20) default 'plain',
		bit_value bit(64) default b'101'
	)`)
	defer helper.Close()
	event := helper.helper.DML2Event("test", "binary_modes",
		"insert into binary_modes (id, fixed_value, blob_value, empty_value) values (x'fbff', x'fbff', x'fbff', x'')")
	row, ok := event.GetNextRow()
	require.True(t, ok)

	for _, tc := range []struct {
		name         string
		mode         string
		schemaType   string
		encoded      string
		fixed        string
		defaultValue string
	}{
		{name: "default", schemaType: "string", encoded: "+/8=", fixed: "+/8AAA==", defaultValue: "YWI="},
		{name: "base64", mode: "base64", schemaType: "string", encoded: "+/8=", fixed: "+/8AAA==", defaultValue: "YWI="},
		{name: "bytes", mode: "bytes", schemaType: "bytes", encoded: "+/8=", fixed: "+/8AAA==", defaultValue: "YWI="},
		{name: "URL safe", mode: "base64-url-safe", schemaType: "string", encoded: "-_8=", fixed: "-_8AAA==", defaultValue: "YWI="},
		{name: "hex", mode: "hex", schemaType: "string", encoded: "fbff", fixed: "fbff0000", defaultValue: "6162"},
	} {
		for _, disableSchema := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/disableSchema=%t", tc.name, disableSchema), func(t *testing.T) {
				cfg := common.NewConfig(config.ProtocolDebezium)
				cfg.EnableTiDBExtension = true
				cfg.TimeZone = time.UTC
				cfg.DebeziumDisableSchema = disableSchema
				if tc.mode != "" {
					cfg.DebeziumBinaryHandlingMode = tc.mode
				}
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
				require.NoError(t, json.Unmarshal(messages[0].Key, &key))
				require.NoError(t, json.Unmarshal(messages[0].Value, &value))
				require.Equal(t, tc.encoded, key["payload"].(map[string]any)["id"])
				data := value["payload"].(map[string]any)["after"].(map[string]any)
				for _, name := range []string{"id", "blob_value"} {
					require.Equal(t, tc.encoded, data[name], name)
				}
				require.Equal(t, tc.defaultValue, data["default_value"])
				require.Equal(t, tc.fixed, data["fixed_value"])
				require.Equal(t, "", data["empty_value"])
				require.Nil(t, data["null_value"])
				require.Equal(t, "plain", data["text_value"])
				require.Equal(t, "BQAAAAAAAAA=", data["bit_value"])
				if disableSchema {
					require.NotContains(t, key, "schema")
					require.NotContains(t, value, "schema")
					return
				}

				keyField := schemaFieldsByName(t, key["schema"].(map[string]any), "id")
				require.Equal(t, tc.schemaType, keyField["type"])
				require.Equal(t, tc.defaultValue, keyField["default"])
				afterSchema := schemaFieldsByName(t, value["schema"].(map[string]any), "after")
				for _, name := range []string{"id", "fixed_value", "blob_value", "empty_value", "null_value", "default_value"} {
					require.Equal(t, tc.schemaType, schemaFieldsByName(t, afterSchema, name)["type"], name)
				}
				require.Equal(t, tc.defaultValue, schemaFieldsByName(t, afterSchema, "default_value")["default"])
				require.Equal(t, "string", schemaFieldsByName(t, afterSchema, "text_value")["type"])
				bitField := schemaFieldsByName(t, afterSchema, "bit_value")
				require.Equal(t, "bytes", bitField["type"])
				require.Equal(t, "BQAAAAAAAAA=", bitField["default"])

				decoder := NewDecoder(cfg, 0, nil)
				decoder.AddKeyValue(messages[0].Key, messages[0].Value)
				decoded := decoder.NextDMLMessage().ToDMLEvent()
				change, ok := decoded.GetNextRow()
				require.True(t, ok)
				common.CompareRow(t, row, helper.tableInfo, change, decoded.TableInfo)
				decoded.PostFlush()
			})
		}
	}
}
