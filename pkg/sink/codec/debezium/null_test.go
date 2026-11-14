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
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/avro"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestDebeziumReplaceNullWithDefault(t *testing.T) {
	_, err := avro.SetupEncoderAndSchemaRegistry4Testing(t.Context(), common.NewConfig(config.ProtocolAvro))
	require.NoError(t, err)
	defer avro.TeardownEncoderAndSchemaRegistry4Testing()

	helper := NewSQLTestHelper(t, "null_defaults", `create table null_defaults (
		id int primary key,
		v int default 7,
		s varchar(16) default 'fallback',
		n int
	)`)
	defer helper.Close()
	insertNull := helper.helper.DML2Event("test", "null_defaults", "insert into null_defaults values (1, null, null, null)")
	insertDefault := helper.helper.DML2Event("test", "null_defaults", "insert into null_defaults (id) values (2)")
	fromNull, _ := helper.helper.DML2UpdateEvent("test", "null_defaults",
		"insert into null_defaults values (3, null, null, null)",
		"update null_defaults set v = 9 where id = 3")
	toNull, _ := helper.helper.DML2UpdateEvent("test", "null_defaults",
		"insert into null_defaults values (4, 9, null, null)",
		"update null_defaults set v = null where id = 4")
	deleteNull := helper.helper.DML2DeleteEvent("test", "null_defaults",
		"insert into null_defaults values (5, null, null, null)",
		"delete from null_defaults where id = 5")

	for _, protocol := range []config.Protocol{config.ProtocolDebezium, config.ProtocolDebeziumAvro} {
		for _, mode := range []struct {
			name    string
			query   string
			replace bool
		}{
			{name: "default", replace: true},
			{name: "true", query: "&debezium-replace-null-with-default=true", replace: true},
			{name: "false", query: "&debezium-replace-null-with-default=false"},
		} {
			for _, disableSchema := range []bool{false, true} {
				if protocol == config.ProtocolDebeziumAvro && disableSchema {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/disableSchema=%t", protocol, mode.name, disableSchema), func(t *testing.T) {
					cfg := common.NewConfig(protocol)
					sinkURI, err := url.Parse("kafka://127.0.0.1:9092/topic?protocol=" + protocol.String() + mode.query)
					require.NoError(t, err)
					require.NoError(t, cfg.Apply(sinkURI, config.GetDefaultReplicaConfig().Sink))
					cfg.TimeZone = time.UTC
					cfg.DebeziumDisableSchema = disableSchema
					if protocol == config.ProtocolDebeziumAvro {
						cfg.AvroConfluentSchemaRegistry = "http://127.0.0.1:8081"
					}

					for _, tc := range []struct {
						name   string
						op     string
						event  *commonEvent.DMLEvent
						before any
						after  any
					}{
						{name: "insert NULL", op: "c", event: insertNull},
						{name: "insert DEFAULT", op: "c", event: insertDefault, after: float64(7)},
						{name: "update from NULL", op: "u", event: fromNull, after: float64(9)},
						{name: "update to NULL", op: "u", event: toNull, before: float64(9)},
						{name: "delete NULL", op: "d", event: deleteNull},
					} {
						t.Run(tc.name, func(t *testing.T) {
							row, ok := tc.event.GetNextRow()
							require.True(t, ok)
							defer tc.event.Rewind()
							var encoder common.EventEncoder = NewBatchEncoder(cfg, "dbserver1")
							if protocol == config.ProtocolDebeziumAvro {
								encoder, err = newAvroBatchEncoderForTest(t.Context(), cfg, "dbserver1")
								require.NoError(t, err)
							}
							require.NoError(t, encoder.AppendRowChangedEvent(t.Context(), "dbserver1.test.null_defaults", &commonEvent.RowEvent{
								TableInfo:      helper.tableInfo,
								CommitTs:       1,
								Event:          row,
								ColumnSelector: columnselector.NewDefaultColumnSelector(),
							}))
							messages := encoder.Build()
							require.Len(t, messages, 1)
							value := messages[0].Value
							if protocol == config.ProtocolDebeziumAvro {
								decoder, err := NewAvroDecoder(t.Context(), cfg, 0, nil)
								require.NoError(t, err)
								value, err = decoder.(*avroDecoder).toDebeziumJSON(value)
								require.NoError(t, err)
							}
							var message map[string]any
							require.NoError(t, json.Unmarshal(value, &message))
							payload := message["payload"].(map[string]any)
							require.Equal(t, tc.op, payload["op"])
							for _, image := range []string{"before", "after"} {
								if tc.op == "c" && image == "before" || tc.op == "d" && image == "after" {
									require.Nil(t, payload[image], image)
									continue
								}
								data, ok := payload[image].(map[string]any)
								require.True(t, ok, image)
								for _, column := range []string{"v", "s", "n"} {
									require.Contains(t, data, column, image)
								}
								expected := tc.after
								if image == "before" {
									expected = tc.before
								}
								if mode.replace && expected == nil {
									expected = float64(7)
								}
								require.Equal(t, expected, data["v"], image)
								if mode.replace || tc.event == insertDefault {
									require.Equal(t, "fallback", data["s"], image)
								} else {
									require.Nil(t, data["s"], image)
								}
								require.Nil(t, data["n"], image)
							}
							if protocol == config.ProtocolDebezium && !disableSchema {
								afterSchema := schemaFieldsByName(t, message["schema"].(map[string]any), "after")
								require.Equal(t, float64(7), schemaFieldsByName(t, afterSchema, "v")["default"])
							}
						})
					}
				})
			}
		}
	}
}
