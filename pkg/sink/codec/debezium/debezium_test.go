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
