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

package event

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestDMLCompatibility(t *testing.T) {
	rowEvent := &RowChangedEvent {
		StartTs: 100,
		CommitTs: 200,
		TableInfo: &common.TableInfo{
			SchemaID: 1,
			TableName: common.TableName {
				Schema: "test",
				Table: "sbtest",
				TableID: 2,
				IsPartition: true,
			},
		},
		Columns: []*common.Column{
			&common.Column{
				Name: "column",
				Type: mysql.TypeEnum,
				Charset: mysql.UTF8Charset,
				Collation: mysql.DefaultCollationName,
				Flag: common.ColumnFlagType(mysql.GeneratedColumnFlag),
				Value: "hahaha",
				Default: "waaaaagh",
			},
		},
		PreColumns: []*common.Column{
			&common.Column{
				Name: "column",
				Type: mysql.TypeEnum,
				Charset: mysql.UTF8Charset,
				Collation: mysql.DefaultCollationName,
				Flag: common.ColumnFlagType(mysql.GeneratedColumnFlag),
				Value: "waaaaagh",
				Default: "waaaaagh",
			},
		},
	}
	redoLog := rowEvent.ToRedoLog()
	marshaled, err := redoLog.MarshalMsg(nil)
	require.Nil(t, err)

	redoLogOld := new(model.RedoLog)
	_, err = redoLogOld.UnmarshalMsg(marshaled)
	require.Nil(t, err)

	marshaled, err = redoLogOld.MarshalMsg(nil)
	require.Nil(t, err)

	redoLog = new(RedoLog)
	_, err = redoLog.UnmarshalMsg(marshaled)
	require.Nil(t, err)
}
