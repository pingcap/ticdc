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

package schemastore

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	parser_model "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestBuildPersistedDDLEventForRenameTablesTiDB81Compat(t *testing.T) {
	rawArgs, err := json.Marshal([]any{
		[]int64{100},
		[]int64{105},
		[]parser_model.CIStr{parser_model.NewCIStr("t102")},
		[]int64{201},
		[]parser_model.CIStr{parser_model.NewCIStr("test")},
	})
	require.NoError(t, err)

	job := &model.Job{
		Type:    model.ActionRenameTables,
		Version: model.JobVersion1,
		Query:   "RENAME TABLE `test`.`t2` TO `test2`.`t102`",
		RawArgs: rawArgs,
		BinlogInfo: &model.HistoryInfo{
			MultipleTableInfos: []*model.TableInfo{
				{
					ID:   201,
					Name: parser_model.NewCIStr("t102"),
				},
			},
		},
	}

	var event PersistedDDLEvent
	require.NotPanics(t, func() {
		event = buildPersistedDDLEventForRenameTables(buildPersistedDDLEventFuncArgs{
			job: job,
			databaseMap: map[int64]*BasicDatabaseInfo{
				100: {Name: "test"},
				105: {Name: "test2"},
			},
		})
	})

	require.Equal(t, []int64{100}, event.ExtraSchemaIDs)
	require.Equal(t, []string{"test"}, event.ExtraSchemaNames)
	require.Equal(t, []string{""}, event.ExtraTableNames)
	require.Equal(t, []int64{105}, event.SchemaIDs)
	require.Equal(t, []string{"test2"}, event.SchemaNames)
	require.Len(t, event.MultipleTableInfos, 1)
}
