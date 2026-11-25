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

package common

import (
	"sync"
	"testing"

	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestTableInfoProviderRecordDDL(t *testing.T) {
	provider := NewTableInfoProvider()
	t1 := commonType.WrapTableInfo("test", &timodel.TableInfo{ID: 1, Name: ast.NewCIStr("t1"), UpdateTS: 100})
	t2 := commonType.WrapTableInfo("test", &timodel.TableInfo{ID: 2, Name: ast.NewCIStr("t2"), UpdateTS: 100})
	t3 := commonType.WrapTableInfo("other", &timodel.TableInfo{ID: 3, Name: ast.NewCIStr("t3"), UpdateTS: 100})
	provider.WriteTableInfo(t1)
	provider.WriteTableInfo(t2)
	provider.WriteTableInfo(t3)

	// Normal DDL invalidates only its target table.
	provider.RecordDDL(&commonEvent.DDLEvent{
		FinishedTs: 110,
		Type:       byte(timodel.ActionAddColumn),
		SchemaName: "test",
		TableName:  "t1",
	})
	_, info := provider.ReadAt("test", "t1", 111)
	require.Nil(t, info)
	_, info = provider.ReadAt("test", "t2", 111)
	require.Same(t, t2, info)

	// Layout-moving DDLs invalidate all cached tables after their commit ts,
	// while earlier commits still see the cached version.
	for _, action := range []timodel.ActionType{
		timodel.ActionRenameTable,
		timodel.ActionRenameTables,
		timodel.ActionExchangeTablePartition,
	} {
		t.Run(action.String(), func(t *testing.T) {
			p := NewTableInfoProvider()
			p.WriteTableInfo(t1)
			p.WriteTableInfo(t2)
			p.WriteTableInfo(t3)

			p.RecordDDL(&commonEvent.DDLEvent{
				FinishedTs: 120,
				Type:       byte(action),
				SchemaName: "test",
				TableName:  "t2",
			})
			_, before := p.ReadAt("other", "t3", 120)
			require.Same(t, t3, before)

			for _, key := range [][2]string{{"test", "t1"}, {"test", "t2"}, {"other", "t3"}} {
				_, after := p.ReadAt(key[0], key[1], 121)
				require.Nil(t, after)
			}
		})
	}
}

func TestTableInfoProviderAllocatorIsolationAndConcurrency(t *testing.T) {
	p1 := NewTableInfoProvider()
	p2 := NewTableInfoProvider()

	id1 := p1.AllocateTableID("test", "t1")
	p1.AddBlockTableID("test", "t1", id1)
	id2 := p2.AllocateTableID("test", "t1")
	p2.AddBlockTableID("test", "t1", id2)

	require.Equal(t, []int64{id1}, p1.GetBlockedTables("test", "t1"))
	require.Equal(t, []int64{id2}, p2.GetBlockedTables("test", "t1"))

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			id := p1.AllocateTableID("test", "concurrent")
			p1.AddBlockTableID("test", "concurrent", id)
			_ = p1.GetBlockedTables("test", "concurrent")
		})
	}
	wg.Wait()
	require.Len(t, p1.GetBlockedTables("test", "concurrent"), 1)
	require.Empty(t, p2.GetBlockedTables("test", "concurrent"))
}
