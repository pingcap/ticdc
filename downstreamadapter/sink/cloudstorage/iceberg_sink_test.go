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
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestStorageSinkIcebergAppendBasic(t *testing.T) {
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?protocol=iceberg&namespace=ns&commit-interval=200ms", parentDir)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.ValidateAndAdjust(sinkURI)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cloudStorageSink, err := newSinkForTest(ctx, replicaConfig, sinkURI, nil)
	require.NoError(t, err)

	go func() {
		_ = cloudStorageSink.Run(ctx)
	}()

	tableInfo := common.WrapTableInfo("test", &timodel.TableInfo{
		ID:         100,
		Name:       ast.NewCIStr("t_iceberg"),
		PKIsHandle: true,
		UpdateTS:   1,
		Columns: []*timodel.ColumnInfo{
			{
				ID:        1,
				Name:      ast.NewCIStr("id"),
				FieldType: *types.NewFieldType(mysql.TypeLong),
			},
			{
				ID:        2,
				Name:      ast.NewCIStr("v"),
				FieldType: *types.NewFieldType(mysql.TypeVarchar),
			},
		},
	})
	rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 2)
	rows.AppendInt64(0, 1)
	rows.AppendString(1, "a")
	rows.AppendInt64(0, 2)
	rows.AppendString(1, "b")

	dmlEvent := commonEvent.NewDMLEvent(common.NewDispatcherID(), tableInfo.TableName.TableID, 1, 2, tableInfo)
	dmlEvent.SetRows(rows)
	dmlEvent.RowTypes = []common.RowType{common.RowTypeInsert, common.RowTypeInsert}
	dmlEvent.Length = 2
	dmlEvent.TableInfoVersion = 1
	cloudStorageSink.AddDMLEvent(dmlEvent)

	metadataGlob := filepath.Join(parentDir, "ns", "test", "t_iceberg", "metadata", "v*.metadata.json")
	dataGlob := filepath.Join(parentDir, "ns", "test", "t_iceberg", "data", "snap-*.parquet")

	require.Eventually(t, func() bool {
		metas, _ := filepath.Glob(metadataGlob)
		dataFiles, _ := filepath.Glob(dataGlob)
		return len(metas) > 0 && len(dataFiles) > 0
	}, 20*time.Second, 200*time.Millisecond)

	metas, err := filepath.Glob(metadataGlob)
	require.NoError(t, err)
	require.NotEmpty(t, metas)

	metadataBytes, err := os.ReadFile(metas[len(metas)-1])
	require.NoError(t, err)
	require.Contains(t, string(metadataBytes), "\"format-version\":2")
	require.Contains(t, string(metadataBytes), "\"tidb.committed_resolved_ts\"")
}
