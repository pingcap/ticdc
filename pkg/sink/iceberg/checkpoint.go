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

package iceberg

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

const (
	checkpointSchemaName      = "__ticdc"
	checkpointTableName       = "__tidb_checkpoints"
	globalCheckpointTableName = "__tidb_global_checkpoints"
)

// RecordCheckpoint writes checkpoint records into the per-table checkpoint table.
func (w *TableWriter) RecordCheckpoint(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	tableInfo *common.TableInfo,
	physicalTableID int64,
	resolvedTs uint64,
	commitResult *CommitResult,
) error {
	if w == nil || w.cfg == nil {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg config is nil")
	}
	if commitResult == nil {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("commit result is nil")
	}
	if tableInfo == nil {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}

	checkpointTableInfo, err := w.getCheckpointTableInfo()
	if err != nil {
		return err
	}

	commitTime := commitResult.CommittedAt
	if commitTime == "" {
		commitTime = time.Now().UTC().Format(time.RFC3339Nano)
	}
	commitTs := strconv.FormatUint(resolvedTs, 10)

	row := ChangeRow{
		Op:         "I",
		CommitTs:   commitTs,
		CommitTime: commitTime,
		Columns: map[string]*string{
			"changefeed_id":     stringPtr(changefeedID.String()),
			"keyspace":          stringPtr(changefeedID.Keyspace()),
			"schema":            stringPtr(tableInfo.GetSchemaName()),
			"table":             stringPtr(tableInfo.GetTableName()),
			"table_id":          stringPtr(strconv.FormatInt(physicalTableID, 10)),
			"resolved_ts":       stringPtr(commitTs),
			"snapshot_id":       stringPtr(strconv.FormatInt(commitResult.SnapshotID, 10)),
			"commit_uuid":       stringPtr(commitResult.CommitUUID),
			"metadata_location": stringPtr(commitResult.MetadataLocation),
			"committed_at":      stringPtr(commitTime),
			"changefeed_gid":    stringPtr(changefeedID.ID().String()),
		},
	}

	_, err = w.AppendChangelog(ctx, changefeedID, checkpointTableInfo, 0, []ChangeRow{row}, resolvedTs)
	return err
}

// RecordGlobalCheckpoint writes checkpoint records into the global checkpoint table.
func (w *TableWriter) RecordGlobalCheckpoint(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	resolvedTs uint64,
) error {
	if w == nil || w.cfg == nil {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg config is nil")
	}

	globalTableInfo, err := w.getGlobalCheckpointTableInfo()
	if err != nil {
		return err
	}

	commitTime := time.Now().UTC().Format(time.RFC3339Nano)
	commitTs := strconv.FormatUint(resolvedTs, 10)

	row := ChangeRow{
		Op:         "I",
		CommitTs:   commitTs,
		CommitTime: commitTime,
		Columns: map[string]*string{
			"changefeed_id":  stringPtr(changefeedID.String()),
			"keyspace":       stringPtr(changefeedID.Keyspace()),
			"resolved_ts":    stringPtr(commitTs),
			"committed_at":   stringPtr(commitTime),
			"changefeed_gid": stringPtr(changefeedID.ID().String()),
		},
	}

	_, err = w.AppendChangelog(ctx, changefeedID, globalTableInfo, 0, []ChangeRow{row}, resolvedTs)
	return err
}

func (w *TableWriter) getCheckpointTableInfo() (*common.TableInfo, error) {
	if w == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg writer is nil")
	}

	w.checkpointOnce.Do(func() {
		ft := func() types.FieldType {
			return *types.NewFieldType(mysql.TypeVarchar)
		}
		w.checkpointTableInfo = common.WrapTableInfo(checkpointSchemaName, &timodel.TableInfo{
			ID:   1,
			Name: ast.NewCIStr(checkpointTableName),
			Columns: []*timodel.ColumnInfo{
				{ID: 1, Name: ast.NewCIStr("changefeed_id"), FieldType: ft()},
				{ID: 2, Name: ast.NewCIStr("keyspace"), FieldType: ft()},
				{ID: 3, Name: ast.NewCIStr("schema"), FieldType: ft()},
				{ID: 4, Name: ast.NewCIStr("table"), FieldType: ft()},
				{ID: 5, Name: ast.NewCIStr("table_id"), FieldType: ft()},
				{ID: 6, Name: ast.NewCIStr("resolved_ts"), FieldType: ft()},
				{ID: 7, Name: ast.NewCIStr("snapshot_id"), FieldType: ft()},
				{ID: 8, Name: ast.NewCIStr("commit_uuid"), FieldType: ft()},
				{ID: 9, Name: ast.NewCIStr("metadata_location"), FieldType: ft()},
				{ID: 10, Name: ast.NewCIStr("committed_at"), FieldType: ft()},
				{ID: 11, Name: ast.NewCIStr("changefeed_gid"), FieldType: ft()},
			},
		})
	})

	return w.checkpointTableInfo, w.checkpointTableErr
}

func (w *TableWriter) getGlobalCheckpointTableInfo() (*common.TableInfo, error) {
	if w == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg writer is nil")
	}

	w.globalCheckpointOnce.Do(func() {
		ft := func() types.FieldType {
			return *types.NewFieldType(mysql.TypeVarchar)
		}
		w.globalCheckpointTableInfo = common.WrapTableInfo(checkpointSchemaName, &timodel.TableInfo{
			ID:   2,
			Name: ast.NewCIStr(globalCheckpointTableName),
			Columns: []*timodel.ColumnInfo{
				{ID: 1, Name: ast.NewCIStr("changefeed_id"), FieldType: ft()},
				{ID: 2, Name: ast.NewCIStr("keyspace"), FieldType: ft()},
				{ID: 3, Name: ast.NewCIStr("resolved_ts"), FieldType: ft()},
				{ID: 4, Name: ast.NewCIStr("committed_at"), FieldType: ft()},
				{ID: 5, Name: ast.NewCIStr("changefeed_gid"), FieldType: ft()},
			},
		})
	})

	return w.globalCheckpointTableInfo, w.globalCheckpointTableErr
}

func stringPtr(v string) *string {
	return &v
}
