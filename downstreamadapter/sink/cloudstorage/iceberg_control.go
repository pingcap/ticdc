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
	"encoding/json"
	"fmt"
	"net/url"
	"path"

	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	pkgcloudstorage "github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/tikv/client-go/v2/oracle"
)

const icebergControlManifestVersion = 1

type icebergDDLManifest struct {
	ManifestVersion   int                              `json:"manifest_version"`
	ChangefeedID      string                           `json:"changefeed_id"`
	EventID           string                           `json:"event_id"`
	DDLGroupID        string                           `json:"ddl_group_id"`
	DDLType           string                           `json:"ddl_type"`
	Query             string                           `json:"query"`
	CommitTs          uint64                           `json:"commit_ts"`
	Seq               uint64                           `json:"seq"`
	SourceDB          string                           `json:"source_db"`
	SourceTable       string                           `json:"source_table"`
	OldDB             string                           `json:"old_db,omitempty"`
	OldTable          string                           `json:"old_table,omitempty"`
	TableIDAfter      int64                            `json:"table_id_after,omitempty"`
	TableVersionAfter uint64                           `json:"table_version_after,omitempty"`
	TableDefAfter     *pkgcloudstorage.TableDefinition `json:"table_def_after,omitempty"`
	DDLApplyClass     string                           `json:"ddl_apply_class"`
	NeedRebuild       bool                             `json:"need_rebuild"`
	RebuildFromTs     uint64                           `json:"rebuild_from_ts,omitempty"`
	NotSync           bool                             `json:"not_sync"`
	NeedDroppedTables *commonEvent.InfluencedTables    `json:"need_dropped_tables,omitempty"`
	NeedAddedTables   []commonEvent.Table              `json:"need_added_tables,omitempty"`
	UpdatedSchemas    []commonEvent.SchemaIDChange     `json:"updated_schemas,omitempty"`
}

type icebergSchemaManifest struct {
	ManifestVersion    int                        `json:"manifest_version"`
	ChangefeedID       string                     `json:"changefeed_id"`
	SourceDB           string                     `json:"source_db"`
	SourceTable        string                     `json:"source_table"`
	SourceTableID      int64                      `json:"source_table_id"`
	TableVersion       uint64                     `json:"table_version"`
	Charset            string                     `json:"charset,omitempty"`
	Collation          string                     `json:"collation,omitempty"`
	HandleKeyColumnIDs []int64                    `json:"handle_key_column_ids,omitempty"`
	HandleKeyColumns   []string                   `json:"handle_key_columns,omitempty"`
	Columns            []pkgcloudstorage.TableCol `json:"columns,omitempty"`
	TiDBTableInfo      *timodel.TableInfo         `json:"tidb_table_info,omitempty"`
}

type icebergGlobalCheckpointManifest struct {
	ManifestVersion int    `json:"manifest_version"`
	ChangefeedID    string `json:"changefeed_id"`
	Keyspace        string `json:"keyspace"`
	ChangefeedGID   string `json:"changefeed_gid"`
	ResolvedTs      uint64 `json:"resolved_ts"`
}

func (p *icebergProcessor) recordGlobalCheckpoint(ctx context.Context, resolvedTs uint64) error {
	if p == nil || p.externalStorage == nil {
		return errors.ErrSinkURIInvalid.GenWithStackByArgs("iceberg external storage is nil")
	}

	manifest := &icebergGlobalCheckpointManifest{
		ManifestVersion: icebergControlManifestVersion,
		ChangefeedID:    p.changefeedID.String(),
		Keyspace:        p.changefeedID.Keyspace(),
		ChangefeedGID:   p.changefeedID.ID().String(),
		ResolvedTs:      resolvedTs,
	}
	return p.writeControlManifest(ctx, p.globalCheckpointManifestPath(resolvedTs), manifest)
}

func (p *icebergProcessor) writeDDLAndSchemaManifests(ctx context.Context, event *commonEvent.DDLEvent) error {
	if p == nil || event == nil {
		return nil
	}

	groupID := fmt.Sprintf("%d-%d", event.GetCommitTs(), event.GetSeq())
	for idx, ddlEvent := range event.GetEvents() {
		if ddlEvent == nil {
			continue
		}
		eventID := fmt.Sprintf("%d-%d-%d", event.GetCommitTs(), event.GetSeq(), idx)
		manifest, err := buildIcebergDDLManifest(p.changefeedID, groupID, eventID, event, ddlEvent)
		if err != nil {
			return err
		}
		if err := p.writeControlManifest(ctx, p.ddlManifestPath(event.GetCommitTs(), event.GetSeq(), eventID), manifest); err != nil {
			return err
		}
		if ddlEvent.TableInfo == nil {
			continue
		}
		schemaManifest, err := buildIcebergSchemaManifest(p.changefeedID, ddlEvent)
		if err != nil {
			return err
		}
		if err := p.writeControlManifest(ctx, p.schemaManifestPath(ddlEvent.SchemaName, ddlEvent.TableName, schemaManifest.TableVersion), schemaManifest); err != nil {
			return err
		}
	}
	return nil
}

func buildIcebergDDLManifest(
	changefeedID commonType.ChangeFeedID,
	groupID string,
	eventID string,
	parent *commonEvent.DDLEvent,
	event *commonEvent.DDLEvent,
) (*icebergDDLManifest, error) {
	ddlApplyClass := classifyIcebergDDL(event.GetDDLType())
	needRebuild := ddlApplyClass == "rebuild_required"

	manifest := &icebergDDLManifest{
		ManifestVersion:   icebergControlManifestVersion,
		ChangefeedID:      changefeedID.String(),
		EventID:           eventID,
		DDLGroupID:        groupID,
		DDLType:           event.GetDDLType().String(),
		Query:             event.Query,
		CommitTs:          parent.GetCommitTs(),
		Seq:               parent.GetSeq(),
		SourceDB:          event.SchemaName,
		SourceTable:       event.TableName,
		OldDB:             event.ExtraSchemaName,
		OldTable:          event.ExtraTableName,
		DDLApplyClass:     ddlApplyClass,
		NeedRebuild:       needRebuild,
		NotSync:           event.NotSync,
		NeedDroppedTables: parent.GetNeedDroppedTables(),
		NeedAddedTables:   parent.GetNeedAddedTables(),
		UpdatedSchemas:    parent.GetUpdatedSchemas(),
	}
	if needRebuild {
		manifest.RebuildFromTs = event.GetCommitTs()
	}
	if event.TableInfo != nil {
		manifest.TableIDAfter = event.TableInfo.TableName.TableID
		manifest.TableVersionAfter = icebergSchemaVersion(event)
		tableDef := &pkgcloudstorage.TableDefinition{}
		tableDef.FromTableInfo(event.SchemaName, event.TableName, event.TableInfo, manifest.TableVersionAfter, false)
		manifest.TableDefAfter = tableDef
	}
	return manifest, nil
}

func buildIcebergSchemaManifest(changefeedID commonType.ChangeFeedID, event *commonEvent.DDLEvent) (*icebergSchemaManifest, error) {
	if event == nil || event.TableInfo == nil {
		return nil, nil
	}

	tableVersion := icebergSchemaVersion(event)
	tableDef := &pkgcloudstorage.TableDefinition{}
	tableDef.FromTableInfo(event.SchemaName, event.TableName, event.TableInfo, tableVersion, false)

	handleKeyIDs := append([]int64(nil), event.TableInfo.GetOrderedHandleKeyColumnIDs()...)
	handleKeyColumns := make([]string, 0, len(handleKeyIDs))
	for _, id := range handleKeyIDs {
		if colInfo, ok := event.TableInfo.GetColumnInfo(id); ok && colInfo != nil {
			handleKeyColumns = append(handleKeyColumns, colInfo.Name.O)
		}
	}

	return &icebergSchemaManifest{
		ManifestVersion:    icebergControlManifestVersion,
		ChangefeedID:       changefeedID.String(),
		SourceDB:           event.SchemaName,
		SourceTable:        event.TableName,
		SourceTableID:      event.TableInfo.TableName.TableID,
		TableVersion:       tableVersion,
		Charset:            event.TableInfo.Charset,
		Collation:          event.TableInfo.Collate,
		HandleKeyColumnIDs: handleKeyIDs,
		HandleKeyColumns:   handleKeyColumns,
		Columns:            tableDef.Columns,
		TiDBTableInfo:      event.TableInfo.ToTiDBTableInfo(),
	}, nil
}

func classifyIcebergDDL(ddlType timodel.ActionType) string {
	switch ddlType {
	case timodel.ActionCreateSchema,
		timodel.ActionDropSchema,
		timodel.ActionCreateTable,
		timodel.ActionCreateTables,
		timodel.ActionDropTable,
		timodel.ActionRenameTable,
		timodel.ActionRenameTables,
		timodel.ActionAddColumn,
		timodel.ActionDropColumn,
		timodel.ActionModifyColumn,
		timodel.ActionSetDefaultValue:
		return "direct_replay"
	case timodel.ActionAddIndex,
		timodel.ActionDropIndex,
		timodel.ActionAddForeignKey,
		timodel.ActionDropForeignKey,
		timodel.ActionModifyTableCharsetAndCollate,
		timodel.ActionModifySchemaCharsetAndCollate:
		return "compatibility_only"
	case timodel.ActionTruncateTable,
		timodel.ActionTruncateTablePartition,
		timodel.ActionAddTablePartition,
		timodel.ActionDropTablePartition,
		timodel.ActionExchangeTablePartition,
		timodel.ActionReorganizePartition,
		timodel.ActionAlterTablePartitioning,
		timodel.ActionRemovePartitioning:
		return "rebuild_required"
	default:
		return "direct_replay"
	}
}

func icebergSchemaVersion(event *commonEvent.DDLEvent) uint64 {
	if event == nil {
		return 0
	}
	if event.TableInfo != nil && event.TableInfo.GetUpdateTS() != 0 {
		return event.TableInfo.GetUpdateTS()
	}
	return event.GetCommitTs()
}

func (p *icebergProcessor) writeControlManifest(ctx context.Context, filePath string, value any) error {
	if p == nil || p.externalStorage == nil {
		return errors.ErrSinkURIInvalid.GenWithStackByArgs("iceberg external storage is nil")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return errors.Trace(err)
	}
	return p.externalStorage.WriteFile(ctx, filePath, payload)
}

func (p *icebergProcessor) ddlManifestPath(commitTs, seq uint64, eventID string) string {
	return path.Join(
		p.controlRootPath(),
		"ddl",
		"date="+oracle.GetTimeFromTS(commitTs).UTC().Format("2006-01-02"),
		fmt.Sprintf("commit_ts=%d", commitTs),
		fmt.Sprintf("seq=%d", seq),
		eventID+".json",
	)
}

func (p *icebergProcessor) schemaManifestPath(schemaName, tableName string, tableVersion uint64) string {
	return path.Join(
		p.controlRootPath(),
		"schema",
		escapeControlPathSegment(schemaName),
		escapeControlPathSegment(tableName),
		fmt.Sprintf("schema_%d.json", tableVersion),
	)
}

func (p *icebergProcessor) globalCheckpointManifestPath(resolvedTs uint64) string {
	return path.Join(
		p.controlRootPath(),
		"checkpoint",
		"global",
		"date="+oracle.GetTimeFromTS(resolvedTs).UTC().Format("2006-01-02"),
		fmt.Sprintf("%d.json", resolvedTs),
	)
}

func (p *icebergProcessor) controlRootPath() string {
	namespace := ""
	if p != nil && p.cfg != nil {
		namespace = p.cfg.Namespace
	}
	if namespace == "" {
		return "control"
	}
	return path.Join(escapeControlPathSegment(namespace), "control")
}

func escapeControlPathSegment(segment string) string {
	return url.PathEscape(segment)
}
