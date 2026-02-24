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

package recorder

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/pkg/errors"
)

type DataLossItem struct {
	PeerClusterID string         `json:"peer_cluster_id"`
	PK            map[string]any `json:"pk"`
	CommitTS      uint64         `json:"commit_ts"`

	PKStr string `json:"-"`
}

func (item *DataLossItem) String() string {
	return fmt.Sprintf("peer cluster: %s, pk: %s, commit ts: %d", item.PeerClusterID, item.PKStr, item.CommitTS)
}

type InconsistentColumn struct {
	Column     string `json:"column"`
	Local      any    `json:"local"`
	Replicated any    `json:"replicated"`
}

func (c *InconsistentColumn) String() string {
	return fmt.Sprintf("column: %s, local: %v, replicated: %v", c.Column, c.Local, c.Replicated)
}

type DataInconsistentItem struct {
	PeerClusterID       string               `json:"peer_cluster_id"`
	PK                  map[string]any       `json:"pk"`
	OriginTS            uint64               `json:"origin_ts"`
	LocalCommitTS       uint64               `json:"local_commit_ts"`
	ReplicatedCommitTS  uint64               `json:"replicated_commit_ts"`
	InconsistentColumns []InconsistentColumn `json:"inconsistent_columns,omitempty"`

	PKStr string `json:"-"`
}

func (item *DataInconsistentItem) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "peer cluster: %s, pk: %s, origin ts: %d, local commit ts: %d, replicated commit ts: %d",
		item.PeerClusterID, item.PKStr, item.OriginTS, item.LocalCommitTS, item.ReplicatedCommitTS)
	if len(item.InconsistentColumns) > 0 {
		sb.WriteString(", inconsistent columns: [")
		for i, col := range item.InconsistentColumns {
			if i > 0 {
				sb.WriteString("; ")
			}
			sb.WriteString(col.String())
		}
		sb.WriteString("]")
	}
	return sb.String()
}

type DataRedundantItem struct {
	PK       map[string]any `json:"pk"`
	OriginTS uint64         `json:"origin_ts"`
	CommitTS uint64         `json:"commit_ts"`

	PKStr string `json:"-"`
}

func (item *DataRedundantItem) String() string {
	return fmt.Sprintf("pk: %s, origin ts: %d, commit ts: %d", item.PKStr, item.OriginTS, item.CommitTS)
}

type LWWViolationItem struct {
	PK               map[string]any `json:"pk"`
	ExistingOriginTS uint64         `json:"existing_origin_ts"`
	ExistingCommitTS uint64         `json:"existing_commit_ts"`
	OriginTS         uint64         `json:"origin_ts"`
	CommitTS         uint64         `json:"commit_ts"`

	PKStr string `json:"-"`
}

func (item *LWWViolationItem) String() string {
	return fmt.Sprintf(
		"pk: %s, existing origin ts: %d, existing commit ts: %d, origin ts: %d, commit ts: %d",
		item.PKStr, item.ExistingOriginTS, item.ExistingCommitTS, item.OriginTS, item.CommitTS)
}

type TableFailureItems struct {
	DataLossItems         []DataLossItem         `json:"data_loss_items"`         // data loss items
	DataInconsistentItems []DataInconsistentItem `json:"data_inconsistent_items"` // data inconsistent items
	DataRedundantItems    []DataRedundantItem    `json:"data_redundant_items"`    // data redundant items
	LWWViolationItems     []LWWViolationItem     `json:"lww_violation_items"`     // lww violation items
}

func NewTableFailureItems() *TableFailureItems {
	return &TableFailureItems{
		DataLossItems:         make([]DataLossItem, 0),
		DataInconsistentItems: make([]DataInconsistentItem, 0),
		DataRedundantItems:    make([]DataRedundantItem, 0),
		LWWViolationItems:     make([]LWWViolationItem, 0),
	}
}

type ClusterReport struct {
	ClusterID string `json:"cluster_id"`

	TimeWindow types.TimeWindow `json:"time_window"`

	TableFailureItems map[string]*TableFailureItems `json:"table_failure_items"` // table failure items

	needFlush bool `json:"-"`
}

func NewClusterReport(clusterID string, timeWindow types.TimeWindow) *ClusterReport {
	return &ClusterReport{
		ClusterID:         clusterID,
		TimeWindow:        timeWindow,
		TableFailureItems: make(map[string]*TableFailureItems),
		needFlush:         false,
	}
}

func (r *ClusterReport) AddDataLossItem(
	peerClusterID, schemaKey string,
	pk map[string]any,
	pkStr string,
	commitTS uint64,
) {
	tableFailureItems, exists := r.TableFailureItems[schemaKey]
	if !exists {
		tableFailureItems = NewTableFailureItems()
		r.TableFailureItems[schemaKey] = tableFailureItems
	}
	tableFailureItems.DataLossItems = append(tableFailureItems.DataLossItems, DataLossItem{
		PeerClusterID: peerClusterID,
		PK:            pk,
		CommitTS:      commitTS,

		PKStr: pkStr,
	})
	r.needFlush = true
}

func (r *ClusterReport) AddDataInconsistentItem(
	peerClusterID, schemaKey string,
	pk map[string]any,
	pkStr string,
	originTS, localCommitTS, replicatedCommitTS uint64,
	inconsistentColumns []InconsistentColumn,
) {
	tableFailureItems, exists := r.TableFailureItems[schemaKey]
	if !exists {
		tableFailureItems = NewTableFailureItems()
		r.TableFailureItems[schemaKey] = tableFailureItems
	}
	tableFailureItems.DataInconsistentItems = append(tableFailureItems.DataInconsistentItems, DataInconsistentItem{
		PeerClusterID:       peerClusterID,
		PK:                  pk,
		OriginTS:            originTS,
		LocalCommitTS:       localCommitTS,
		ReplicatedCommitTS:  replicatedCommitTS,
		InconsistentColumns: inconsistentColumns,

		PKStr: pkStr,
	})
	r.needFlush = true
}

func (r *ClusterReport) AddDataRedundantItem(
	schemaKey string,
	pk map[string]any,
	pkStr string,
	originTS, commitTS uint64,
) {
	tableFailureItems, exists := r.TableFailureItems[schemaKey]
	if !exists {
		tableFailureItems = NewTableFailureItems()
		r.TableFailureItems[schemaKey] = tableFailureItems
	}
	tableFailureItems.DataRedundantItems = append(tableFailureItems.DataRedundantItems, DataRedundantItem{
		PK:       pk,
		OriginTS: originTS,
		CommitTS: commitTS,

		PKStr: pkStr,
	})
	r.needFlush = true
}

func (r *ClusterReport) AddLWWViolationItem(
	schemaKey string,
	pk map[string]any,
	pkStr string,
	existingOriginTS, existingCommitTS uint64,
	originTS, commitTS uint64,
) {
	tableFailureItems, exists := r.TableFailureItems[schemaKey]
	if !exists {
		tableFailureItems = NewTableFailureItems()
		r.TableFailureItems[schemaKey] = tableFailureItems
	}
	tableFailureItems.LWWViolationItems = append(tableFailureItems.LWWViolationItems, LWWViolationItem{
		PK:               pk,
		ExistingOriginTS: existingOriginTS,
		ExistingCommitTS: existingCommitTS,
		OriginTS:         originTS,
		CommitTS:         commitTS,

		PKStr: pkStr,
	})
	r.needFlush = true
}

type Report struct {
	Round          uint64                    `json:"round"`
	ClusterReports map[string]*ClusterReport `json:"cluster_reports"`
	needFlush      bool                      `json:"-"`
}

func NewReport(round uint64) *Report {
	return &Report{
		Round:          round,
		ClusterReports: make(map[string]*ClusterReport),
		needFlush:      false,
	}
}

func (r *Report) AddClusterReport(clusterID string, clusterReport *ClusterReport) {
	r.ClusterReports[clusterID] = clusterReport
	r.needFlush = r.needFlush || clusterReport.needFlush
}

func (r *Report) MarshalReport() string {
	var reportMsg strings.Builder
	fmt.Fprintf(&reportMsg, "round: %d\n", r.Round)

	// Sort cluster IDs for deterministic output
	clusterIDs := make([]string, 0, len(r.ClusterReports))
	for clusterID := range r.ClusterReports {
		clusterIDs = append(clusterIDs, clusterID)
	}
	sort.Strings(clusterIDs)

	for _, clusterID := range clusterIDs {
		clusterReport := r.ClusterReports[clusterID]
		if !clusterReport.needFlush {
			continue
		}
		fmt.Fprintf(&reportMsg, "\n[cluster: %s]\n", clusterID)
		fmt.Fprintf(&reportMsg, "time window: %s\n", clusterReport.TimeWindow.String())

		// Sort schema keys for deterministic output
		schemaKeys := make([]string, 0, len(clusterReport.TableFailureItems))
		for schemaKey := range clusterReport.TableFailureItems {
			schemaKeys = append(schemaKeys, schemaKey)
		}
		sort.Strings(schemaKeys)

		for _, schemaKey := range schemaKeys {
			tableFailureItems := clusterReport.TableFailureItems[schemaKey]
			fmt.Fprintf(&reportMsg, "  - [table name: %s]\n", schemaKey)
			if len(tableFailureItems.DataLossItems) > 0 {
				fmt.Fprintf(&reportMsg, "  - [data loss items: %d]\n", len(tableFailureItems.DataLossItems))
				for _, dataLossItem := range tableFailureItems.DataLossItems {
					fmt.Fprintf(&reportMsg, "    - [%s]\n", dataLossItem.String())
				}
			}
			if len(tableFailureItems.DataInconsistentItems) > 0 {
				fmt.Fprintf(&reportMsg, "  - [data inconsistent items: %d]\n", len(tableFailureItems.DataInconsistentItems))
				for _, dataInconsistentItem := range tableFailureItems.DataInconsistentItems {
					fmt.Fprintf(&reportMsg, "    - [%s]\n", dataInconsistentItem.String())
				}
			}
			if len(tableFailureItems.DataRedundantItems) > 0 {
				fmt.Fprintf(&reportMsg, "  - [data redundant items: %d]\n", len(tableFailureItems.DataRedundantItems))
				for _, dataRedundantItem := range tableFailureItems.DataRedundantItems {
					fmt.Fprintf(&reportMsg, "    - [%s]\n", dataRedundantItem.String())
				}
			}
			if len(tableFailureItems.LWWViolationItems) > 0 {
				fmt.Fprintf(&reportMsg, "  - [lww violation items: %d]\n", len(tableFailureItems.LWWViolationItems))
				for _, lwwViolationItem := range tableFailureItems.LWWViolationItems {
					fmt.Fprintf(&reportMsg, "    - [%s]\n", lwwViolationItem.String())
				}
			}
		}
	}
	reportMsg.WriteString("\n")
	return reportMsg.String()
}

func (r *Report) NeedFlush() bool {
	return r.needFlush
}

type SchemaTableVersionKey struct {
	types.SchemaTableKey
	types.VersionKey
}

func NewSchemaTableVersionKeyFromVersionKeyMap(versionKeyMap map[types.SchemaTableKey]types.VersionKey) []SchemaTableVersionKey {
	result := make([]SchemaTableVersionKey, 0, len(versionKeyMap))
	for schemaTableKey, versionKey := range versionKeyMap {
		result = append(result, SchemaTableVersionKey{
			SchemaTableKey: schemaTableKey,
			VersionKey:     versionKey,
		})
	}
	return result
}

type CheckpointClusterInfo struct {
	TimeWindow types.TimeWindow        `json:"time_window"`
	MaxVersion []SchemaTableVersionKey `json:"max_version"`
}

type CheckpointItem struct {
	Round       uint64                           `json:"round"`
	ClusterInfo map[string]CheckpointClusterInfo `json:"cluster_info"`
}

type Checkpoint struct {
	CheckpointItems [3]*CheckpointItem `json:"checkpoint_items"`
}

func NewCheckpoint() *Checkpoint {
	return &Checkpoint{
		CheckpointItems: [3]*CheckpointItem{
			nil,
			nil,
			nil,
		},
	}
}

func (c *Checkpoint) NewTimeWindowData(round uint64, timeWindowData map[string]types.TimeWindowData) {
	newCheckpointItem := CheckpointItem{
		Round:       round,
		ClusterInfo: make(map[string]CheckpointClusterInfo),
	}
	for clusterID, timeWindow := range timeWindowData {
		newCheckpointItem.ClusterInfo[clusterID] = CheckpointClusterInfo{
			TimeWindow: timeWindow.TimeWindow,
			MaxVersion: NewSchemaTableVersionKeyFromVersionKeyMap(timeWindow.MaxVersion),
		}
	}
	c.CheckpointItems[0] = c.CheckpointItems[1]
	c.CheckpointItems[1] = c.CheckpointItems[2]
	c.CheckpointItems[2] = &newCheckpointItem
}

type ScanRange struct {
	StartVersionKey string
	EndVersionKey   string
	StartDataPath   string
	EndDataPath     string
}

func (c *Checkpoint) ToScanRange(clusterID string) (map[types.SchemaTableKey]*ScanRange, error) {
	result := make(map[types.SchemaTableKey]*ScanRange)
	if c.CheckpointItems[2] == nil {
		return result, nil
	}
	for _, versionKey := range c.CheckpointItems[2].ClusterInfo[clusterID].MaxVersion {
		result[versionKey.SchemaTableKey] = &ScanRange{
			StartVersionKey: versionKey.VersionPath,
			EndVersionKey:   versionKey.VersionPath,
			StartDataPath:   versionKey.DataPath,
			EndDataPath:     versionKey.DataPath,
		}
	}
	if c.CheckpointItems[1] == nil {
		return result, nil
	}
	for _, versionKey := range c.CheckpointItems[1].ClusterInfo[clusterID].MaxVersion {
		scanRange, ok := result[versionKey.SchemaTableKey]
		if !ok {
			return nil, errors.Errorf("schema table key %s.%s not found in result", versionKey.Schema, versionKey.Table)
		}
		scanRange.StartVersionKey = versionKey.VersionPath
		scanRange.StartDataPath = versionKey.DataPath
	}
	if c.CheckpointItems[0] == nil {
		return result, nil
	}
	for _, versionKey := range c.CheckpointItems[0].ClusterInfo[clusterID].MaxVersion {
		scanRange, ok := result[versionKey.SchemaTableKey]
		if !ok {
			return nil, errors.Errorf("schema table key %s.%s not found in result", versionKey.Schema, versionKey.Table)
		}
		scanRange.StartVersionKey = versionKey.VersionPath
		scanRange.StartDataPath = versionKey.DataPath
	}
	return result, nil
}
