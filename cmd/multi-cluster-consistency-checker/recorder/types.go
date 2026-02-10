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
	"strings"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/pkg/errors"
)

type DataLossItem struct {
	DownstreamClusterID string `json:"downstream_cluster_id"`
	PK                  string `json:"pk"`
	OriginTS            uint64 `json:"origin_ts"`
	CommitTS            uint64 `json:"commit_ts"`
	Inconsistent        bool   `json:"inconsistent"`
}

func (item *DataLossItem) String() string {
	errType := "data loss"
	if item.Inconsistent {
		errType = "data inconsistent"
	}
	return fmt.Sprintf("downstream cluster: %s, pk: %s, origin ts: %d, commit ts: %d, type: %s", item.DownstreamClusterID, item.PK, item.OriginTS, item.CommitTS, errType)
}

type DataRedundantItem struct {
	PK       string `json:"pk"`
	OriginTS uint64 `json:"origin_ts"`
	CommitTS uint64 `json:"commit_ts"`
}

func (item *DataRedundantItem) String() string {
	return fmt.Sprintf("pk: %s, origin ts: %d, commit ts: %d", item.PK, item.OriginTS, item.CommitTS)
}

type LWWViolationItem struct {
	PK               string `json:"pk"`
	ExistingOriginTS uint64 `json:"existing_origin_ts"`
	ExistingCommitTS uint64 `json:"existing_commit_ts"`
	OriginTS         uint64 `json:"origin_ts"`
	CommitTS         uint64 `json:"commit_ts"`
}

func (item *LWWViolationItem) String() string {
	return fmt.Sprintf(
		"pk: %s, existing origin ts: %d, existing commit ts: %d, origin ts: %d, commit ts: %d",
		item.PK, item.ExistingOriginTS, item.ExistingCommitTS, item.OriginTS, item.CommitTS)
}

type ClusterReport struct {
	ClusterID string `json:"cluster_id"`

	DataLossItems      []DataLossItem      `json:"data_loss_items"`      // data loss items
	DataRedundantItems []DataRedundantItem `json:"data_redundant_items"` // data redundant items
	LWWViolationItems  []LWWViolationItem  `json:"lww_violation_items"`  // lww violation items

	needFlush bool `json:"-"`
}

func NewClusterReport(clusterID string) *ClusterReport {
	return &ClusterReport{
		ClusterID:          clusterID,
		DataLossItems:      make([]DataLossItem, 0),
		DataRedundantItems: make([]DataRedundantItem, 0),
		LWWViolationItems:  make([]LWWViolationItem, 0),
		needFlush:          false,
	}
}

func (r *ClusterReport) AddDataLossItem(downstreamClusterID, pk string, originTS, commitTS uint64, inconsistent bool) {
	r.DataLossItems = append(r.DataLossItems, DataLossItem{
		DownstreamClusterID: downstreamClusterID,
		PK:                  pk,
		OriginTS:            originTS,
		CommitTS:            commitTS,
		Inconsistent:        inconsistent,
	})
	r.needFlush = true
}

func (r *ClusterReport) AddDataRedundantItem(pk string, originTS, commitTS uint64) {
	r.DataRedundantItems = append(r.DataRedundantItems, DataRedundantItem{
		PK:       pk,
		OriginTS: originTS,
		CommitTS: commitTS,
	})
	r.needFlush = true
}

func (r *ClusterReport) AddLWWViolationItem(
	pk string,
	existingOriginTS, existingCommitTS uint64,
	originTS, commitTS uint64,
) {
	r.LWWViolationItems = append(r.LWWViolationItems, LWWViolationItem{
		PK:               pk,
		ExistingOriginTS: existingOriginTS,
		ExistingCommitTS: existingCommitTS,
		OriginTS:         originTS,
		CommitTS:         commitTS,
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
	for clusterID, clusterReport := range r.ClusterReports {
		if !clusterReport.needFlush {
			continue
		}
		fmt.Fprintf(&reportMsg, "\n[cluster: %s]\n", clusterID)
		if len(clusterReport.DataLossItems) > 0 {
			fmt.Fprintf(&reportMsg, "  - [data loss items: %d]\n", len(clusterReport.DataLossItems))
			for _, dataLossItem := range clusterReport.DataLossItems {
				fmt.Fprintf(&reportMsg, "    - [%s]\n", dataLossItem.String())
			}
		}
		if len(clusterReport.DataRedundantItems) > 0 {
			fmt.Fprintf(&reportMsg, "  - [data redundant items: %d]\n", len(clusterReport.DataRedundantItems))
			for _, dataRedundantItem := range clusterReport.DataRedundantItems {
				fmt.Fprintf(&reportMsg, "    - [%s]\n", dataRedundantItem.String())
			}
		}
		if len(clusterReport.LWWViolationItems) > 0 {
			fmt.Fprintf(&reportMsg, "  - [lww violation items: %d]\n", len(clusterReport.LWWViolationItems))
			for _, lwwViolationItem := range clusterReport.LWWViolationItems {
				fmt.Fprintf(&reportMsg, "    - [%s]\n", lwwViolationItem.String())
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
	for downstreamClusterID, timeWindow := range timeWindowData {
		newCheckpointItem.ClusterInfo[downstreamClusterID] = CheckpointClusterInfo{
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
