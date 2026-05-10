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

package types

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
)

// PkType is a distinct type for encoded primary key strings, making it clear
// at the type level that the value has been serialized / encoded.
type PkType string

type CdcVersion struct {
	CommitTs uint64
	OriginTs uint64
}

func (v *CdcVersion) GetCompareTs() uint64 {
	if v.OriginTs > 0 {
		return v.OriginTs
	}
	return v.CommitTs
}

type SchemaTableKey struct {
	Schema string
	Table  string
}

type VersionKey struct {
	Version uint64
	// Version Path is a hint for the next version path to scan
	VersionPath string
	// Data Path is a hint for the next data path to scan
	DataPath string
}

// TimeWindow is the time window of the cluster, including the left boundary, right boundary and checkpoint ts
// Assert 1: LeftBoundary < CheckpointTs < RightBoundary
// Assert 2: The other cluster's checkpoint timestamp of next time window should be larger than the PDTimestampAfterTimeWindow saved in this cluster's time window
// Assert 3: CheckpointTs of this cluster should be larger than other clusters' RightBoundary of previous time window
// Assert 4: RightBoundary of this cluster should be larger than other clusters' CheckpointTs of this time window
type TimeWindow struct {
	LeftBoundary  uint64 `json:"left_boundary"`
	RightBoundary uint64 `json:"right_boundary"`
	// CheckpointTs is the checkpoint timestamp for each local-to-replicated changefeed,
	// mapping from replicated cluster ID to the checkpoint timestamp
	CheckpointTs map[string]uint64 `json:"checkpoint_ts"`
	// PDTimestampAfterTimeWindow is the max PD timestamp after the time window for each replicated cluster,
	// mapping from local cluster ID to the max PD timestamp
	PDTimestampAfterTimeWindow map[string]uint64 `json:"pd_timestamp_after_time_window"`
	// NextMinLeftBoundary is the minimum left boundary of the next time window for the cluster
	NextMinLeftBoundary uint64 `json:"next_min_left_boundary"`
}

func (t *TimeWindow) String() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "time window boundary: (%d, %d]\n", t.LeftBoundary, t.RightBoundary)

	// Sort cluster IDs for deterministic output
	clusterIDs := make([]string, 0, len(t.CheckpointTs))
	for id := range t.CheckpointTs {
		clusterIDs = append(clusterIDs, id)
	}
	sort.Strings(clusterIDs)

	for _, replicatedClusterID := range clusterIDs {
		fmt.Fprintf(&builder, "checkpoint ts [replicated cluster: %s]: %d\n", replicatedClusterID, t.CheckpointTs[replicatedClusterID])
	}
	return builder.String()
}

type TimeWindowData struct {
	TimeWindow
	Data       map[cloudstorage.DmlPathKey]IncrementalData
	MaxVersion map[SchemaTableKey]VersionKey
}

type IncrementalData struct {
	DataContentSlices map[cloudstorage.FileIndexKey][][]byte
	ColumnFieldTypes  map[string]*ptypes.FieldType
}
