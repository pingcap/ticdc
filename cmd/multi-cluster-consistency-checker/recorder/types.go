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
)

type DataLossItem struct {
	DownstreamClusterID string
	PK                  string
	OriginTS            uint64
	CommitTS            uint64
	Inconsistent        bool
}

func (item *DataLossItem) String() string {
	errType := "data loss"
	if item.Inconsistent {
		errType = "data inconsistent"
	}
	return fmt.Sprintf("downstream cluster: %s, pk: %s, origin ts: %d, commit ts: %d, type: %s", item.DownstreamClusterID, item.PK, item.OriginTS, item.CommitTS, errType)
}

type DataRedundantItem struct {
	PK       string
	OriginTS uint64
	CommitTS uint64
}

func (item *DataRedundantItem) String() string {
	return fmt.Sprintf("pk: %s, origin ts: %d, commit ts: %d", item.PK, item.OriginTS, item.CommitTS)
}

type LWWViolationItem struct {
	PK               string
	ExistingOriginTS uint64
	ExistingCommitTS uint64
	OriginTS         uint64
	CommitTS         uint64
}

func (item *LWWViolationItem) String() string {
	return fmt.Sprintf(
		"pk: %s, existing origin ts: %d, existing commit ts: %d, origin ts: %d, commit ts: %d",
		item.PK, item.ExistingOriginTS, item.ExistingCommitTS, item.OriginTS, item.CommitTS)
}

type ClusterReport struct {
	ClusterID string

	DataLossItems      []DataLossItem
	DataRedundantItems []DataRedundantItem
	LWWViolationItems  []LWWViolationItem
}

func NewClusterReport(clusterID string) *ClusterReport {
	return &ClusterReport{
		ClusterID:          clusterID,
		DataLossItems:      make([]DataLossItem, 0),
		DataRedundantItems: make([]DataRedundantItem, 0),
		LWWViolationItems:  make([]LWWViolationItem, 0),
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
}

func (r *ClusterReport) AddDataRedundantItem(pk string, originTS, commitTS uint64) {
	r.DataRedundantItems = append(r.DataRedundantItems, DataRedundantItem{
		PK:       pk,
		OriginTS: originTS,
		CommitTS: commitTS,
	})
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
}

type Report struct {
	Round          uint64
	ClusterReports map[string]*ClusterReport
}

func NewReport(round uint64) *Report {
	return &Report{
		Round:          round,
		ClusterReports: make(map[string]*ClusterReport),
	}
}

func (r *Report) AddClusterReport(clusterID string, clusterReport *ClusterReport) {
	r.ClusterReports[clusterID] = clusterReport
}

func (r *Report) MarshalReport() string {
	var reportMsg strings.Builder
	reportMsg.WriteString(fmt.Sprintf("round: %d\n", r.Round))
	for clusterID, clusterReport := range r.ClusterReports {
		reportMsg.WriteString(fmt.Sprintf("\n[cluster: %s]\n", clusterID))
		reportMsg.WriteString(fmt.Sprintf("  - [data loss items: %d]\n", len(clusterReport.DataLossItems)))
		for _, dataLossItem := range clusterReport.DataLossItems {
			reportMsg.WriteString(fmt.Sprintf("    - [%s]\n", dataLossItem.String()))
		}
		reportMsg.WriteString(fmt.Sprintf("  - [data redundant items: %d]\n", len(clusterReport.DataRedundantItems)))
		for _, dataRedundantItem := range clusterReport.DataRedundantItems {
			reportMsg.WriteString(fmt.Sprintf("    - [%s]\n", dataRedundantItem.String()))
		}
		reportMsg.WriteString(fmt.Sprintf("  - [lww violation items: %d]\n", len(clusterReport.LWWViolationItems)))
		for _, lwwViolationItem := range clusterReport.LWWViolationItems {
			reportMsg.WriteString(fmt.Sprintf("    - [%s]\n", lwwViolationItem.String()))
		}
	}
	reportMsg.WriteString("\n")
	return reportMsg.String()
}
