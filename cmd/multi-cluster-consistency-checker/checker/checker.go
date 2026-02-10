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

package checker

import (
	"context"
	"sort"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/config"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/decoder"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/recorder"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"go.uber.org/zap"
)

type versionCacheEntry struct {
	previous   int
	cdcVersion types.CdcVersion
}

type clusterViolationChecker struct {
	clusterID                            string
	twoPreviousTimeWindowKeyVersionCache map[types.PkType]versionCacheEntry
}

func newClusterViolationChecker(clusterID string) *clusterViolationChecker {
	return &clusterViolationChecker{
		clusterID:                            clusterID,
		twoPreviousTimeWindowKeyVersionCache: make(map[types.PkType]versionCacheEntry),
	}
}

func (c *clusterViolationChecker) NewRecordFromCheckpoint(record *decoder.Record, previous int) {
	entry, exists := c.twoPreviousTimeWindowKeyVersionCache[record.Pk]
	if !exists {
		c.twoPreviousTimeWindowKeyVersionCache[record.Pk] = versionCacheEntry{
			previous:   previous,
			cdcVersion: record.CdcVersion,
		}
		return
	}
	entryCompareTs := entry.cdcVersion.GetCompareTs()
	recordCompareTs := record.GetCompareTs()
	if entryCompareTs < recordCompareTs {
		c.twoPreviousTimeWindowKeyVersionCache[record.Pk] = versionCacheEntry{
			previous:   previous,
			cdcVersion: record.CdcVersion,
		}
	}
}

func (c *clusterViolationChecker) Check(r *decoder.Record, report *recorder.ClusterReport) {
	entry, exists := c.twoPreviousTimeWindowKeyVersionCache[r.Pk]
	if !exists {
		c.twoPreviousTimeWindowKeyVersionCache[r.Pk] = versionCacheEntry{
			previous:   0,
			cdcVersion: r.CdcVersion,
		}
		return
	}
	if entry.cdcVersion.CommitTs >= r.CommitTs {
		// duplicated old version, just skip it
		return
	}
	entryCompareTs := entry.cdcVersion.GetCompareTs()
	recordCompareTs := r.GetCompareTs()
	if entryCompareTs >= recordCompareTs {
		// violation detected
		log.Error("LWW violation detected",
			zap.String("clusterID", c.clusterID),
			zap.Any("entry", entry),
			zap.Any("record", r))
		report.AddLWWViolationItem(string(r.Pk), entry.cdcVersion.OriginTs, entry.cdcVersion.CommitTs, r.OriginTs, r.CommitTs)
		return
	}
	c.twoPreviousTimeWindowKeyVersionCache[r.Pk] = versionCacheEntry{
		previous:   0,
		cdcVersion: r.CdcVersion,
	}
}

func (c *clusterViolationChecker) UpdateCache() {
	newTwoPreviousTimeWindowKeyVersionCache := make(map[types.PkType]versionCacheEntry)
	for primaryKey, entry := range c.twoPreviousTimeWindowKeyVersionCache {
		if entry.previous >= 2 {
			continue
		}
		newTwoPreviousTimeWindowKeyVersionCache[primaryKey] = versionCacheEntry{
			previous:   entry.previous + 1,
			cdcVersion: entry.cdcVersion,
		}
	}
	c.twoPreviousTimeWindowKeyVersionCache = newTwoPreviousTimeWindowKeyVersionCache
}

type timeWindowDataCache struct {
	// upstreamDataCache is a map of primary key to a map of commit ts to a record
	upstreamDataCache map[types.PkType]map[uint64]*decoder.Record

	// downstreamDataCache is a map of primary key to a map of origin ts to a record
	downstreamDataCache map[types.PkType]map[uint64]*decoder.Record

	leftBoundary  uint64
	rightBoundary uint64
	checkpointTs  map[string]uint64
}

func newTimeWindowDataCache(leftBoundary, rightBoundary uint64, checkpointTs map[string]uint64) timeWindowDataCache {
	return timeWindowDataCache{
		upstreamDataCache:   make(map[types.PkType]map[uint64]*decoder.Record),
		downstreamDataCache: make(map[types.PkType]map[uint64]*decoder.Record),
		leftBoundary:        leftBoundary,
		rightBoundary:       rightBoundary,
		checkpointTs:        checkpointTs,
	}
}

func (twdc *timeWindowDataCache) newUpstreamRecord(record *decoder.Record) {
	recordsMap, exists := twdc.upstreamDataCache[record.Pk]
	if !exists {
		recordsMap = make(map[uint64]*decoder.Record)
		twdc.upstreamDataCache[record.Pk] = recordsMap
	}
	recordsMap[record.CommitTs] = record
}

func (twdc *timeWindowDataCache) newDownstreamRecord(record *decoder.Record) {
	recordsMap, exists := twdc.downstreamDataCache[record.Pk]
	if !exists {
		recordsMap = make(map[uint64]*decoder.Record)
		twdc.downstreamDataCache[record.Pk] = recordsMap
	}
	recordsMap[record.OriginTs] = record
}

func (twdc *timeWindowDataCache) NewRecord(record *decoder.Record) {
	if record.CommitTs <= twdc.leftBoundary {
		// record is before the left boundary, just skip it
		return
	}
	if record.OriginTs == 0 {
		twdc.newUpstreamRecord(record)
	} else {
		twdc.newDownstreamRecord(record)
	}
}

type clusterDataChecker struct {
	clusterID string

	timeWindowDataCaches [3]timeWindowDataCache

	rightBoundary uint64

	overDataCaches []*decoder.Record

	clusterViolationChecker *clusterViolationChecker

	report *recorder.ClusterReport
}

func newClusterDataChecker(clusterID string) *clusterDataChecker {
	return &clusterDataChecker{
		clusterID:               clusterID,
		timeWindowDataCaches:    [3]timeWindowDataCache{},
		rightBoundary:           0,
		overDataCaches:          make([]*decoder.Record, 0),
		clusterViolationChecker: newClusterViolationChecker(clusterID),
	}
}

func (cd *clusterDataChecker) InitializeFromCheckpoint(
	ctx context.Context,
	checkpointDataMap map[cloudstorage.DmlPathKey]types.IncrementalData,
	checkpoint *recorder.Checkpoint,
) error {
	if checkpoint == nil {
		return nil
	}
	if checkpoint.CheckpointItems[2] == nil {
		return nil
	}
	clusterInfo := checkpoint.CheckpointItems[2].ClusterInfo[cd.clusterID]
	cd.rightBoundary = clusterInfo.TimeWindow.RightBoundary
	cd.timeWindowDataCaches[2] = newTimeWindowDataCache(
		clusterInfo.TimeWindow.LeftBoundary, clusterInfo.TimeWindow.RightBoundary, clusterInfo.TimeWindow.CheckpointTs)
	if checkpoint.CheckpointItems[1] != nil {
		clusterInfo = checkpoint.CheckpointItems[1].ClusterInfo[cd.clusterID]
		cd.timeWindowDataCaches[1] = newTimeWindowDataCache(
			clusterInfo.TimeWindow.LeftBoundary, clusterInfo.TimeWindow.RightBoundary, clusterInfo.TimeWindow.CheckpointTs)
	}
	for _, incrementalData := range checkpointDataMap {
		for _, contents := range incrementalData.DataContentSlices {
			for _, content := range contents {
				records, err := decoder.Decode(content)
				if err != nil {
					return errors.Trace(err)
				}
				for _, record := range records {
					cd.newRecordFromCheckpoint(record)
				}
			}
		}
	}
	return nil
}

func (cd *clusterDataChecker) newRecordFromCheckpoint(record *decoder.Record) {
	if record.CommitTs > cd.rightBoundary {
		cd.overDataCaches = append(cd.overDataCaches, record)
		return
	}
	if cd.timeWindowDataCaches[2].leftBoundary < record.CommitTs {
		cd.timeWindowDataCaches[2].NewRecord(record)
		cd.clusterViolationChecker.NewRecordFromCheckpoint(record, 1)

	} else if cd.timeWindowDataCaches[1].leftBoundary < record.CommitTs {
		cd.timeWindowDataCaches[1].NewRecord(record)
		cd.clusterViolationChecker.NewRecordFromCheckpoint(record, 2)
	}
}

func (cd *clusterDataChecker) PrepareNextTimeWindowData(timeWindow types.TimeWindow) error {
	if timeWindow.LeftBoundary != cd.rightBoundary {
		return errors.Errorf("time window left boundary(%d) mismatch right boundary ts(%d)", timeWindow.LeftBoundary, cd.rightBoundary)
	}
	cd.timeWindowDataCaches[0] = cd.timeWindowDataCaches[1]
	cd.timeWindowDataCaches[1] = cd.timeWindowDataCaches[2]
	newTimeWindowDataCache := newTimeWindowDataCache(timeWindow.LeftBoundary, timeWindow.RightBoundary, timeWindow.CheckpointTs)
	cd.rightBoundary = timeWindow.RightBoundary
	newOverDataCache := make([]*decoder.Record, 0, len(cd.overDataCaches))
	for _, overRecord := range cd.overDataCaches {
		if overRecord.CommitTs > timeWindow.RightBoundary {
			newOverDataCache = append(newOverDataCache, overRecord)
		} else {
			newTimeWindowDataCache.NewRecord(overRecord)
		}
	}
	cd.timeWindowDataCaches[2] = newTimeWindowDataCache
	cd.overDataCaches = newOverDataCache
	return nil
}

func (cd *clusterDataChecker) NewRecord(record *decoder.Record) {
	if record.CommitTs > cd.rightBoundary {
		cd.overDataCaches = append(cd.overDataCaches, record)
		return
	}
	cd.timeWindowDataCaches[2].NewRecord(record)
}

func (cd *clusterDataChecker) findClusterDownstreamDataInTimeWindow(timeWindowIdx int, pk types.PkType, originTs uint64) (*decoder.Record, bool) {
	records, exists := cd.timeWindowDataCaches[timeWindowIdx].downstreamDataCache[pk]
	if !exists {
		return nil, false
	}
	if record, exists := records[originTs]; exists {
		return record, false
	}
	for _, record := range records {
		if record.GetCompareTs() >= originTs {
			return nil, true
		}
	}
	return nil, false
}

func (cd *clusterDataChecker) findClusterUpstreamDataInTimeWindow(timeWindowIdx int, pk types.PkType, commitTs uint64) bool {
	records, exists := cd.timeWindowDataCaches[timeWindowIdx].upstreamDataCache[pk]
	if !exists {
		return false
	}
	_, exists = records[commitTs]
	return exists
}

// datalossDetection iterates through the upstream data cache [1] and [2] and filter out the records
// whose checkpoint ts falls within the (checkpoint[1], checkpoint[2]]. The record must be present
// in the downstream data cache [1] or [2] or another new record is present in the downstream data
// cache [1] or [2].
func (cd *clusterDataChecker) dataLossDetection(checker *DataChecker) {
	for _, upstreamDataCache := range cd.timeWindowDataCaches[1].upstreamDataCache {
		for _, record := range upstreamDataCache {
			for downstreamClusterID, checkpointTs := range cd.timeWindowDataCaches[1].checkpointTs {
				if record.CommitTs <= checkpointTs {
					continue
				}
				downstreamRecord, skipped := checker.FindClusterDownstreamData(downstreamClusterID, record.Pk, record.CommitTs)
				if skipped {
					continue
				}
				if downstreamRecord == nil {
					// data loss detected
					log.Error("data loss detected",
						zap.String("upstreamClusterID", cd.clusterID),
						zap.String("downstreamClusterID", downstreamClusterID),
						zap.Any("record", record))
					cd.report.AddDataLossItem(downstreamClusterID, string(record.Pk), record.OriginTs, record.CommitTs, false)
				} else if !record.EqualDownstreamRecord(downstreamRecord) {
					// data inconsistent detected
					log.Error("data inconsistent detected",
						zap.String("upstreamClusterID", cd.clusterID),
						zap.String("downstreamClusterID", downstreamClusterID),
						zap.Any("record", record))
					cd.report.AddDataLossItem(downstreamClusterID, string(record.Pk), record.OriginTs, record.CommitTs, true)
				}
			}
		}
	}
	for _, upstreamDataCache := range cd.timeWindowDataCaches[2].upstreamDataCache {
		for _, record := range upstreamDataCache {
			for downstreamClusterID, checkpointTs := range cd.timeWindowDataCaches[2].checkpointTs {
				if record.CommitTs > checkpointTs {
					continue
				}
				downstreamRecord, skipped := checker.FindClusterDownstreamData(downstreamClusterID, record.Pk, record.CommitTs)
				if skipped {
					continue
				}
				if downstreamRecord == nil {
					// data loss detected
					log.Error("data loss detected",
						zap.String("upstreamClusterID", cd.clusterID),
						zap.String("downstreamClusterID", downstreamClusterID),
						zap.Any("record", record))
					cd.report.AddDataLossItem(downstreamClusterID, string(record.Pk), record.OriginTs, record.CommitTs, false)
				} else if !record.EqualDownstreamRecord(downstreamRecord) {
					// data inconsistent detected
					log.Error("data inconsistent detected",
						zap.String("upstreamClusterID", cd.clusterID),
						zap.String("downstreamClusterID", downstreamClusterID),
						zap.Any("record", record))
					cd.report.AddDataLossItem(downstreamClusterID, string(record.Pk), record.OriginTs, record.CommitTs, true)
				}
			}
		}
	}
}

// dataRedundantDetection iterates through the downstream data cache [2]. The record must be present
// in the upstream data cache [1] [2] or [3].
func (cd *clusterDataChecker) dataRedundantDetection(checker *DataChecker) {
	for _, downstreamDataCache := range cd.timeWindowDataCaches[2].downstreamDataCache {
		for _, record := range downstreamDataCache {
			// For downstream records, OriginTs is the upstream commit ts
			if !checker.FindClusterUpstreamData(cd.clusterID, record.Pk, record.OriginTs) {
				// data redundant detected
				log.Error("data redundant detected",
					zap.String("downstreamClusterID", cd.clusterID),
					zap.Any("record", record))
				cd.report.AddDataRedundantItem(string(record.Pk), record.OriginTs, record.CommitTs)
			}
		}
	}
}

// lwwViolationDetection check the orderliness of the records
func (cd *clusterDataChecker) lwwViolationDetection() {
	for pk, upstreamRecords := range cd.timeWindowDataCaches[2].upstreamDataCache {
		downstreamRecords := cd.timeWindowDataCaches[2].downstreamDataCache[pk]
		pkRecords := make([]*decoder.Record, 0, len(upstreamRecords)+len(downstreamRecords))
		for _, upstreamRecord := range upstreamRecords {
			pkRecords = append(pkRecords, upstreamRecord)
		}
		for _, downstreamRecord := range downstreamRecords {
			pkRecords = append(pkRecords, downstreamRecord)
		}
		sort.Slice(pkRecords, func(i, j int) bool {
			return pkRecords[i].CommitTs < pkRecords[j].CommitTs
		})
		for _, record := range pkRecords {
			cd.clusterViolationChecker.Check(record, cd.report)
		}
	}
	for pk, downstreamRecords := range cd.timeWindowDataCaches[2].downstreamDataCache {
		if _, exists := cd.timeWindowDataCaches[2].upstreamDataCache[pk]; exists {
			continue
		}
		pkRecords := make([]*decoder.Record, 0, len(downstreamRecords))
		for _, downstreamRecord := range downstreamRecords {
			pkRecords = append(pkRecords, downstreamRecord)
		}
		sort.Slice(pkRecords, func(i, j int) bool {
			return pkRecords[i].CommitTs < pkRecords[j].CommitTs
		})
		for _, record := range pkRecords {
			cd.clusterViolationChecker.Check(record, cd.report)
		}
	}

	cd.clusterViolationChecker.UpdateCache()
}

func (cd *clusterDataChecker) Check(checker *DataChecker) {
	cd.report = recorder.NewClusterReport(cd.clusterID)
	// CHECK 1 - Data Loss Detection
	cd.dataLossDetection(checker)
	// CHECK 2 - Data Redundant Detection
	cd.dataRedundantDetection(checker)
	// CHECK 3 - LWW Violation Detection
	cd.lwwViolationDetection()
}

func (cd *clusterDataChecker) GetReport() *recorder.ClusterReport {
	return cd.report
}

type DataChecker struct {
	round               uint64
	checkableRound      uint64
	clusterDataCheckers map[string]*clusterDataChecker
}

func NewDataChecker(ctx context.Context, clusterConfig map[string]config.ClusterConfig, checkpointDataMap map[string]map[cloudstorage.DmlPathKey]types.IncrementalData, checkpoint *recorder.Checkpoint) *DataChecker {
	clusterDataChecker := make(map[string]*clusterDataChecker)
	for clusterID := range clusterConfig {
		clusterDataChecker[clusterID] = newClusterDataChecker(clusterID)
	}
	checker := &DataChecker{
		round:               0,
		checkableRound:      0,
		clusterDataCheckers: clusterDataChecker,
	}
	checker.initializeFromCheckpoint(ctx, checkpointDataMap, checkpoint)
	return checker
}

func (c *DataChecker) initializeFromCheckpoint(ctx context.Context, checkpointDataMap map[string]map[cloudstorage.DmlPathKey]types.IncrementalData, checkpoint *recorder.Checkpoint) {
	if checkpoint == nil {
		return
	}
	if checkpoint.CheckpointItems[2] == nil {
		return
	}
	c.round = checkpoint.CheckpointItems[2].Round + 1
	c.checkableRound = checkpoint.CheckpointItems[2].Round + 1
	for _, clusterDataChecker := range c.clusterDataCheckers {
		clusterDataChecker.InitializeFromCheckpoint(ctx, checkpointDataMap[clusterDataChecker.clusterID], checkpoint)
	}
}

// FindClusterDownstreamData checks whether the record is present in the downstream data
// cache [1] or [2] or another new record is present in the downstream data cache [1] or [2].
func (c *DataChecker) FindClusterDownstreamData(clusterID string, pk types.PkType, originTs uint64) (*decoder.Record, bool) {
	clusterDataChecker, exists := c.clusterDataCheckers[clusterID]
	if !exists {
		return nil, false
	}
	record, skipped := clusterDataChecker.findClusterDownstreamDataInTimeWindow(1, pk, originTs)
	if skipped || record != nil {
		return record, skipped
	}
	return clusterDataChecker.findClusterDownstreamDataInTimeWindow(2, pk, originTs)
}

func (c *DataChecker) FindClusterUpstreamData(downstreamClusterID string, pk types.PkType, commitTs uint64) bool {
	for _, clusterDataChecker := range c.clusterDataCheckers {
		if clusterDataChecker.clusterID == downstreamClusterID {
			continue
		}
		if clusterDataChecker.findClusterUpstreamDataInTimeWindow(0, pk, commitTs) {
			return true
		}
		if clusterDataChecker.findClusterUpstreamDataInTimeWindow(1, pk, commitTs) {
			return true
		}
		if clusterDataChecker.findClusterUpstreamDataInTimeWindow(2, pk, commitTs) {
			return true
		}
	}
	return false
}

func (c *DataChecker) CheckInNextTimeWindow(ctx context.Context, newTimeWindowData map[string]types.TimeWindowData) (*recorder.Report, error) {
	if err := c.decodeNewTimeWindowData(ctx, newTimeWindowData); err != nil {
		log.Error("failed to decode new time window data", zap.Error(err))
		return nil, errors.Annotate(err, "failed to decode new time window data")
	}
	report := recorder.NewReport(c.round)
	if c.checkableRound >= 3 {
		for clusterID, clusterDataChecker := range c.clusterDataCheckers {
			clusterDataChecker.Check(c)
			report.AddClusterReport(clusterID, clusterDataChecker.GetReport())
		}
	} else {
		c.checkableRound++
	}
	c.round++
	return report, nil
}

func (c *DataChecker) decodeNewTimeWindowData(ctx context.Context, newTimeWindowData map[string]types.TimeWindowData) error {
	if len(newTimeWindowData) != len(c.clusterDataCheckers) {
		return errors.Errorf("number of clusters mismatch, expected %d, got %d", len(c.clusterDataCheckers), len(newTimeWindowData))
	}
	for clusterID, timeWindowData := range newTimeWindowData {
		clusterDataChecker, exists := c.clusterDataCheckers[clusterID]
		if !exists {
			return errors.Errorf("cluster %s not found", clusterID)
		}
		if err := clusterDataChecker.PrepareNextTimeWindowData(timeWindowData.TimeWindow); err != nil {
			return errors.Trace(err)
		}
		for _, incrementalData := range timeWindowData.Data {
			for _, contents := range incrementalData.DataContentSlices {
				for _, content := range contents {
					records, err := decoder.Decode(content)
					if err != nil {
						return errors.Trace(err)
					}
					for _, record := range records {
						clusterDataChecker.NewRecord(record)
					}
				}
			}
		}
	}

	return nil
}
