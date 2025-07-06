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

package split

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type SplitType int

const (
	SplitByTraffic SplitType = iota
	SplitByRegion
)

type splitter interface {
	split(
		ctx context.Context, span *heartbeatpb.TableSpan, spansNum int,
	) []*heartbeatpb.TableSpan
}

type Splitter struct {
	regionCounterSplitter *regionCountSplitter
	writeKeySplitter      *writeSplitter
	changefeedID          common.ChangeFeedID
}

// We support two kind of splits:
//  1. Split by region count, each span will contains similar count of regions.
//     we use SplitByRegion when we add new table span, to make the incremental scan more balanced.
//     we will check whether the span exceed the region count threshold or write key count threshold.
//  2. Split by write key, each span will contains similar count of write keys.
//     we use SplitByTraffic when we do split in balance, to make the sink throughput more balanced.
func NewSplitter(
	changefeedID common.ChangeFeedID,
	pdapi pdutil.PDAPIClient,
	config *config.ChangefeedSchedulerConfig,
) *Splitter {
	return &Splitter{
		changefeedID:          changefeedID,
		regionCounterSplitter: newRegionCountSplitter(changefeedID, config.RegionCountPerSpan, config.RegionThreshold),
		writeKeySplitter:      newWriteSplitter(changefeedID, pdapi),
	}
}

func (s *Splitter) Split(ctx context.Context,
	span *heartbeatpb.TableSpan, spansNum int,
	splitType SplitType,
) []*heartbeatpb.TableSpan {
	spans := []*heartbeatpb.TableSpan{span}
	switch splitType {
	case SplitByRegion:
		spans = s.regionCounterSplitter.split(ctx, span, spansNum)
	case SplitByTraffic:
		spans = s.writeKeySplitter.split(ctx, span, spansNum)
	default:
		log.Warn("splitter: unknown split type", zap.Any("splitType", splitType))
	}
	return spans
}

// ShouldSplit is used to determine whether the span should span:
// If sink is mysql-sink, span can split only when the table only have the unique and only one primary key.
func ShouldSplit(tableInfo *common.TableInfo, isMysqlSink bool) bool {
	if isMysqlSink {
		if !tableInfo.HasPrimaryKey() {
			return false
		}
		for _, index := range tableInfo.GetIndices() {
			if index.Unique {
				return false
			}
		}
	}
	return true
}

// RegionCache is a simplified interface of tikv.RegionCache.
// It is useful to restrict RegionCache usage and mocking in tests.
type RegionCache interface {
	// ListRegionIDsInKeyRange lists ids of regions in [startKey,endKey].
	LoadRegionsInKeyRange(
		bo *tikv.Backoffer, startKey, endKey []byte,
	) (regions []*tikv.Region, err error)
}
