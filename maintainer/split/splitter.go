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

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/tikv/client-go/v2/tikv"
)

const MaxRegionCountForWriteBytesSplit = 5000

type SplitType string

const (
	SplitTypeWriteBytes  SplitType = "write_bytes"
	SplitTypeRegionCount SplitType = "region_count"
)

type splitter interface {
	split(
		ctx context.Context, span *heartbeatpb.TableSpan, spansNum int,
	) []*heartbeatpb.TableSpan
}

type Splitter struct {
	regionCounterSplitter *regionCountSplitter
	writeBytesSplitter    *writeBytesSplitter
	changefeedID          common.ChangeFeedID
}

// Now we only support Split span by region count,  each span will contains similar count of regions.
func NewSplitter(
	keyspaceID uint32,
	changefeedID common.ChangeFeedID,
	config *config.ChangefeedSchedulerConfig,
) *Splitter {
	return &Splitter{
		changefeedID:          changefeedID,
		regionCounterSplitter: newRegionCountSplitter(keyspaceID, changefeedID, config.RegionCountPerSpan, config.RegionThreshold),
		writeBytesSplitter:    newWriteBytesSplitter(keyspaceID, changefeedID),
	}
}

func (s *Splitter) Split(ctx context.Context,
	span *heartbeatpb.TableSpan, spansNum int, splitType SplitType,
) []*heartbeatpb.TableSpan {
	switch splitType {
	case SplitTypeWriteBytes:
		return s.writeBytesSplitter.split(ctx, span, spansNum)
	case SplitTypeRegionCount:
		return s.regionCounterSplitter.split(ctx, span, spansNum)
	}
	return nil
}

// RegionCache is a simplified interface of tikv.RegionCache.
// It is useful to restrict RegionCache usage and mocking in tests.
type RegionCache interface {
	// ListRegionIDsInKeyRange lists ids of regions in [startKey,endKey].
	LoadRegionsInKeyRange(
		bo *tikv.Backoffer, startKey, endKey []byte,
	) (regions []*tikv.Region, err error)
}

func GetSplitType(regionCount int) SplitType {
	if regionCount < MaxRegionCountForWriteBytesSplit {
		return SplitTypeWriteBytes
	}
	return SplitTypeRegionCount
}
