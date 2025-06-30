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
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/tikv/client-go/v2/tikv"
)

const (
	// spanRegionLimit is the maximum number of regions a span can cover.
	spanRegionLimit = 50000
	// DefaultMaxSpanNumber is the maximum number of spans that can be split
	// in single batch.
	DefaultMaxSpanNumber = 100
)

// RegionCache is a simplified interface of tikv.RegionCache.
// It is useful to restrict RegionCache usage and mocking in tests.
// TODO: change the function to get regions faster
type RegionCache interface {
	// ListRegionIDsInKeyRange lists ids of regions in [startKey,endKey].
	ListRegionIDsInKeyRange(
		bo *tikv.Backoffer, startKey, endKey []byte,
	) (regionIDs []uint64, err error)
	// LocateRegionByID searches for the region with ID.
	LocateRegionByID(bo *tikv.Backoffer, regionID uint64) (*tikv.KeyLocation, error)
}

type splitter interface {
	split(
		ctx context.Context, span *heartbeatpb.TableSpan, totalCaptures int,
	) []*heartbeatpb.TableSpan
}

type Splitter struct {
	regionCounterSplitter *regionCountSplitter
	writeKeySplitter      *writeSplitter
	changefeedID          common.ChangeFeedID
}

// NewSplitter returns a Splitter.
func NewSplitter(
	changefeedID common.ChangeFeedID,
	pdapi pdutil.PDAPIClient,
	config *config.ChangefeedSchedulerConfig,
) *Splitter {
	return &Splitter{
		changefeedID:          changefeedID,
		regionCounterSplitter: newRegionCountSplitter(changefeedID, config.RegionThreshold, config.RegionCountPerSpan),
		writeKeySplitter:      newWriteSplitter(changefeedID, pdapi, config.WriteKeyThreshold),
	}
}

func (s *Splitter) SplitSpansByRegion(ctx context.Context,
	span *heartbeatpb.TableSpan, spansNum int,
) []*heartbeatpb.TableSpan {
	spans := []*heartbeatpb.TableSpan{span}
	spans = s.regionCounterSplitter.split(ctx, span, spansNum)
	if len(spans) > 1 {
		return spans
	}
	return spans
}

func (s *Splitter) SplitSpansByWriteKey(ctx context.Context,
	span *heartbeatpb.TableSpan,
	spansNum int,
) []*heartbeatpb.TableSpan {
	spans := []*heartbeatpb.TableSpan{span}
	spans = s.writeKeySplitter.split(ctx, span, spansNum)
	if len(spans) > 1 {
		return spans
	}
	return spans
}
