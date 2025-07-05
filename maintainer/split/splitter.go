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
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// RegionCache is a simplified interface of tikv.RegionCache.
// It is useful to restrict RegionCache usage and mocking in tests.
type RegionCache interface {
	// ListRegionIDsInKeyRange lists ids of regions in [startKey,endKey].
	LoadRegionsInKeyRange(
		bo *tikv.Backoffer, startKey, endKey []byte,
	) (regions []*tikv.Region, err error)
}

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

func (s *Splitter) Split(ctx context.Context,
	span *heartbeatpb.TableSpan, spansNum int,
	splitType replica.SplitType,
) []*heartbeatpb.TableSpan {
	spans := []*heartbeatpb.TableSpan{span}
	switch splitType {
	case replica.SplitByRegion:
		spans = s.regionCounterSplitter.split(ctx, span, spansNum)
	case replica.SplitByTraffic:
		spans = s.writeKeySplitter.split(ctx, span, spansNum)
	default:
		log.Warn("splitter: unknown split type", zap.Any("splitType", splitType))
	}
	return spans
}
