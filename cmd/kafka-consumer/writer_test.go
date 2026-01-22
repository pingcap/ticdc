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

package main

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestAppendRow2Group_DoesNotDropCommitTsFallbackBeforeApplied(t *testing.T) {
	// Scenario:
	// 1) TiCDC writes DML messages to Kafka in commitTs order.
	// 2) Under network partition / changefeed restart, TiCDC may replay older commitTs,
	//    which will be appended to Kafka at a larger offset (commitTs appears to go backwards).
	//
	// The kafka-consumer must not drop these "fallback commitTs" events unless they have
	// already been flushed to downstream (AppliedWatermark), otherwise the replay cannot
	// heal the missing window.
	replicaCfg := config.GetDefaultReplicaConfig()
	eventRouter, err := eventrouter.NewEventRouter(replicaCfg.Sink, "test-topic", false, false)
	require.NoError(t, err)

	w := &writer{
		progresses:             []*partitionProgress{{partition: 0, eventsGroup: make(map[int64]*util.EventsGroup)}},
		eventRouter:            eventRouter,
		protocol:               config.ProtocolCanalJSON,
		partitionTableAccessor: codecCommon.NewPartitionTableAccessor(),
	}

	newDMLEvent := func(tableID int64, commitTs uint64) *commonEvent.DMLEvent {
		return &commonEvent.DMLEvent{
			PhysicalTableID: tableID,
			CommitTs:        commitTs,
			RowTypes:        []common.RowType{common.RowTypeUpdate},
			Rows:            chunk.NewChunkWithCapacity(nil, 0),
			TableInfo: &common.TableInfo{
				TableName: common.TableName{Schema: "test", Table: "t"},
			},
		}
	}

	progress := w.progresses[0]

	// Step 1: observe a larger commitTs first (e.g. produced before restart).
	w.appendRow2Group(newDMLEvent(1, 200), progress, kafka.Offset(10))

	// Step 2: observe a smaller commitTs later (e.g. replayed after restart).
	w.appendRow2Group(newDMLEvent(1, 100), progress, kafka.Offset(11))

	group := progress.eventsGroup[1]
	require.NotNil(t, group)

	// Expect: commitTs=100 is still kept and can be resolved.
	resolved := group.Resolve(150)
	require.Len(t, resolved, 1)
	require.Equal(t, uint64(100), resolved[0].CommitTs)

	// Step 3: once downstream has flushed beyond commitTs=100, the replay is safe to ignore.
	group.AppliedWatermark = 200
	w.appendRow2Group(newDMLEvent(1, 100), progress, kafka.Offset(12))
	resolved = group.Resolve(150)
	require.Empty(t, resolved)
}
