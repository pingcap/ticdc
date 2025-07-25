// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"context"
	"math/rand"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/utils/chann"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestDeframenter(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)

	inputCh := make(chan eventFragment)
	outputCh := chann.NewUnlimitedChannel[eventFragment, any](nil, nil)
	defrag := newDefragmenter(inputCh, []*chann.UnlimitedChannel[eventFragment, any]{outputCh})
	eg.Go(func() error {
		return defrag.Run(egCtx)
	})

	uri := "file:///tmp/test"
	txnCnt := 50
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	changefeedID := common.NewChangefeedID4Test("test", "table1")
	encoderConfig, err := helper.GetEncoderConfig(changefeedID, sinkURI, config.ProtocolCsv,
		replicaConfig.Sink, config.DefaultMaxMessageBytes)
	require.NoError(t, err)

	var seqNumbers []uint64
	for i := 0; i < txnCnt; i++ {
		seqNumbers = append(seqNumbers, uint64(i+1))
	}
	rand.New(rand.NewSource(time.Now().UnixNano()))
	rand.Shuffle(len(seqNumbers), func(i, j int) {
		seqNumbers[i], seqNumbers[j] = seqNumbers[j], seqNumbers[i]
	})

	tidbTableInfo := &timodel.TableInfo{
		ID:   100,
		Name: ast.NewCIStr("table1"),
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
			{ID: 2, Name: ast.NewCIStr("c2"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
		},
	}
	tableInfo := common.WrapTableInfo("test", tidbTableInfo)
	for i := 0; i < txnCnt; i++ {
		go func(seq uint64) {
			frag := eventFragment{
				versionedTable: cloudstorage.VersionedTableName{
					TableNameWithPhysicTableID: common.TableName{
						Schema:  "test",
						Table:   "table1",
						TableID: 100,
					},
				},
				seqNumber: seq,
			}
			rand.New(rand.NewSource(time.Now().UnixNano()))
			n := 1 + rand.Intn(1000)
			vals := make([]interface{}, 0, n)
			for j := 0; j < n; j++ {
				vals = append(vals, j+1, "hello world")
			}
			frag.event = &commonEvent.DMLEvent{
				PhysicalTableID:  100,
				TableInfo:        tableInfo,
				TableInfoVersion: 1,
				Rows:             chunk.MutRowFromValues(vals...).ToRow().Chunk(),
			}
			encoder, err := codec.NewTxnEventEncoder(encoderConfig)
			require.Nil(t, err)
			err = encoder.AppendTxnEvent(frag.event)
			require.NoError(t, err)
			frag.encodedMsgs = encoder.Build()

			for _, msg := range frag.encodedMsgs {
				msg.Key = []byte(strconv.Itoa(int(seq)))
			}
			inputCh <- frag
		}(uint64(i + 1))
	}

	prevSeq := 0
	buffer := make([]eventFragment, 0, 1024)

	frags, ok := outputCh.GetMultipleNoGroup(buffer)
	if !ok {
		log.Panic("unexpected error")
	}
	for _, frag := range frags {
		for _, msg := range frag.encodedMsgs {
			curSeq, err := strconv.Atoi(string(msg.Key))
			require.Nil(t, err)
			require.GreaterOrEqual(t, curSeq, prevSeq)
			prevSeq = curSeq
		}
	}

	cancel()
	require.ErrorIs(t, eg.Wait(), context.Canceled)
}
