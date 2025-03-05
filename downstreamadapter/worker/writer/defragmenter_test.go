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
package writer

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestDeframenter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)

	inputCh := make(chan EventFragment)
	outputCh := chann.NewAutoDrainChann[EventFragment]()
	defrag := NewDefragmenter(inputCh, []*chann.DrainableChann[EventFragment]{outputCh})
	eg.Go(func() error {
		return defrag.Run(egCtx)
	})

	uri := "file:///tmp/test"
	txnCnt := 50
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	changefeedID := common.NewChangefeedID4Test("test", "table1")
	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, config.ProtocolCsv,
		replicaConfig.Sink, config.DefaultMaxMessageBytes)
	require.Nil(t, err)
	encoder, err := codec.NewTxnEventEncoder(encoderConfig)
	require.Nil(t, err)

	var seqNumbers []uint64
	for i := 0; i < txnCnt; i++ {
		seqNumbers = append(seqNumbers, uint64(i+1))
	}
	rand.New(rand.NewSource(time.Now().UnixNano()))
	rand.Shuffle(len(seqNumbers), func(i, j int) {
		seqNumbers[i], seqNumbers[j] = seqNumbers[j], seqNumbers[i]
	})

	for i := 0; i < txnCnt; i++ {
		go func(seq uint64) {
			frag := EventFragment{
				versionedTable: cloudstorage.VersionedTableName{
					TableNameWithPhysicTableID: common.TableName{
						Schema:  "test",
						Table:   "table1",
						TableID: 100,
					},
				},
				seqNumber: seq,
				event:     &commonEvent.DMLEvent{},
			}
			helper := commonEvent.NewEventTestHelper(t)
			defer helper.Close()

			helper.Tk().MustExec("use test")
			job := helper.DDL2Job("create table table1(c1 int, c2 varchar(255))")
			require.NotNil(t, job)
			helper.ApplyJob(job)
			rand.New(rand.NewSource(time.Now().UnixNano()))
			n := 1 + rand.Intn(1000)
			dmls := make([]string, 0, n)
			for j := 0; j < n; j++ {
				dmls = append(dmls, fmt.Sprintf("insert into table1 values(%d, 'hello world')", j+1))
			}
			frag.event = helper.DML2Event(job.SchemaName, job.TableName, dmls...)

			err := encoder.AppendTxnEvent(frag.event)
			require.NoError(t, err)
			frag.encodedMsgs = encoder.Build()

			for _, msg := range frag.encodedMsgs {
				msg.Key = []byte(strconv.Itoa(int(seq)))
			}
			inputCh <- frag
		}(uint64(i + 1))
	}

	prevSeq := 0
LOOP:
	for {
		select {
		case frag := <-outputCh.Out():
			for _, msg := range frag.encodedMsgs {
				curSeq, err := strconv.Atoi(string(msg.Key))
				require.Nil(t, err)
				require.GreaterOrEqual(t, curSeq, prevSeq)
				prevSeq = curSeq
			}
		case <-time.After(5 * time.Second):
			break LOOP
		}
	}
	cancel()
	require.ErrorIs(t, eg.Wait(), context.Canceled)
}
