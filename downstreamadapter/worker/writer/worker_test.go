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
	"net/url"
	"sync"
	"testing"

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

func testWorker(
	t *testing.T,
) (*Worker, chan EventFragment, chan EventFragment) {
	uri := fmt.Sprintf("file:///%s", t.TempDir())
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	changefeedID := common.NewChangefeedID4Test("test", "table1")
	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, config.ProtocolCsv,
		replicaConfig.Sink, config.DefaultMaxMessageBytes)
	require.Nil(t, err)
	encoder, err := codec.NewTxnEventEncoder(encoderConfig)
	require.Nil(t, err)

	encodedCh := make(chan EventFragment)
	msgCh := make(chan EventFragment, 1024)
	return NewWorker(1, changefeedID, encoder, msgCh, encodedCh), msgCh, encodedCh
}

func TestEncodeEvents(t *testing.T) {
	encodingWorker, _, encodedCh := testWorker(t)
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	outputChs := []*chann.DrainableChann[EventFragment]{chann.NewAutoDrainChann[EventFragment]()}
	defragmenter := NewDefragmenter(encodedCh, outputChs)
	eg.Go(func() error {
		return defragmenter.Run(egCtx)
	})

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table table1(c1 int, c2 varchar(255))")
	require.NotNil(t, job)
	helper.ApplyJob(job)
	dmlEvent := helper.DML2Event(job.SchemaName, job.TableName, "insert into table1 values(100, 'hello world')", "insert into table1 values(200, '你好，世界')")

	err := encodingWorker.encodeEvents(EventFragment{
		versionedTable: cloudstorage.VersionedTableName{
			TableNameWithPhysicTableID: common.TableName{
				Schema:  "test",
				Table:   "table1",
				TableID: 100,
			},
		},
		seqNumber: 1,
		event:     dmlEvent,
	})
	require.Nil(t, err)
	cancel()
	require.ErrorIs(t, eg.Wait(), context.Canceled)
}

func TestEncodingWorkerRun(t *testing.T) {
	encodingWorker, msgCh, encodedCh := testWorker(t)
	ctx, cancel := context.WithCancel(context.Background())
	eg, egCtx := errgroup.WithContext(ctx)
	outputChs := []*chann.DrainableChann[EventFragment]{chann.NewAutoDrainChann[EventFragment]()}
	defragmenter := NewDefragmenter(encodedCh, outputChs)
	eg.Go(func() error {
		return defragmenter.Run(egCtx)
	})

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	job := helper.DDL2Job("create table table1(c1 int, c2 varchar(255))")
	require.NotNil(t, job)
	helper.ApplyJob(job)
	dmlEvent := helper.DML2Event(job.SchemaName, job.TableName, "insert into table1 values(100, 'hello world')")

	for i := 0; i < 3; i++ {
		frag := EventFragment{
			versionedTable: cloudstorage.VersionedTableName{
				TableNameWithPhysicTableID: common.TableName{
					Schema:  "test",
					Table:   "table1",
					TableID: 100,
				},
			},
			seqNumber: uint64(i + 1),
			event:     dmlEvent,
		}
		msgCh <- frag
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = encodingWorker.Run(ctx)
	}()

	cancel()
	encodingWorker.Close()
	wg.Wait()
}
