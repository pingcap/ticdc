// Copyright 2025 PingCAP, Inc.
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

package pulsar

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func newPulsarSinkForTest(t *testing.T) (*sink, error) {
	sinkURL := "pulsar://127.0.0.1:6650/persistent://public/default/test?" +
		"protocol=canal-json&pulsar-version=v2.10.0&enable-tidb-extension=true&" +
		"authentication-token=eyJhbcGcixxxxxxxxxxxxxx"
	sinkURI, err := url.Parse(sinkURL)
	require.NoError(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink = &config.SinkConfig{
		Protocol: aws.String("canal-json"),
	}

	ctx := context.Background()
	changefeedID := common.NewChangefeedID4Test("test", "test")
	comp, protocol, err := newPulsarSinkComponentForTest(ctx, changefeedID, sinkURI, replicaConfig.Sink)
	require.NoError(t, err)

	statistics := metrics.NewStatistics(changefeedID, "sink")
	pulsarSink := &sink{
		changefeedID: changefeedID,
		dmlProducer:  newMockDMLProducer(),
		ddlProducer:  newMockDDLProducer(),

		checkpointTsChan: make(chan uint64, 16),
		eventChan:        make(chan *commonEvent.DMLEvent, 32),
		rowChan:          make(chan *commonEvent.MQRowEvent, 32),

		protocol:      protocol,
		partitionRule: helper.GetDDLDispatchRule(protocol),
		comp:          comp,

		isNormal:   atomic.NewBool(true),
		statistics: statistics,
		ctx:        ctx,
	}
	go pulsarSink.Run(ctx)
	return pulsarSink, nil
}

func TestPulsarSinkBasicFunctionality(t *testing.T) {
	pulsarSink, err := newPulsarSinkForTest(t)
	require.NoError(t, err)

	var count atomic.Int64

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	ddlEvent := &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 1,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count.Add(1) },
		},
	}

	ddlEvent2 := &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		FinishedTs: 4,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count.Add(1) },
		},
	}

	dmlEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')", "insert into t values (2, 'test2');")
	dmlEvent.PostTxnFlushed = []func(){
		func() { count.Add(1) },
	}
	dmlEvent.CommitTs = 2

	err = pulsarSink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	pulsarSink.AddDMLEvent(dmlEvent)
	time.Sleep(1 * time.Second)

	ddlEvent2.PostFlush()

	require.Len(t, pulsarSink.dmlProducer.(*mockProducer).GetAllEvents(), 2)
	require.Len(t, pulsarSink.ddlProducer.(*mockProducer).GetAllEvents(), 1)

	require.Equal(t, count.Load(), int64(3))
}
