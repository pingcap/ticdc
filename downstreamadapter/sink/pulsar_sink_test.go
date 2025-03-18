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

package sink

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/pingcap/ticdc/downstreamadapter/worker"
	"github.com/pingcap/ticdc/downstreamadapter/worker/producer"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/stretchr/testify/require"
)

func newPulsarSinkForTest(t *testing.T) (*PulsarSink, producer.DMLProducer, producer.DDLProducer, error) {
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
	pulsarComponent, protocol, err := worker.GetPulsarSinkComponentForTest(ctx, changefeedID, sinkURI, replicaConfig.Sink)
	require.NoError(t, err)

	statistics := metrics.NewStatistics(changefeedID, "PulsarSink")

	dmlMockProducer := producer.NewMockPulsarDMLProducer()
	dmlWorker := worker.NewMQDMLWorker(
		changefeedID,
		protocol,
		dmlMockProducer,
		pulsarComponent.EncoderGroup,
		pulsarComponent.ColumnSelector,
		pulsarComponent.EventRouter,
		pulsarComponent.TopicManager,
		statistics)

	ddlMockProducer := producer.NewMockPulsarDDLProducer()
	ddlWorker := worker.NewMQDDLWorker(
		changefeedID,
		protocol,
		ddlMockProducer,
		pulsarComponent.Encoder,
		pulsarComponent.EventRouter,
		pulsarComponent.TopicManager,
		statistics)

	sink := &PulsarSink{
		changefeedID: changefeedID,
		dmlWorker:    dmlWorker,
		ddlWorker:    ddlWorker,
		topicManager: pulsarComponent.TopicManager,
		statistics:   statistics,
		ctx:          ctx,
	}
	go sink.Run(ctx)
	return sink, dmlMockProducer, ddlMockProducer, nil
}

func TestPulsarSinkBasicFunctionality(t *testing.T) {
	sink, dmlProducer, ddlProducer, err := newPulsarSinkForTest(t)
	require.NoError(t, err)

	count.Store(0)

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

	err = sink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	err = sink.AddDMLEvent(dmlEvent)
	require.NoError(t, err)
	time.Sleep(1 * time.Second)

	sink.PassBlockEvent(ddlEvent2)

	require.Len(t, dmlProducer.(*producer.PulsarMockProducer).GetAllEvents(), 2)
	require.Len(t, ddlProducer.(*producer.PulsarMockProducer).GetAllEvents(), 1)

	require.Equal(t, count.Load(), int64(3))
}
