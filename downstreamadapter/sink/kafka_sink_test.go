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
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/downstreamadapter/worker"
	"github.com/pingcap/ticdc/downstreamadapter/worker/producer"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

func newKafkaSinkForTest() (*KafkaSink, producer.DMLProducer, producer.DDLProducer, error) {
	ctx := context.Background()
	changefeedID := common.NewChangefeedID4Test("test", "test")
	openProtocol := "open-protocol"
	sinkConfig := &config.SinkConfig{Protocol: &openProtocol}
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, "127.0.0.1:9092", kafka.DefaultMockTopicName)

	sinkURI, err := url.Parse(uri)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	statistics := metrics.NewStatistics(changefeedID, "KafkaSink")
	kafkaComponent, protocol, err := worker.GetKafkaSinkComponentForTest(ctx, changefeedID, sinkURI, sinkConfig)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	// We must close adminClient when this func return cause by an error
	// otherwise the adminClient will never be closed and lead to a goroutine leak.
	defer func() {
		if err != nil && kafkaComponent.AdminClient != nil {
			kafkaComponent.AdminClient.Close()
		}
	}()

	dmlMockProducer := producer.NewMockKafkaDMLProducer()

	dmlWorker := worker.NewMQDMLWorker(
		changefeedID,
		protocol,
		dmlMockProducer,
		kafkaComponent.EncoderGroup,
		kafkaComponent.ColumnSelector,
		kafkaComponent.EventRouter,
		kafkaComponent.TopicManager,
		statistics)

	ddlMockProducer := producer.NewMockKafkaDDLProducer()
	ddlWorker := worker.NewMQDDLWorker(
		changefeedID,
		protocol,
		ddlMockProducer,
		kafkaComponent.Encoder,
		kafkaComponent.EventRouter,
		kafkaComponent.TopicManager,
		statistics)

	sink := &KafkaSink{
		changefeedID:     changefeedID,
		dmlWorker:        dmlWorker,
		ddlWorker:        ddlWorker,
		adminClient:      kafkaComponent.AdminClient,
		topicManager:     kafkaComponent.TopicManager,
		statistics:       statistics,
		metricsCollector: kafkaComponent.Factory.MetricsCollector(kafkaComponent.AdminClient),
	}
	go sink.Run(ctx)
	return sink, dmlMockProducer, ddlMockProducer, nil
}

func TestKafkaSinkBasicFunctionality(t *testing.T) {
	sink, dmlProducer, ddlProducer, err := newKafkaSinkForTest()
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

	err = sink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	sink.AddDMLEvent(dmlEvent)

	time.Sleep(1 * time.Second)

	sink.PassBlockEvent(ddlEvent2)

	require.Len(t, dmlProducer.(*producer.KafkaMockProducer).GetAllEvents(), 2)
	require.Len(t, ddlProducer.(*producer.KafkaMockProducer).GetAllEvents(), 1)

	require.Equal(t, count.Load(), int64(3))
}
