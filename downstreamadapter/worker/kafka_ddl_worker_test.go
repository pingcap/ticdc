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

package worker

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/worker/producer"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

// ddl | checkpoint ts

func kafkaDDLWorkerForTest(t *testing.T) *KafkaDDLWorker {
	ctx := context.Background()
	changefeedID := common.NewChangefeedID4Test("test", "test")
	openProtocol := "open-protocol"
	sinkConfig := &config.SinkConfig{Protocol: &openProtocol}
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, "127.0.0.1:9092", kafka.DefaultMockTopicName)

	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	kafkaComponent, protocol, err := GetKafkaSinkComponentForTest(ctx, changefeedID, sinkURI, sinkConfig)
	require.NoError(t, err)

	statistics := metrics.NewStatistics(changefeedID, "KafkaSink")
	ddlMockProducer := producer.NewMockDDLProducer()
	ddlWorker := NewKafkaDDLWorker(changefeedID, protocol, ddlMockProducer,
		kafkaComponent.Encoder, kafkaComponent.EventRouter, kafkaComponent.TopicManager,
		statistics)
	return ddlWorker
}

func TestWriteDDLEvents(t *testing.T) {
	count = 0

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
			func() { count++ },
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
			func() { count++ },
		},
	}

	ctx := context.Background()
	ddlWorker := kafkaDDLWorkerForTest(t)
	err := ddlWorker.WriteBlockEvent(ctx, ddlEvent)
	require.NoError(t, err)

	err = ddlWorker.WriteBlockEvent(ctx, ddlEvent2)
	require.NoError(t, err)

	// Wait for the events to be received by the worker.
	require.Len(t, ddlWorker.producer.(*producer.MockProducer).GetAllEvents(), 2)
	require.Equal(t, count, 2)
}

func TestWriteCheckpointTs(t *testing.T) {
	ddlWorker := kafkaDDLWorkerForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := ddlWorker.Run(ctx)
		require.True(t, errors.Is(err, context.Canceled))
	}()

	tableSchemaStore := util.NewTableSchemaStore([]*heartbeatpb.SchemaInfo{}, common.KafkaSinkType)
	ddlWorker.SetTableSchemaStore(tableSchemaStore)

	ddlWorker.AddCheckpoint(1)
	ddlWorker.AddCheckpoint(2)

	time.Sleep(1 * time.Second)

	require.Len(t, ddlWorker.producer.(*producer.MockProducer).GetAllEvents(), 2)
	cancel()
}
