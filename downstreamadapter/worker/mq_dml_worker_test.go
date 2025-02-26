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
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

var count int

func kafkaDMLWorkerForTest(t *testing.T) *MQDMLWorker {
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
	dmlMockProducer := producer.NewMockDMLProducer()

	dmlWorker := NewMQDMLWorker(changefeedID, protocol, dmlMockProducer,
		kafkaComponent.EncoderGroup, kafkaComponent.ColumnSelector,
		kafkaComponent.EventRouter, kafkaComponent.TopicManager,
		statistics)
	return dmlWorker
}

func TestWriteEvents(t *testing.T) {
	count = 0

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	dmlEvent := helper.DML2Event("test", "t", "insert into t values (1, 'test')", "insert into t values (2, 'test2');")
	dmlEvent.PostTxnFlushed = []func(){
		func() { count++ },
	}
	dmlEvent.CommitTs = 2

	dmlWorker := kafkaDMLWorkerForTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := dmlWorker.Run(ctx)
		require.True(t, errors.Is(err, context.Canceled))
	}()
	dmlWorker.AddDMLEvent(dmlEvent)

	// Wait for the events to be received by the worker.
	time.Sleep(time.Second)
	require.Len(t, dmlWorker.producer.(*producer.MockProducer).GetAllEvents(), 2)
	require.Equal(t, count, 1)
	cancel()
}
