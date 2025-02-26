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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/pingcap/ticdc/downstreamadapter/worker/producer"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/pkg/sink/pulsar"
	"github.com/pingcap/ticdc/pkg/sink/util"
	mm "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/stretchr/testify/require"
)

// ddl | checkpoint ts

func kafkaDDLWorkerForTest(t *testing.T) *MQDDLWorker {
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
	ddlWorker := NewMQDDLWorker(changefeedID, protocol, ddlMockProducer,
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

// pulsar
var pulsarSchemaList = []string{sink.PulsarScheme, sink.PulsarSSLScheme, sink.PulsarHTTPScheme, sink.PulsarHTTPSScheme}

// newPulsarConfig set config
func newPulsarConfig(t *testing.T, schema string) (*config.PulsarConfig, *url.URL) {
	sinkURL := fmt.Sprintf("%s://127.0.0.1:6650/persistent://public/default/test?", schema) +
		"protocol=canal-json&pulsar-version=v2.10.0&enable-tidb-extension=true&" +
		"authentication-token=eyJhbcGcixxxxxxxxxxxxxx"
	sinkURI, err := url.Parse(sinkURL)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	require.NoError(t, replicaConfig.ValidateAndAdjust(sinkURI))
	c, err := pulsar.NewPulsarConfig(sinkURI, replicaConfig.Sink.PulsarConfig)
	require.NoError(t, err)
	return c, sinkURI
}

func pulsarDDLWorkerForTest(t *testing.T, schema string) *MQDDLWorker {
	_, sinkURI := newPulsarConfig(t, schema)
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink = &config.SinkConfig{
		Protocol: aws.String("canal-json"),
	}

	ctx := context.Background()
	changefeedID := common.NewChangefeedID4Test("test", "test")
	kafkaComponent, protocol, err := GetPulsarSinkComponentForTest(ctx, changefeedID, sinkURI, replicaConfig.Sink)
	require.NoError(t, err)

	statistics := metrics.NewStatistics(changefeedID, "KafkaSink")
	ddlMockProducer := producer.NewMockDDLProducer()
	ddlWorker := NewMQDDLWorker(changefeedID, protocol, ddlMockProducer,
		kafkaComponent.Encoder, kafkaComponent.EventRouter, kafkaComponent.TopicManager,
		statistics)
	return ddlWorker
}

// TestNewPulsarDDLSink tests the NewPulsarDDLSink
func TestNewPulsarDDLSink(t *testing.T) {
	t.Parallel()
	for _, schema := range pulsarSchemaList {
		ddlSink := pulsarDDLWorkerForTest(t, schema)

		checkpointTs := uint64(417318403368288260)
		// tables := []*model.TableInfo{
		// 	{
		// 		TableName: model.TableName{
		// 			Schema: "cdc",
		// 			Table:  "person",
		// 		},
		// 	},
		// 	{
		// 		TableName: model.TableName{
		// 			Schema: "cdc",
		// 			Table:  "person1",
		// 		},
		// 	},
		// 	{
		// 		TableName: model.TableName{
		// 			Schema: "cdc",
		// 			Table:  "person2",
		// 		},
		// 	},
		// }

		ddlSink.AddCheckpoint(checkpointTs)

		events := ddlSink.producer.(*producer.MockProducer).GetAllEvents()
		require.Len(t, events, 1, "All topics and partitions should be broadcast")
	}
}

// TestPulsarDDLSinkNewSuccess tests the NewPulsarDDLSink write a event to pulsar
func TestPulsarDDLSinkNewSuccess(t *testing.T) {
	t.Parallel()
	for _, schema := range pulsarSchemaList {
		ddlSink := pulsarDDLWorkerForTest(t, schema)
		require.NotNil(t, ddlSink)
	}
}

func TestPulsarWriteDDLEventToZeroPartition(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, schema := range pulsarSchemaList {
		ddlSink := pulsarDDLWorkerForTest(t, schema)
		require.NotNil(t, ddlSink)

		ddl := &event.DDLEvent{
			FinishedTs: 417318403368288260,
			SchemaName: "cdc",
			TableName:  "person",
			Query:      "create table person(id int, name varchar(32), primary key(id))",
			Type:       byte(mm.ActionCreateTable),
		}
		err := ddlSink.WriteBlockEvent(ctx, ddl)
		require.NoError(t, err)

		err = ddlSink.WriteBlockEvent(ctx, ddl)
		require.NoError(t, err)

		require.Len(t, ddlSink.producer.(*producer.MockProducer).GetAllEvents(),
			2, "Write DDL 2 Events")
	}
}
