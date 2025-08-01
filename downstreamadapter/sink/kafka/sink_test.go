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

package kafka

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func newKafkaSinkForTest() (*sink, error) {
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
		return nil, errors.Trace(err)
	}
	statistics := metrics.NewStatistics(changefeedID, "sink")
	comp, protocol, err := newKafkaSinkComponentForTest(ctx, changefeedID, sinkURI, sinkConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// We must close adminClient when this func return cause by an error
	// otherwise the adminClient will never be closed and lead to a goroutine leak.
	defer func() {
		if err != nil && comp.adminClient != nil {
			comp.close()
		}
	}()

	asyncProducer, err := comp.factory.AsyncProducer()
	if err != nil {
		return nil, err
	}

	syncProducer, err := comp.factory.SyncProducer()
	if err != nil {
		return nil, err
	}

	s := &sink{
		changefeedID:     changefeedID,
		dmlProducer:      asyncProducer,
		ddlProducer:      syncProducer,
		metricsCollector: comp.factory.MetricsCollector(comp.adminClient),

		partitionRule: helper.GetDDLDispatchRule(protocol),
		protocol:      protocol,
		comp:          comp,
		statistics:    statistics,

		checkpointChan: make(chan uint64, 16),
		eventChan:      chann.NewUnlimitedChannelDefault[*commonEvent.DMLEvent](),
		rowChan:        chann.NewUnlimitedChannelDefault[*commonEvent.MQRowEvent](),

		isNormal: atomic.NewBool(true),
		ctx:      ctx,
	}
	go s.Run(ctx)
	return s, nil
}

func TestKafkaSinkBasicFunctionality(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	var count atomic.Int64
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

	dmlEvent := helper.DML2Event("test", "t",
		"insert into t values (1, 'test')",
		"insert into t values (2, 'test2');")
	dmlEvent.PostTxnFlushed = []func(){
		func() { count.Add(1) },
	}
	dmlEvent.CommitTs = 2

	kafkaSink, err := newKafkaSinkForTest()
	require.NoError(t, err)

	kafkaSink.ddlProducer.(*kafka.MockSaramaSyncProducer).SyncProducer.ExpectSendMessageAndSucceed()
	err = kafkaSink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	kafkaSink.dmlProducer.(*kafka.MockSaramaAsyncProducer).AsyncProducer.ExpectInputAndSucceed()
	kafkaSink.dmlProducer.(*kafka.MockSaramaAsyncProducer).AsyncProducer.ExpectInputAndSucceed()
	kafkaSink.AddDMLEvent(dmlEvent)

	ddlEvent2.PostFlush()

	require.Eventually(t,
		func() bool {
			return count.Load() == int64(3)
		}, 5*time.Second, time.Second)
}
