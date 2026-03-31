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
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/chann"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type staticTopicManager struct {
	partitionNum int32
}

func (m staticTopicManager) GetPartitionNum(context.Context, string) (int32, error) {
	return m.partitionNum, nil
}

func (m staticTopicManager) CreateTopicAndWaitUntilVisible(context.Context, string) (int32, error) {
	return m.partitionNum, nil
}

func (m staticTopicManager) Close() {}

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
		eventChan:        chann.NewUnlimitedChannelDefault[*commonEvent.DMLEvent](),
		rowChan:          chann.NewUnlimitedChannelDefault[*commonEvent.MQRowEvent](),

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

func TestCalculateKeyPartitionsUsesSourceIdentityForColumnSelector(t *testing.T) {
	t.Parallel()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("create database source_db")
	helper.DDL2Job("create table source_db.orders (id int primary key, name varchar(32))")

	dmlEvent := helper.DML2Event("source_db", "orders", "insert into source_db.orders values (1, 'alice')")
	dmlEvent.TableInfo = dmlEvent.TableInfo.CloneWithRouting("target_db", "orders_routed")

	sinkConfig := &config.SinkConfig{
		DispatchRules: []*config.DispatchRule{
			{
				Matcher:       []string{"source_db.orders"},
				PartitionRule: "table",
				TopicRule:     "source_topic",
			},
			{
				Matcher:       []string{"target_db.orders_routed"},
				PartitionRule: "ts",
				TopicRule:     "target_topic",
			},
		},
		ColumnSelectors: []*config.ColumnSelector{
			{
				Matcher: []string{"source_db.orders"},
				Columns: []string{"id"},
			},
			{
				Matcher: []string{"target_db.orders_routed"},
				Columns: []string{"id", "name"},
			},
		},
	}

	router, err := eventrouter.NewEventRouter(sinkConfig, "default_topic", true, false)
	require.NoError(t, err)
	selectors, err := columnselector.New(sinkConfig)
	require.NoError(t, err)

	s := &sink{
		changefeedID: common.NewChangefeedID4Test("test", "test"),
		comp: component{
			eventRouter:    router,
			columnSelector: selectors,
			topicManager:   staticTopicManager{partitionNum: 4},
		},
		eventChan: chann.NewUnlimitedChannelDefault[*commonEvent.DMLEvent](),
		rowChan:   chann.NewUnlimitedChannelDefault[*commonEvent.MQRowEvent](),
	}

	s.eventChan.Push(dmlEvent)
	s.eventChan.Close()

	require.NoError(t, s.calculateKeyPartitions(context.Background()))

	mqEvent, ok := s.rowChan.Get()
	require.True(t, ok)
	require.Equal(t, "source_topic", mqEvent.Key.Topic)

	idCol := mustGetColumnByName(t, dmlEvent.TableInfo, "id")
	nameCol := mustGetColumnByName(t, dmlEvent.TableInfo, "name")
	require.True(t, mqEvent.RowEvent.ColumnSelector.Select(idCol))
	require.False(t, mqEvent.RowEvent.ColumnSelector.Select(nameCol))
}

func mustGetColumnByName(t *testing.T, tableInfo *common.TableInfo, name string) *timodel.ColumnInfo {
	t.Helper()

	for _, col := range tableInfo.GetColumns() {
		if col != nil && col.Name.O == name {
			return col
		}
	}

	t.Fatalf("column %s not found", name)
	return nil
}
