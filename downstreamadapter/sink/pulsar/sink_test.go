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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/statistics"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/prometheus/client_golang/prometheus"
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

	statistics := statistics.New(changefeedID, common.DefaultKeyspaceID)
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
	registry := prometheus.NewRegistry()
	statistics.InitMetrics(registry)

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
	producer := pulsarSink.dmlProducer.(*mockProducer)
	producer.callbackCh = make(chan func(), 2)

	err = pulsarSink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	metricLabels := prometheus.Labels{"changefeed": pulsarSink.changefeedID.Name()}
	beforeWriteBytes := counterValueForLabels(t, registry, "ticdc_sink_write_bytes_total", metricLabels)
	pulsarSink.AddDMLEvent(dmlEvent)
	callbacks := make([]func(), 0, 2)
	for range 2 {
		select {
		case callback := <-producer.callbackCh:
			callbacks = append(callbacks, callback)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for Pulsar messages")
		}
	}
	require.Equal(t, beforeWriteBytes, counterValueForLabels(t, registry, "ticdc_sink_write_bytes_total", metricLabels))
	for _, callback := range callbacks {
		require.NotNil(t, callback)
		callback()
	}

	ddlEvent2.PostFlush()

	require.Len(t, producer.GetAllEvents(), 2)
	require.Len(t, pulsarSink.ddlProducer.(*mockProducer).GetAllEvents(), 1)

	require.Equal(t, count.Load(), int64(3))
	require.Equal(t, float64(dmlEvent.GetSize()), counterValueForLabels(t, registry, "ticdc_sink_write_bytes_total", metricLabels)-beforeWriteBytes)
}

func counterValueForLabels(
	t *testing.T,
	registry *prometheus.Registry,
	metricName string,
	labels prometheus.Labels,
) float64 {
	t.Helper()

	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	for _, metricFamily := range metricFamilies {
		if metricFamily.GetName() != metricName {
			continue
		}
		for _, metric := range metricFamily.Metric {
			matchedLabels := 0
			for _, label := range metric.Label {
				if value, ok := labels[label.GetName()]; ok {
					if value != label.GetValue() {
						break
					}
					matchedLabels++
				}
			}
			if matchedLabels == len(labels) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestPulsarSinkBatchConfig(t *testing.T) {
	sink := &sink{}
	require.Equal(t, 4096, sink.BatchCount())
	require.Zero(t, sink.BatchBytes())
}

func TestPulsarSinkNewWithComponentReturnsDMLProducerError(t *testing.T) {
	changefeedID := common.NewChangefeedID4Test("test", "test")
	expectedErr := cerror.ErrPulsarNewProducer.GenWithStackByArgs()
	ddlProducerCreated := false
	var err error

	require.NotPanics(t, func() {
		_, err = newWithComponent(
			context.Background(),
			changefeedID,
			common.DefaultKeyspaceID,
			&config.SinkConfig{},
			component{},
			config.ProtocolCanalJSON,
			func(common.ChangeFeedID, component, chan error) (dmlProducer, error) {
				var producer *dmlProducers
				return producer, expectedErr
			},
			func(common.ChangeFeedID, component, *config.SinkConfig) (ddlProducer, error) {
				ddlProducerCreated = true
				return newMockDDLProducer(), nil
			},
		)
	})

	require.Error(t, err)
	require.EqualError(t, err, expectedErr.Error())
	require.False(t, ddlProducerCreated)
}

func TestPulsarSinkNewWithComponentReturnsDDLProducerError(t *testing.T) {
	changefeedID := common.NewChangefeedID4Test("test", "test")
	expectedErr := cerror.ErrPulsarNewProducer.GenWithStackByArgs()
	var err error

	require.NotPanics(t, func() {
		_, err = newWithComponent(
			context.Background(),
			changefeedID,
			common.DefaultKeyspaceID,
			&config.SinkConfig{},
			component{},
			config.ProtocolCanalJSON,
			func(common.ChangeFeedID, component, chan error) (dmlProducer, error) {
				return newMockDMLProducer(), nil
			},
			func(common.ChangeFeedID, component, *config.SinkConfig) (ddlProducer, error) {
				var producer *ddlProducers
				return producer, expectedErr
			},
		)
	})

	require.Error(t, err)
	require.EqualError(t, err, expectedErr.Error())
}
