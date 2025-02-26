// Copyright 2023 PingCAP, Inc.
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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/pingcap/ticdc/downstreamadapter/worker/producer"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	pulsarConfig "github.com/pingcap/ticdc/pkg/sink/pulsar"
	mm "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/stretchr/testify/require"
)

const (
	// MockPulsarTopic is the mock topic for pulsar
	MockPulsarTopic = "pulsar_test"
)

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
	c, err := pulsarConfig.NewPulsarConfig(sinkURI, replicaConfig.Sink.PulsarConfig)
	require.NoError(t, err)
	return c, sinkURI
}

// TestNewPulsarDDLSink tests the NewPulsarDDLSink
func TestNewPulsarDDLSink(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, schema := range pulsarSchemaList {
		_, sinkURI := newPulsarConfig(t, schema)
		replicaConfig := config.GetDefaultReplicaConfig()
		replicaConfig.Sink = &config.SinkConfig{
			Protocol: aws.String("canal-json"),
		}
		changefeedID := common.NewChangefeedID4Test("test", "test")

		ctx = context.WithValue(ctx, "testing.T", t)
		pulsarComponent, protocol, err := GetPulsarSinkComponentForTest(ctx, changefeedID, sinkURI, replicaConfig.Sink)
		require.NoError(t, err)

		statistics := metrics.NewStatistics(changefeedID, "PulsarSink")
		ddlMockProducer := producer.NewMockDDLProducer()
		ddlSink := NewPulsarDDLWorker(changefeedID, protocol, pulsarComponent.Config, ddlMockProducer,
			pulsarComponent.Encoder, pulsarComponent.EventRouter, pulsarComponent.TopicManager, statistics,
		)
		require.NotNil(t, ddlSink)

		checkpointTs := uint64(417318403368288260)
		tables := []*model.TableInfo{
			{
				TableName: model.TableName{
					Schema: "cdc",
					Table:  "person",
				},
			},
			{
				TableName: model.TableName{
					Schema: "cdc",
					Table:  "person1",
				},
			},
			{
				TableName: model.TableName{
					Schema: "cdc",
					Table:  "person2",
				},
			},
		}

		ddlSink.AddCheckpoint(checkpointTs)

		events := ddlSink.producer.(*producer.MockProducer).GetAllEvents()
		require.Len(t, events, 1, "All topics and partitions should be broadcast")
	}
}

// TestPulsarDDLSinkNewSuccess tests the NewPulsarDDLSink write a event to pulsar
func TestPulsarDDLSinkNewSuccess(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, schema := range pulsarSchemaList {
		_, sinkURI := newPulsarConfig(t, schema)
		changefeedID := common.NewChangefeedID4Test("test", "test")
		replicaConfig := config.GetDefaultReplicaConfig()
		replicaConfig.Sink = &config.SinkConfig{
			Protocol: aws.String("canal-json"),
		}

		ctx = context.WithValue(ctx, "testing.T", t)
		pulsarComponent, protocol, err := GetPulsarSinkComponentForTest(ctx, changefeedID, sinkURI, replicaConfig.Sink)
		require.NoError(t, err)

		statistics := metrics.NewStatistics(changefeedID, "PulsarSink")
		ddlMockProducer := producer.NewMockDDLProducer()
		ddlSink := NewPulsarDDLWorker(changefeedID, protocol, pulsarComponent.Config, ddlMockProducer,
			pulsarComponent.Encoder, pulsarComponent.EventRouter, pulsarComponent.TopicManager, statistics,
		)
		require.NotNil(t, ddlSink)
	}
}

func TestPulsarWriteDDLEventToZeroPartition(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, schema := range pulsarSchemaList {
		_, sinkURI := newPulsarConfig(t, schema)
		changefeedID := common.NewChangefeedID4Test("test", "test")
		replicaConfig := config.GetDefaultReplicaConfig()
		replicaConfig.Sink = &config.SinkConfig{
			Protocol: aws.String("canal-json"),
		}

		ctx = context.WithValue(ctx, "testing.T", t)
		pulsarComponent, protocol, err := GetPulsarSinkComponentForTest(ctx, changefeedID, sinkURI, replicaConfig.Sink)
		require.NoError(t, err)

		statistics := metrics.NewStatistics(changefeedID, "PulsarSink")
		ddlMockProducer := producer.NewMockDDLProducer()
		ddlSink := NewPulsarDDLWorker(changefeedID, protocol, pulsarComponent.Config, ddlMockProducer,
			pulsarComponent.Encoder, pulsarComponent.EventRouter, pulsarComponent.TopicManager, statistics,
		)
		require.NotNil(t, ddlSink)

		ddl := &event.DDLEvent{
			FinishedTs: 417318403368288260,
			SchemaName: "cdc",
			TableName:  "person",
			Query:      "create table person(id int, name varchar(32), primary key(id))",
			Type:       byte(mm.ActionCreateTable),
		}
		err = ddlSink.WriteBlockEvent(ctx, ddl)
		require.NoError(t, err)

		err = ddlSink.WriteBlockEvent(ctx, ddl)
		require.NoError(t, err)

		require.Len(t, ddlSink.producer.(*producer.MockProducer).GetAllEvents(),
			2, "Write DDL 2 Events")
	}
}
