// Copyright 2024 PingCAP, Inc.
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

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	sinkutil "github.com/pingcap/ticdc/pkg/sink/util"
	"github.com/pingcap/tiflow/pkg/sink"
)

type Sink interface {
	SinkType() common.SinkType
	IsNormal() bool

	AddDMLEvent(event *commonEvent.DMLEvent) error
	WriteBlockEvent(event commonEvent.BlockEvent) error
	PassBlockEvent(event commonEvent.BlockEvent)
	AddCheckpointTs(ts uint64)

	SetTableSchemaStore(tableSchemaStore *sinkutil.TableSchemaStore)
	Close(removeChangefeed bool)
	Run(ctx context.Context) error
}

func NewSink(ctx context.Context, config *config.ChangefeedConfig, changefeedID common.ChangeFeedID) (Sink, error) {
	sinkURI, err := url.Parse(config.SinkURI)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	scheme := sink.GetScheme(sinkURI)
	switch scheme {
	case sink.MySQLScheme, sink.MySQLSSLScheme, sink.TiDBScheme, sink.TiDBSSLScheme:
		return newMySQLSink(ctx, changefeedID, config, sinkURI)
	case sink.KafkaScheme, sink.KafkaSSLScheme:
		return newKafkaSink(ctx, changefeedID, sinkURI, config.SinkConfig)
	case sink.PulsarScheme, sink.PulsarSSLScheme, sink.PulsarHTTPScheme, sink.PulsarHTTPSScheme:
		return newPulsarSink(ctx, changefeedID, sinkURI, config.SinkConfig)
	case sink.S3Scheme, sink.FileScheme, sink.GCSScheme, sink.GSScheme, sink.AzblobScheme, sink.AzureScheme, sink.CloudStorageNoopScheme:
		return newCloudStorageSink(ctx, changefeedID, sinkURI, config.SinkConfig, nil)
	case sink.BlackHoleScheme:
		return newBlackHoleSink()
	}
	return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs(sinkURI)
}

func VerifySink(ctx context.Context, config *config.ChangefeedConfig, changefeedID common.ChangeFeedID) error {
	sinkURI, err := url.Parse(config.SinkURI)
	if err != nil {
		return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	scheme := sink.GetScheme(sinkURI)
	switch scheme {
	case sink.MySQLScheme, sink.MySQLSSLScheme, sink.TiDBScheme, sink.TiDBSSLScheme:
		return verifyMySQLSink(ctx, sinkURI, config)
	case sink.KafkaScheme, sink.KafkaSSLScheme:
		return verifyKafkaSink(ctx, changefeedID, sinkURI, config.SinkConfig)
	case sink.PulsarScheme, sink.PulsarSSLScheme, sink.PulsarHTTPScheme, sink.PulsarHTTPSScheme:
		return verifyPulsarSink(ctx, changefeedID, sinkURI, config.SinkConfig)
	case sink.S3Scheme, sink.FileScheme, sink.GCSScheme, sink.GSScheme, sink.AzblobScheme, sink.AzureScheme, sink.CloudStorageNoopScheme:
		return verifyCloudStorageSink(ctx, changefeedID, sinkURI, config.SinkConfig)
	case sink.BlackHoleScheme:
		return nil
	}
	return cerror.ErrSinkURIInvalid.GenWithStackByArgs(sinkURI)
}
