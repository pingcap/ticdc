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

	"github.com/pingcap/ticdc/downstreamadapter/sink/blackhole"
	"github.com/pingcap/ticdc/downstreamadapter/sink/cloudstorage"
	"github.com/pingcap/ticdc/downstreamadapter/sink/kafka"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mysql"
	"github.com/pingcap/ticdc/downstreamadapter/sink/pulsar"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
)

type Sink interface {
	SinkType() common.SinkType
	IsNormal() bool

	AddDMLEvent(event *commonEvent.DMLEvent)
	WriteBlockEvent(event commonEvent.BlockEvent) error
	AddCheckpointTs(ts uint64)

	SetTableSchemaStore(tableSchemaStore *commonEvent.TableSchemaStore)
	Close(removeChangefeed bool)
	Run(ctx context.Context) error
}

func New(ctx context.Context, cfg *config.ChangefeedConfig, changefeedID common.ChangeFeedID) (Sink, error) {
	sinkURI, err := url.Parse(cfg.SinkURI)
	if err != nil {
		return nil, errors.WrapError(errors.ErrSinkURIInvalid, err)
	}
	scheme := config.GetScheme(sinkURI)
	switch scheme {
	case config.MySQLScheme, config.MySQLSSLScheme, config.TiDBScheme, config.TiDBSSLScheme:
		return mysql.New(ctx, changefeedID, cfg, sinkURI)
	case config.KafkaScheme, config.KafkaSSLScheme:
		return kafka.New(ctx, changefeedID, sinkURI, cfg.SinkConfig)
	case config.PulsarScheme, config.PulsarSSLScheme, config.PulsarHTTPScheme, config.PulsarHTTPSScheme:
		return pulsar.New(ctx, changefeedID, sinkURI, cfg.SinkConfig)
	case config.S3Scheme, config.FileScheme, config.GCSScheme, config.GSScheme, config.AzblobScheme, config.AzureScheme, config.CloudStorageNoopScheme:
		return cloudstorage.New(ctx, changefeedID, sinkURI, cfg.SinkConfig, cfg.EnableTableAcrossNodes, nil)
	case config.BlackHoleScheme:
		return blackhole.New()
	}
	return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs(sinkURI)
}

func Verify(ctx context.Context, cfg *config.ChangefeedConfig, changefeedID common.ChangeFeedID) error {
	sinkURI, err := url.Parse(cfg.SinkURI)
	if err != nil {
		return errors.WrapError(errors.ErrSinkURIInvalid, err)
	}
	scheme := config.GetScheme(sinkURI)
	switch scheme {
	case config.MySQLScheme, config.MySQLSSLScheme, config.TiDBScheme, config.TiDBSSLScheme:
		return mysql.Verify(ctx, sinkURI, cfg)
	case config.KafkaScheme, config.KafkaSSLScheme:
		return kafka.Verify(ctx, changefeedID, sinkURI, cfg.SinkConfig)
	case config.PulsarScheme, config.PulsarSSLScheme, config.PulsarHTTPScheme, config.PulsarHTTPSScheme:
		return pulsar.Verify(ctx, changefeedID, sinkURI, cfg.SinkConfig)
	case config.S3Scheme, config.FileScheme, config.GCSScheme, config.GSScheme, config.AzblobScheme, config.AzureScheme, config.CloudStorageNoopScheme:
		return cloudstorage.Verify(ctx, changefeedID, sinkURI, cfg.SinkConfig, cfg.EnableTableAcrossNodes)
	case config.BlackHoleScheme:
		return nil
	}
	return errors.ErrSinkURIInvalid.GenWithStackByArgs(sinkURI)
}
