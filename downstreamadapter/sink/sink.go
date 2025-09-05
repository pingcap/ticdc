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
	pkgConfig "github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/util"
)

type Sink interface {
	SinkType() common.SinkType
	IsNormal() bool

	AddDMLEvent(event *commonEvent.DMLEvent)
	WriteBlockEvent(event commonEvent.BlockEvent) error
	AddCheckpointTs(ts uint64)

	SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore)
	Close(removeChangefeed bool)
	Run(ctx context.Context) error
}

func New(ctx context.Context, config *config.ChangefeedConfig, changefeedID common.ChangeFeedID) (Sink, error) {
	sinkURI, err := url.Parse(config.SinkURI)
	if err != nil {
		return nil, errors.WrapError(errors.ErrSinkURIInvalid, err)
	}
	scheme := pkgConfig.GetScheme(sinkURI)
	switch scheme {
	case pkgConfig.MySQLScheme, pkgConfig.MySQLSSLScheme, pkgConfig.TiDBScheme, pkgConfig.TiDBSSLScheme:
		return mysql.New(ctx, changefeedID, config, sinkURI)
	case pkgConfig.KafkaScheme, pkgConfig.KafkaSSLScheme:
		return kafka.New(ctx, changefeedID, sinkURI, config.SinkConfig)
	case pkgConfig.PulsarScheme, pkgConfig.PulsarSSLScheme, pkgConfig.PulsarHTTPScheme, pkgConfig.PulsarHTTPSScheme:
		return pulsar.New(ctx, changefeedID, sinkURI, config.SinkConfig)
	case pkgConfig.S3Scheme, pkgConfig.FileScheme, pkgConfig.GCSScheme, pkgConfig.GSScheme, pkgConfig.AzblobScheme, pkgConfig.AzureScheme, pkgConfig.CloudStorageNoopScheme:
		return cloudstorage.New(ctx, changefeedID, sinkURI, config.SinkConfig, nil)
	case pkgConfig.BlackHoleScheme:
		return blackhole.New()
	}
	return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs(sinkURI)
}

func Verify(ctx context.Context, config *config.ChangefeedConfig, changefeedID common.ChangeFeedID) error {
	sinkURI, err := url.Parse(config.SinkURI)
	if err != nil {
		return errors.WrapError(errors.ErrSinkURIInvalid, err)
	}
	scheme := pkgConfig.GetScheme(sinkURI)
	switch scheme {
	case pkgConfig.MySQLScheme, pkgConfig.MySQLSSLScheme, pkgConfig.TiDBScheme, pkgConfig.TiDBSSLScheme:
		return mysql.Verify(ctx, sinkURI, config)
	case pkgConfig.KafkaScheme, pkgConfig.KafkaSSLScheme:
		return kafka.Verify(ctx, changefeedID, sinkURI, config.SinkConfig)
	case pkgConfig.PulsarScheme, pkgConfig.PulsarSSLScheme, pkgConfig.PulsarHTTPScheme, pkgConfig.PulsarHTTPSScheme:
		return pulsar.Verify(ctx, changefeedID, sinkURI, config.SinkConfig)
	case pkgConfig.S3Scheme, pkgConfig.FileScheme, pkgConfig.GCSScheme, pkgConfig.GSScheme, pkgConfig.AzblobScheme, pkgConfig.AzureScheme, pkgConfig.CloudStorageNoopScheme:
		return cloudstorage.Verify(ctx, changefeedID, sinkURI, config.SinkConfig)
	case pkgConfig.BlackHoleScheme:
		return nil
	}
	return errors.ErrSinkURIInvalid.GenWithStackByArgs(sinkURI)
}
