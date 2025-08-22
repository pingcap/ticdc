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
	"github.com/pingcap/ticdc/downstreamadapter/sink/txnsink"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	mysqlpkg "github.com/pingcap/ticdc/pkg/sink/mysql"
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

func New(ctx context.Context, cfg *config.ChangefeedConfig, changefeedID common.ChangeFeedID) (Sink, error) {
	sinkURI, err := url.Parse(cfg.SinkURI)
	if err != nil {
		return nil, errors.WrapError(errors.ErrSinkURIInvalid, err)
	}
	scheme := config.GetScheme(sinkURI)
	switch scheme {
	case config.MySQLScheme, config.MySQLSSLScheme, config.TiDBScheme, config.TiDBSSLScheme:
		return newTxnSinkAdapter(ctx, changefeedID, cfg, sinkURI)
	case config.KafkaScheme, config.KafkaSSLScheme:
		return kafka.New(ctx, changefeedID, sinkURI, cfg.SinkConfig)
	case config.PulsarScheme, config.PulsarSSLScheme, config.PulsarHTTPScheme, config.PulsarHTTPSScheme:
		return pulsar.New(ctx, changefeedID, sinkURI, cfg.SinkConfig)
	case config.S3Scheme, config.FileScheme, config.GCSScheme, config.GSScheme, config.AzblobScheme, config.AzureScheme, config.CloudStorageNoopScheme:
		return cloudstorage.New(ctx, changefeedID, sinkURI, cfg.SinkConfig, nil)
	case config.BlackHoleScheme:
		return blackhole.New()
	}
	return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs(sinkURI)
}

// newTxnSinkAdapter creates a txnSink adapter that uses the same database connection as mysqlSink
func newTxnSinkAdapter(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	config *config.ChangefeedConfig,
	sinkURI *url.URL,
) (Sink, error) {
	// Use the same database connection logic as mysqlSink
	_, db, err := mysqlpkg.NewMysqlConfigAndDB(ctx, changefeedID, sinkURI, config)
	if err != nil {
		return nil, err
	}

	// Create txnSink configuration
	txnConfig := &txnsink.TxnSinkConfig{
		MaxConcurrentTxns: 16,
		BatchSize:         16,
		FlushInterval:     100,
		MaxSQLBatchSize:   1024 * 16, // 1MB
	}

	// Create and return txnSink
	return txnsink.New(ctx, changefeedID, db, txnConfig), nil
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
		return cloudstorage.Verify(ctx, changefeedID, sinkURI, cfg.SinkConfig)
	case config.BlackHoleScheme:
		return nil
	}
	return errors.ErrSinkURIInvalid.GenWithStackByArgs(sinkURI)
}
