// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type franzFactory struct {
	changefeedID common.ChangeFeedID
	clientOpts   []kgo.Opt
	producerOpts []kgo.Opt
	timeout      time.Duration
}

func newFranzFactory(ctx context.Context, o *options, changefeedID common.ChangeFeedID) (Factory, error) {
	clientOpts, err := clientOptions(o)
	if err != nil {
		return nil, err
	}
	timeout := requestTimeout(o)
	admin, err := newAdmin(ctx, changefeedID, clientOpts, timeout)
	if err != nil {
		return nil, err
	}
	defer admin.Close()

	if err := adjustOptions(ctx, changefeedID, admin, o, o.Topic); err != nil {
		return nil, err
	}
	producerOpts, err := producerOptions(o)
	if err != nil {
		return nil, err
	}

	compression := strings.ToLower(strings.TrimSpace(o.Compression))
	if compression == "" {
		compression = "none"
	}

	log.Info("kafka sink configuration resolved",
		zap.String("namespace", changefeedID.Keyspace()),
		zap.String("changefeed", changefeedID.Name()),
		zap.String("client", KafkaClientFranz),
		zap.String("topic", o.Topic),
		zap.Int32("partitionNum", o.PartitionNum),
		zap.Int("maxMessageBytes", o.MaxMessageBytes),
		zap.Int("maxBatchedBytes", o.MaxBatchedBytes),
		zap.String("compression", compression),
		zap.Int16("requiredAcks", int16(o.RequiredAcks)),
		zap.Int("maxRetry", o.MaxRetry),
		zap.Duration("dialTimeout", o.DialTimeout),
		zap.Duration("readTimeout", o.ReadTimeout),
		zap.Duration("writeTimeout", o.WriteTimeout))
	return &franzFactory{
		changefeedID: changefeedID,
		clientOpts:   clientOpts,
		producerOpts: producerOpts,
		timeout:      timeout,
	}, nil
}

func (f *franzFactory) AdminClient(ctx context.Context) (AdminClient, error) {
	return newAdmin(ctx, f.changefeedID, f.clientOpts, f.timeout)
}

func (f *franzFactory) SyncProducer(ctx context.Context) (SyncProducer, error) {
	client, err := newProducerClient(ctx, f.changefeedID, "sync-producer", f.clientOpts, f.producerOpts)
	if err != nil {
		return nil, err
	}
	return &syncProducer{id: f.changefeedID, client: client, timeout: f.timeout}, nil
}

func (f *franzFactory) AsyncProducer(ctx context.Context) (AsyncProducer, error) {
	client, err := newProducerClient(ctx, f.changefeedID, "async-producer", f.clientOpts, f.producerOpts)
	if err != nil {
		return nil, err
	}
	return &asyncProducer{client: client, changefeedID: f.changefeedID, errCh: make(chan error, 1)}, nil
}

func (f *franzFactory) MetricsCollector(AdminClient) MetricsCollector {
	return noopMetricsCollector{}
}

func (f *franzFactory) CleanupMetrics() { cleanupMetrics(f.changefeedID) }

type noopMetricsCollector struct{}

func (noopMetricsCollector) Run(ctx context.Context) { <-ctx.Done() }
