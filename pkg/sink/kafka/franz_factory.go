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
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type franzFactory struct {
	changefeedID common.ChangeFeedID
	client       *kgo.Client
	closeOnce    sync.Once
}

func newFranzFactory(ctx context.Context, o *options, changefeedID common.ChangeFeedID) (Factory, error) {
	clientOpts, err := clientOptions(ctx, o)
	if err != nil {
		return nil, err
	}
	admin, err := newAdmin(ctx, changefeedID, clientOpts)
	if err != nil {
		return nil, err
	}
	err = adjustOptions(ctx, changefeedID, admin, o, o.Topic)
	admin.Close()
	if err != nil {
		return nil, err
	}
	producerOpts := producerOptions(o)
	metricsHook := newMetricsHook(changefeedID)
	opts := make([]kgo.Opt, 0, len(clientOpts)+len(producerOpts)+4)
	opts = append(opts, clientOpts...)
	opts = append(opts,
		kgo.WithContext(ctx),
		kgo.WithLogger(newClientLogger(changefeedID, "shared")),
		kgo.WithHooks(metricsHook),
		kgo.MetadataMinAge(adminMetadataMinAge))
	opts = append(opts, producerOpts...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		cleanupMetrics(changefeedID)
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
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
		client:       client,
	}, nil
}

func (f *franzFactory) AdminClient(context.Context) (AdminClient, error) {
	return &admin{
		changefeed: f.changefeedID,
		admin:      kadm.NewClient(f.client),
	}, nil
}

func (f *franzFactory) SyncProducer(context.Context) (SyncProducer, error) {
	return &syncProducer{
		id:     f.changefeedID,
		client: f.client,
	}, nil
}

func (f *franzFactory) AsyncProducer(context.Context) (AsyncProducer, error) {
	return &asyncProducer{
		client:       f.client,
		changefeedID: f.changefeedID,
		resultCh:     make(chan asyncProduceResult, producerMaxBufferedRecords),
	}, nil
}

func (f *franzFactory) Close() {
	f.closeOnce.Do(func() {
		if f.client != nil {
			f.client.Close()
		}
		cleanupMetrics(f.changefeedID)
	})
}

func (f *franzFactory) MetricsCollector(AdminClient) MetricsCollector {
	return noopMetricsCollector{}
}

type noopMetricsCollector struct{}

func (noopMetricsCollector) Run(ctx context.Context) { <-ctx.Done() }
