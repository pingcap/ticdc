// Copyright 2022 PingCAP, Inc.
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

package producer

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"go.uber.org/zap"
)

// kafkaDMLProducer is used to send messages to kafka.
type KafkaDMLProducer struct {
	// id indicates which processor (changefeed) this sink belongs to.
	id commonType.ChangeFeedID
	// asyncProducer is used to send messages to kafka asynchronously.
	asyncProducer kafka.AsyncProducer
	// closedMu is used to protect `closed`.
	// We need to ensure that closed producers are never written to.
	closedMu sync.RWMutex
	// closed is used to indicate whether the producer is closed.
	// We also use it to guard against double closes.
	closed bool
}

// NewKafkaDMLProducer creates a new kafka producer.
func NewKafkaDMLProducer(
	changefeedID commonType.ChangeFeedID,
	asyncProducer kafka.AsyncProducer,
) *KafkaDMLProducer {
	k := &KafkaDMLProducer{
		id:            changefeedID,
		asyncProducer: asyncProducer,
		closed:        false,
	}
	return k
}

func (k *KafkaDMLProducer) Run(ctx context.Context) error {
	err := k.asyncProducer.AsyncRunCallback(ctx)
	if err != nil && errors.Cause(err) != context.Canceled {
		return err
	}
	return nil
}

func (k *KafkaDMLProducer) AsyncSendMessage(
	ctx context.Context, topic string,
	partition int32, message *common.Message,
) error {
	// We have to hold the lock to avoid writing to a closed producer.
	// Close may be blocked for a long time.
	k.closedMu.RLock()
	defer k.closedMu.RUnlock()

	// If the producer is closed, we should skip the message and return an error.
	if k.closed {
		return cerror.ErrKafkaProducerClosed.GenWithStackByArgs()
	}
	return k.asyncProducer.AsyncSend(ctx, topic, partition, message)
}

func (k *KafkaDMLProducer) Close() {
	// We have to hold the lock to synchronize closing with writing.
	k.closedMu.Lock()
	defer k.closedMu.Unlock()
	// If the producer has already been closed, we should skip this close operation.
	if k.closed {
		// We need to guard against double closing the clients,
		// which could lead to panic.
		log.Warn("Kafka DML producer already closed",
			zap.String("namespace", k.id.Namespace()),
			zap.String("changefeed", k.id.Name()))
		return
	}
	k.asyncProducer.Close()
	k.closed = true
}
