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

package source

import (
	"cmp"
	"context"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/auth"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/consumer/budget"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	pulsarutil "github.com/pingcap/ticdc/pkg/sink/pulsar"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type pulsarReader struct {
	client     pulsar.Client
	consumer   pulsar.Consumer
	topic      string
	memory     *budget.Budget
	continuing atomic.Bool
	partitions map[string]int32
}

func newPulsarReader(opts Options) *pulsarReader {
	upstreamURI := opts.UpstreamURI
	consumerID := opts.ConsumerID
	replicaConfig := opts.ReplicaConfig
	memory := opts.Budget
	topic := strings.Trim(upstreamURI.Path, "/")
	if strings.Contains(topic, ",") {
		log.Panic("consumer accepts one Pulsar topic", zap.String("topic", topic))
	}
	query := upstreamURI.Query()
	pulsarConfig := config.PulsarConfig{}
	if replicaConfig.Sink.PulsarConfig != nil {
		pulsarConfig = *replicaConfig.Sink.PulsarConfig
	}
	brokerScheme := upstreamURI.Scheme
	switch brokerScheme {
	case config.PulsarHTTPScheme:
		brokerScheme = "http"
	case config.PulsarHTTPSScheme:
		brokerScheme = "https"
	}
	clientOptions := pulsar.ClientOptions{
		URL:                   brokerScheme + "://" + upstreamURI.Host,
		ConnectionTimeout:     5 * time.Second,
		OperationTimeout:      5 * time.Second,
		TLSValidateHostname:   true,
		TLSTrustCertsFilePath: cmp.Or(query.Get("ca"), util.GetOrZero(pulsarConfig.TLSTrustCertsFilePath)),
		TLSCertificateFile:    cmp.Or(query.Get("cert"), util.GetOrZero(pulsarConfig.TLSCertificateFile)),
		TLSKeyFilePath:        cmp.Or(query.Get("key"), util.GetOrZero(pulsarConfig.TLSKeyFilePath)),
		Logger:                pulsarutil.NewPulsarLogger(log.L()),
		MemoryLimitBytes:      64 << 20,
	}
	if (clientOptions.TLSCertificateFile == "") != (clientOptions.TLSKeyFilePath == "") {
		log.Panic("Pulsar TLS certificate and key must be configured together")
	}
	oauth2 := config.OAuth2{}
	if pulsarConfig.OAuth2 != nil {
		oauth2 = *pulsarConfig.OAuth2
	}
	for name, value := range map[string]*string{
		"oauth2-issuer-url": &oauth2.OAuth2IssuerURL, "oauth2-private-key": &oauth2.OAuth2PrivateKey,
		"oauth2-client-id": &oauth2.OAuth2ClientID, "oauth2-scope": &oauth2.OAuth2Scope, "oauth2-audience": &oauth2.OAuth2Audience,
	} {
		if query.Has(name) {
			*value = query.Get(name)
		}
	}
	token := cmp.Or(query.Get("authentication-token"), util.GetOrZero(pulsarConfig.AuthenticationToken))
	tokenFile := cmp.Or(query.Get("token-from-file"), util.GetOrZero(pulsarConfig.TokenFromFile))
	certificate := cmp.Or(query.Get("auth-tls-certificate-path"), util.GetOrZero(pulsarConfig.AuthTLSCertificatePath))
	key := cmp.Or(query.Get("auth-tls-private-key-path"), util.GetOrZero(pulsarConfig.AuthTLSPrivateKeyPath))
	if (certificate == "") != (key == "") {
		log.Panic("Pulsar authentication certificate and key must be configured together")
	}
	useOAuth2 := oauth2.OAuth2IssuerURL != "" || oauth2.OAuth2ClientID != "" || oauth2.OAuth2PrivateKey != "" || oauth2.OAuth2Audience != "" || oauth2.OAuth2Scope != ""
	var authCount int
	for _, enabled := range []bool{token != "", tokenFile != "", certificate != "", useOAuth2} {
		if enabled {
			authCount++
		}
	}
	if authCount > 1 {
		log.Panic("configure only one Pulsar authentication method")
	}
	switch {
	case token != "":
		clientOptions.Authentication = pulsar.NewAuthenticationToken(token)
	case tokenFile != "":
		clientOptions.Authentication = pulsar.NewAuthenticationTokenFromFile(tokenFile)
	case certificate != "":
		clientOptions.Authentication = pulsar.NewAuthenticationTLS(certificate, key)
	case useOAuth2:
		if oauth2.OAuth2IssuerURL == "" || oauth2.OAuth2ClientID == "" || oauth2.OAuth2PrivateKey == "" || oauth2.OAuth2Audience == "" {
			log.Panic("Pulsar OAuth2 requires issuer, client ID, private key and audience")
		}
		authentication, err := auth.NewAuthenticationOAuth2WithParams(map[string]string{
			auth.ConfigParamType: auth.ConfigParamTypeClientCredentials, auth.ConfigParamIssuerURL: oauth2.OAuth2IssuerURL,
			auth.ConfigParamClientID: oauth2.OAuth2ClientID, auth.ConfigParamKeyFile: oauth2.OAuth2PrivateKey,
			auth.ConfigParamAudience: oauth2.OAuth2Audience, auth.ConfigParamScope: oauth2.OAuth2Scope,
		})
		if err != nil {
			log.Panic("Pulsar OAuth2 authentication initialization failed", zap.Error(err))
		}
		clientOptions.Authentication = authentication
	}
	client, err := pulsar.NewClient(clientOptions)
	if err != nil {
		log.Panic("Pulsar client initialization failed", zap.Error(err))
	}
	topics, err := client.TopicPartitions(topic)
	if err != nil {
		log.Panic("Pulsar partition discovery failed", zap.String("topic", topic), zap.Error(err))
	}
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                             topic,
		SubscriptionName:                  consumerID,
		Type:                              pulsar.Exclusive,
		SubscriptionInitialPosition:       pulsar.SubscriptionPositionEarliest,
		ReceiverQueueSize:                 64,
		MessageChannel:                    make(chan pulsar.ConsumerMessage, 64),
		EnableAutoScaledReceiverQueueSize: true,
		AckWithResponse:                   true,
		MaxPendingChunkedMessage:          1,
	})
	if err != nil {
		log.Panic("Pulsar topic subscription failed", zap.String("topic", topic), zap.String("consumerID", consumerID), zap.Error(err))
	}
	partitions := make(map[string]int32, len(topics))
	for id, partitionTopic := range topics {
		partitions[partitionTopic] = int32(id)
	}
	c := &pulsarReader{
		client:     client,
		consumer:   consumer,
		topic:      topic,
		memory:     memory,
		partitions: partitions,
	}
	log.Info("pulsar reader initialized", zap.String("topic", topic), zap.Int("partitionCount", len(partitions)))
	return c
}

func (c *pulsarReader) Streams() []Stream {
	streams := make([]Stream, 0, len(c.partitions))
	for _, partition := range c.partitions {
		streams = append(streams, Stream{
			ID:    partition,
			Topic: c.topic,
		})
	}
	return streams
}

func (c *pulsarReader) Read(ctx context.Context) ([]*Input, error) {
	var message pulsar.ConsumerMessage
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case message = <-c.consumer.Chan():
	}
	var (
		batch []*Input
		bytes int64
	)
	for {
		partition, ok := c.partitions[message.Topic()]
		if !ok {
			return nil, errors.ErrPulsarInvalidConfig.GenWithStackByArgs("Pulsar partitions changed; restart the consumer")
		}
		size := int64(len(message.Key()) + len(message.Payload()))
		continuing := c.continuing.Load()
		if continuing {
			c.memory.ForceAcquire(size)
		}
		if !continuing {
			if err := c.memory.Acquire(ctx, size); err != nil {
				return nil, err
			}
		}
		id := message.ID()
		batch = append(batch, &Input{
			Stream: partition,
			Key:    []byte(message.Key()),
			Value:  message.Payload(),
			Bytes:  size,
			Ack: func(ctx context.Context) error {
				return c.confirmPosition(ctx, id)
			},
		})
		bytes += size
		if len(batch) >= readBatchRows || bytes >= readBatchBytes || c.memory.Used() >= c.memory.Limit() {
			return batch, nil
		}
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case message = <-c.consumer.Chan():
		default:
			return batch, nil
		}
	}
}

func (c *pulsarReader) ContinueReading(enabled bool) {
	c.continuing.Store(enabled)
}

func (c *pulsarReader) Semantics() Semantics {
	// Pulsar preserves per-partition order but DDLs can be decoded out of order.
	return Semantics{DDLsCanReorder: true}
}

func (c *pulsarReader) Advance(Progress) bool {
	// Pulsar uses global watermarks and has no selective reading boundary.
	return true
}

func (c *pulsarReader) Close() {
	c.consumer.Close()
	c.client.Close()
}

func (c *pulsarReader) confirmPosition(ctx context.Context, id pulsar.MessageID) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := c.consumer.AckIDCumulative(id); err != nil {
		return errors.WrapError(errors.ErrInternalCheckFailed, err, "confirm Pulsar messages")
	}
	return nil
}
