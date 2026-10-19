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

package main

import (
	"cmp"
	"context"
	"iter"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/auth"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	pulsarutil "github.com/pingcap/ticdc/pkg/sink/pulsar"
	putil "github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

type pulsarRecord struct {
	ack       *ack
	messageID pulsar.MessageID
}

type pulsarPartition struct {
	records []pulsarRecord
	mu      sync.Mutex // Protects records, shared by Read and Confirm.
}

type pulsarReader struct {
	*readLimit
	client      pulsar.Client
	consumer    pulsar.Consumer
	memory      *memoryUsage
	partitions  map[string]*pulsarPartition
	confirmable chan<- struct{}
}

func newPulsarReader(ctx context.Context, upstreamURI *url.URL, consumerID string, replicaConfig *config.ReplicaConfig, memory *memoryUsage, confirmable chan<- struct{}) (reader *pulsarReader) {
	if err := context.Cause(ctx); err != nil {
		log.Panic("Pulsar reader initialization canceled", zap.Error(err))
	}
	if upstreamURI.Host == "" {
		log.Panic("Pulsar upstream URI must include an endpoint")
	}
	topic := strings.Trim(upstreamURI.Path, "/")
	if topic == "" {
		log.Panic("Pulsar upstream URI must include a topic")
	}
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
		TLSTrustCertsFilePath: cmp.Or(query.Get("ca"), putil.GetOrZero(pulsarConfig.TLSTrustCertsFilePath)),
		TLSCertificateFile:    cmp.Or(query.Get("cert"), putil.GetOrZero(pulsarConfig.TLSCertificateFile)),
		TLSKeyFilePath:        cmp.Or(query.Get("key"), putil.GetOrZero(pulsarConfig.TLSKeyFilePath)),
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
	token := cmp.Or(query.Get("authentication-token"), putil.GetOrZero(pulsarConfig.AuthenticationToken))
	tokenFile := cmp.Or(query.Get("token-from-file"), putil.GetOrZero(pulsarConfig.TokenFromFile))
	certificate := cmp.Or(query.Get("auth-tls-certificate-path"), putil.GetOrZero(pulsarConfig.AuthTLSCertificatePath))
	key := cmp.Or(query.Get("auth-tls-private-key-path"), putil.GetOrZero(pulsarConfig.AuthTLSPrivateKeyPath))
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
	defer func() {
		if reader == nil {
			client.Close()
		}
	}()
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
	partitions := make(map[string]*pulsarPartition, len(topics))
	for _, partitionTopic := range topics {
		partitions[partitionTopic] = &pulsarPartition{}
	}
	c := &pulsarReader{
		readLimit:   newReadLimit(memory),
		client:      client,
		consumer:    consumer,
		memory:      memory,
		partitions:  partitions,
		confirmable: confirmable,
	}
	log.Info("Pulsar reader initialized", zap.String("topic", topic), zap.Int("partitionCount", len(partitions)))
	return c
}

type pulsarInput struct {
	message pulsar.ConsumerMessage
	record  *ack
}

func (c *pulsarReader) Run(ctx context.Context, parser *inputParser, output chan<- []assemblyInput) error {
	return readAndParse(ctx, c.Read, func(ctx context.Context, input pulsarInput) iter.Seq2[assemblyInput, error] {
		scope := inputScope{record: input.record, dmlBoundary: &readBoundary{reached: true}, ddlOrder: commitOrder}
		return parser.parseMessage(ctx, scope, []byte(input.message.Key()), input.message.Payload())
	}, output)
}

func (c *pulsarReader) Read(ctx context.Context) ([]pulsarInput, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	var message pulsar.ConsumerMessage
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case message = <-c.consumer.Chan():
	}
	var (
		batch []pulsarInput
		bytes int64
	)
	for {
		partition := c.partitions[message.Topic()]
		messageID := message.ID()
		size := int64(len(message.Key()) + len(message.Payload()))
		record := newAck(c.memory, size, c.confirmable)
		partition.mu.Lock()
		partition.records = append(partition.records, pulsarRecord{
			ack:       record,
			messageID: messageID,
		})
		partition.mu.Unlock()
		batch = append(batch, pulsarInput{message: message, record: record})
		bytes += size
		if len(batch) >= batchRows || bytes >= batchBytes || c.memory.used() >= c.memory.limit {
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

func (c *pulsarReader) Advance(_ []readUpdate) {
}

func (c *pulsarReader) Boundary(_ []event.SchemaTableName, _ uint64) *readBoundary {
	// Topic-wide checkpoints provide this transport's completion boundaries.
	return nil
}

func (c *pulsarReader) Confirm(ctx context.Context) error {
	for _, partition := range c.partitions {
		partition.mu.Lock()
		var count int
		for _, record := range partition.records {
			if !record.ack.ready() {
				break
			}
			count++
		}
		if count == 0 {
			partition.mu.Unlock()
			continue
		}
		messageID := partition.records[count-1].messageID
		partition.mu.Unlock()
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if err := c.consumer.AckIDCumulative(messageID); err != nil {
			return errors.WrapError(errors.ErrInternalCheckFailed, err, "confirm Pulsar messages")
		}
		partition.mu.Lock()
		records := partition.records
		clear(records[:count])
		partition.records = records[count:]
		partition.mu.Unlock()
	}
	return nil
}

func (c *pulsarReader) Close() {
	c.consumer.Close()
	c.client.Close()
}
