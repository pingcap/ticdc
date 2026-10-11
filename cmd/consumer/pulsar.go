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
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/auth"
	"github.com/pingcap/log"
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
	client       pulsar.Client
	consumer     pulsar.Consumer
	memory       *memoryUsage
	partitions   map[string]*pulsarPartition
	confirmable  chan<- struct{}
	watermark    uint64
	hasWatermark bool
}

func newPulsarReader(ctx context.Context, upstreamURI *url.URL, consumerID string, replicaConfig *config.ReplicaConfig, memory *memoryUsage, confirmable chan<- struct{}) (reader *pulsarReader, err error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if upstreamURI.Host == "" {
		return nil, errors.ErrInvalidReplicaConfig.FastGenByArgs("pulsar upstream-uri must include an endpoint")
	}
	topic := strings.Trim(upstreamURI.Path, "/")
	if topic == "" {
		return nil, errors.ErrInvalidReplicaConfig.FastGenByArgs("pulsar upstream-uri must include a topic")
	}
	if strings.Contains(topic, ",") {
		return nil, errors.ErrPulsarInvalidConfig.FastGenByArgs("cdc_consumer accepts one Pulsar topic")
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
		return nil, errors.ErrPulsarInvalidConfig.FastGenByArgs("Pulsar TLS certificate and key must be configured together")
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
		return nil, errors.ErrPulsarInvalidConfig.FastGenByArgs("Pulsar authentication certificate and key must be configured together")
	}
	useOAuth2 := oauth2.OAuth2IssuerURL != "" || oauth2.OAuth2ClientID != "" || oauth2.OAuth2PrivateKey != "" || oauth2.OAuth2Audience != "" || oauth2.OAuth2Scope != ""
	var authCount int
	for _, enabled := range []bool{token != "", tokenFile != "", certificate != "", useOAuth2} {
		if enabled {
			authCount++
		}
	}
	if authCount > 1 {
		return nil, errors.ErrPulsarInvalidConfig.FastGenByArgs("configure only one Pulsar authentication method")
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
			return nil, errors.ErrPulsarInvalidConfig.FastGenByArgs("Pulsar OAuth2 requires issuer, client ID, private key and audience")
		}
		clientOptions.Authentication, err = auth.NewAuthenticationOAuth2WithParams(map[string]string{
			auth.ConfigParamType: auth.ConfigParamTypeClientCredentials, auth.ConfigParamIssuerURL: oauth2.OAuth2IssuerURL,
			auth.ConfigParamClientID: oauth2.OAuth2ClientID, auth.ConfigParamKeyFile: oauth2.OAuth2PrivateKey,
			auth.ConfigParamAudience: oauth2.OAuth2Audience, auth.ConfigParamScope: oauth2.OAuth2Scope,
		})
		if err != nil {
			return nil, errors.WrapError(errors.ErrPulsarInvalidConfig, err, "initialize Pulsar OAuth2 authentication")
		}
	}
	client, err := pulsar.NewClient(clientOptions)
	if err != nil {
		return nil, errors.WrapError(errors.ErrPulsarInvalidConfig, err, "create Pulsar client")
	}
	defer func() {
		if reader == nil {
			client.Close()
		}
	}()
	topics, err := client.TopicPartitions(topic)
	if err != nil {
		return nil, errors.WrapError(errors.ErrPulsarInvalidConfig, err, "discover Pulsar partitions")
	}
	if len(topics) == 0 {
		return nil, errors.ErrPulsarInvalidConfig.FastGenByArgs("Pulsar topic has an invalid partition count")
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
		return nil, errors.WrapError(errors.ErrPulsarInvalidConfig, err, "subscribe to Pulsar topic")
	}
	// Reserve SDK prefetch headroom inside the shared soft budget. Its queues
	// scale down under the client memory limit; oversized messages may exceed it.
	memory.externalBytes = func() int64 { return clientOptions.MemoryLimitBytes }
	partitions := make(map[string]*pulsarPartition, len(topics))
	for _, partitionTopic := range topics {
		partitions[partitionTopic] = &pulsarPartition{}
	}
	c := &pulsarReader{
		client:      client,
		consumer:    consumer,
		memory:      memory,
		partitions:  partitions,
		confirmable: confirmable,
	}
	log.Info("Pulsar reader initialized", zap.String("topic", topic), zap.Int("partitionCount", len(partitions)))
	return c, nil
}

func (c *pulsarReader) Read(ctx context.Context) (*readData, error) {
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case message, ok := <-c.consumer.Chan():
		if !ok {
			return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Pulsar reader stopped unexpectedly")
		}
		partition := c.partitions[message.Topic()]
		if partition == nil {
			return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Pulsar message belongs to an unknown partition")
		}
		messageID := message.ID()
		size := int64(len(message.Key()) + len(message.Payload()) + len(messageID.Serialize()) + 256)
		record, err := c.memory.newAck(ctx, size, c.confirmable)
		if err != nil {
			return nil, err
		}
		partition.mu.Lock()
		partition.records = append(partition.records, pulsarRecord{
			ack:       record,
			messageID: messageID,
		})
		partition.mu.Unlock()
		return &readData{
			key:           []byte(message.Key()),
			value:         message.Payload(),
			record:        record,
			retainedBytes: 256,
			dmlBoundary:   &readBoundary{reached: true},
			ddlOrder:      commitOrder,
		}, nil
	}
}

func (c *pulsarReader) Advance(ctx context.Context, feedback readFeedback) (readProgress, error) {
	result := readProgress{
		watermark:    c.watermark,
		hasWatermark: c.hasWatermark,
	}
	result.needsMoreInput = feedback.boundaryDDL != nil && (!c.hasWatermark || feedback.boundaryDDL.GetCommitTs() > c.watermark)
	if !feedback.hasWatermark {
		return result, nil
	}
	c.watermark = max(c.watermark, feedback.watermark)
	c.hasWatermark = true
	if err := c.memory.reserve(ctx, 128); err != nil {
		return result, err
	}
	feedback.data.record.refs.Add(1)
	result.watermark, result.hasWatermark = c.watermark, true
	result.control = &readControl{
		watermark: feedback.watermark,
		records:   []*ack{feedback.data.record},
	}
	return result, nil
}

func (c *pulsarReader) Confirm(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	type pulsarCommit struct {
		partition *pulsarPartition
		count     int
		messageID pulsar.MessageID
	}
	var commits []pulsarCommit
	for _, partition := range c.partitions {
		partition.mu.Lock()
		var count int
		for _, record := range partition.records {
			refs := record.ack.refs.Load()
			if refs < 0 {
				partition.mu.Unlock()
				return errors.ErrInternalCheckFailed.FastGenByArgs("Pulsar input completed more than once")
			}
			if refs != 0 {
				break
			}
			count++
		}
		if count != 0 {
			commits = append(commits, pulsarCommit{
				partition: partition,
				count:     count,
				messageID: partition.records[count-1].messageID,
			})
		}
		partition.mu.Unlock()
	}
	for _, commit := range commits {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if err := c.consumer.AckIDCumulative(commit.messageID); err != nil {
			return errors.WrapError(errors.ErrInternalCheckFailed, err, "confirm Pulsar messages")
		}
		partition := commit.partition
		partition.mu.Lock()
		records := partition.records
		for _, record := range records[:commit.count] {
			c.memory.confirm(record.ack)
		}
		clear(records[:commit.count])
		partition.records = records[commit.count:]
		partition.mu.Unlock()
	}
	return nil
}

func (c *pulsarReader) Close() error {
	c.consumer.Close()
	c.client.Close()
	return nil
}
