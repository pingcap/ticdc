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
	"slices"
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

type pulsarReader struct {
	client       pulsar.Client
	consumer     pulsar.Consumer
	memory       *memoryUsage
	records      map[int32][]*ack
	mu           sync.Mutex
	partitionIDs map[string]int32
	messageIDs   map[*ack]pulsar.MessageID
	positions    map[int32]pulsar.MessageID
	checkpoints  []*pulsarCheckpoint
	watermark    uint64
}

type pulsarCheckpoint struct {
	watermark uint64
	record    *ack
	positions map[int32]pulsar.MessageID
}

func newPulsarReader(ctx context.Context, upstreamURI *url.URL, consumerID string, replicaConfig *config.ReplicaConfig, memory *memoryUsage) (*pulsarReader, error) {
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
	var err error
	authCount := 0
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
	topics, err := client.TopicPartitions(topic)
	if err != nil {
		client.Close()
		return nil, errors.WrapError(errors.ErrPulsarInvalidConfig, err, "discover Pulsar partitions")
	}
	if len(topics) == 0 {
		client.Close()
		return nil, errors.ErrPulsarInvalidConfig.FastGenByArgs("Pulsar topic has an invalid partition count")
	}
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topics: topics, SubscriptionName: consumerID, Type: pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		ReceiverQueueSize:           1, MessageChannel: make(chan pulsar.ConsumerMessage, 1),
		AckWithResponse: true, MaxPendingChunkedMessage: 1,
	})
	if err != nil {
		client.Close()
		return nil, errors.WrapError(errors.ErrPulsarInvalidConfig, err, "subscribe to Pulsar topic")
	}
	partitionIDs := make(map[string]int32, len(topics))
	for index, partitionTopic := range topics {
		partitionIDs[partitionTopic] = int32(index)
	}
	c := &pulsarReader{
		client: client, consumer: consumer, memory: memory, partitionIDs: partitionIDs,
		records: make(map[int32][]*ack), messageIDs: make(map[*ack]pulsar.MessageID),
		positions: make(map[int32]pulsar.MessageID),
	}
	log.Info("Pulsar reader initialized", zap.String("topic", topic), zap.Int("partitionCount", len(partitionIDs)))
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
		partitionID, ok := c.partitionIDs[message.Topic()]
		if !ok {
			return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Pulsar message belongs to an unknown partition")
		}
		size := int64(len(message.Key()) + len(message.Payload()) + len(message.ID().Serialize()) + 256)
		if size > maxRecordBytes {
			return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Pulsar message exceeds its size limit")
		}
		record, err := c.memory.newAck(ctx, size)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		c.messageIDs[record] = message.ID()
		c.records[partitionID] = append(c.records[partitionID], record)
		c.mu.Unlock()
		position := message.ID()
		if position.BatchIdx() >= 0 && position.BatchIdx()+1 == position.BatchSize() {
			// Broker boundaries cover whole entries, including every batch message.
			position = pulsar.NewMessageID(position.LedgerID(), position.EntryID(), -1, position.PartitionIdx())
		}
		c.positions[partitionID] = position
		return &readData{key: []byte(message.Key()), value: message.Payload(), partition: partitionID, record: record}, nil
	}
}

// Checkpoint payloads need a broker snapshot to cover idle partitions as well.
func (c *pulsarReader) readCheckpoint(ctx context.Context, watermark uint64, record *ack) error {
	positions, err := c.consumer.GetLastMessageIDs()
	if err != nil {
		return errors.WrapError(errors.ErrInternalCheckFailed, err, "read Pulsar checkpoint positions")
	}
	if len(positions) != len(c.partitionIDs) {
		return errors.ErrInternalCheckFailed.FastGenByArgs("Pulsar checkpoint does not cover every subscribed partition")
	}
	if err := c.memory.reserve(ctx, 128+int64(len(positions))*128); err != nil {
		return err
	}
	checkpointPositions := make(map[int32]pulsar.MessageID, len(positions))
	for _, position := range positions {
		partitionID, ok := c.partitionIDs[position.Topic()]
		if !ok {
			return errors.ErrInternalCheckFailed.FastGenByArgs("Pulsar checkpoint belongs to an unknown partition")
		}
		checkpointPositions[partitionID] = position
	}
	record.refs.Add(1)
	c.checkpoints = append(c.checkpoints, &pulsarCheckpoint{watermark: watermark, record: record, positions: checkpointPositions})
	return nil
}

func (c *pulsarReader) advanceWatermarks() []*pulsarCheckpoint {
	var completed []*pulsarCheckpoint
	for len(c.checkpoints) != 0 {
		checkpoint := c.checkpoints[0]
		for partitionID, target := range checkpoint.positions {
			if target.EntryID() < 0 {
				continue
			}
			position := c.positions[partitionID]
			if position == nil {
				return completed
			}
			comparison := cmp.Compare(position.LedgerID(), target.LedgerID())
			if comparison == 0 {
				comparison = cmp.Compare(position.EntryID(), target.EntryID())
			}
			if comparison < 0 || (comparison == 0 && position.BatchIdx() >= 0) {
				return completed
			}
		}
		c.watermark = max(c.watermark, checkpoint.watermark)
		completed = append(completed, checkpoint)
		c.memory.release(128 + int64(len(checkpoint.positions))*128)
		c.checkpoints[0] = nil
		c.checkpoints = c.checkpoints[1:]
	}
	return completed
}

func (c *pulsarReader) Confirm(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for partitionID, records := range c.records {
		count := 0
		for _, record := range records {
			refs := record.refs.Load()
			if refs < 0 {
				return errors.ErrInternalCheckFailed.FastGenByArgs("Pulsar input completed more than once")
			}
			if refs != 0 {
				break
			}
			count++
		}
		if count == 0 {
			continue
		}
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if err := c.consumer.AckIDCumulative(c.messageIDs[records[count-1]]); err != nil {
			return errors.WrapError(errors.ErrInternalCheckFailed, err, "confirm Pulsar messages")
		}
		for _, record := range records[:count] {
			c.memory.confirm(record)
			delete(c.messageIDs, record)
		}
		c.records[partitionID] = slices.Delete(records, 0, count)
	}
	return nil
}

func (c *pulsarReader) Close() error {
	c.consumer.Close()
	c.client.Close()
	return nil
}
