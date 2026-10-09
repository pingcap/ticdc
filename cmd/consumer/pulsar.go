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
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	pulsarutil "github.com/pingcap/ticdc/pkg/sink/pulsar"
	putil "github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

type pulsarReader struct {
	client            pulsar.Client
	consumer          pulsar.Consumer
	buffer            *readBuffer
	mu                sync.Mutex
	partitionIDs      map[string]int32
	messageIDs        map[*inputRecord]pulsar.MessageID
	pendingWatermarks []*readResult
	watermark         uint64
	hasWatermark      bool
}

func newPulsarReader(ctx context.Context, upstreamURI *url.URL, consumerID, timezone string, replicaConfig *config.ReplicaConfig, memory *bufferUsage) (*pulsarReader, error) {
	topic := strings.Trim(upstreamURI.Path, "/")
	if strings.Contains(topic, ",") {
		return nil, errors.ErrPulsarInvalidConfig.FastGenByArgs("cdc_consumer accepts one Pulsar topic")
	}
	query := upstreamURI.Query()
	protocol, err := config.ParseSinkProtocolFromString(cmp.Or(query.Get(config.ProtocolKey), putil.GetOrZero(replicaConfig.Sink.Protocol), "canal-json"))
	if err != nil {
		return nil, err
	}
	if protocol != config.ProtocolCanalJSON {
		return nil, errors.ErrPulsarInvalidConfig.FastGenByArgs("Pulsar consumer requires canal-json")
	}
	codecConfig := codecCommon.NewConfig(protocol)
	if err := codecConfig.Apply(upstreamURI, replicaConfig.Sink); err != nil {
		return nil, err
	}
	if !codecConfig.EnableTiDBExtension {
		return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("enable-tidb-extension must be true")
	}
	codecConfig.TimeZone, err = putil.GetTimezone(timezone)
	if err != nil {
		return nil, err
	}
	// Canal's DDL metadata belongs to the logical topic. Decode its merged
	// stream serially while retaining each physical partition's ACK position.
	decoder, err := codec.NewEventDecoder(ctx, 0, codecConfig, topic, nil)
	if err != nil {
		return nil, err
	}
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
	partitions := make(map[int32]*partition, len(topics))
	partitionIDs := make(map[string]int32, len(topics))
	schemas := make(map[schemaKey]bool)
	schemaPointers := make(map[*common.TableInfo]bool)
	for index, partitionTopic := range topics {
		partitionIDs[partitionTopic] = int32(index)
		partitions[int32(index)] = &partition{decoder: decoder, schemas: schemas, schemaPointers: schemaPointers}
	}

	buffer := &readBuffer{memory: memory, protocol: protocol, partitions: partitions, orderedDML: true, dmlBoundary: ^uint64(0)}
	c := &pulsarReader{client: client, consumer: consumer, buffer: buffer, partitionIDs: partitionIDs, messageIDs: make(map[*inputRecord]pulsar.MessageID)}
	log.Info("Pulsar reader initialized", zap.String("topic", topic), zap.Int("partitionCount", len(partitions)))
	return c, nil
}

func (c *pulsarReader) Read(ctx context.Context) (*readResult, error) {
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		if c.hasWatermark || len(c.buffer.pendingDDL) != 0 || c.buffer.orderedDML {
			if result := c.buffer.nextReady(c.watermark); result != nil {
				return result, nil
			}
		}
		if len(c.pendingWatermarks) != 0 {
			result := c.pendingWatermarks[0]
			c.pendingWatermarks[0] = nil
			c.pendingWatermarks = c.pendingWatermarks[1:]
			return result, nil
		}
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
			record, err := c.buffer.newRecord(ctx, size)
			if err != nil {
				return nil, err
			}
			p := c.buffer.partitions[partitionID]
			c.mu.Lock()
			c.messageIDs[record] = message.ID()
			p.records = append(p.records, record)
			c.mu.Unlock()
			p.decoder.AddKeyValue([]byte(message.Key()), message.Payload())
			for {
				messageType, hasNext := p.decoder.HasNext()
				if !hasNext {
					break
				}
				switch messageType {
				case codecCommon.MessageTypeRow:
					message := p.decoder.NextDMLMessage()
					if message == nil {
						return nil, errors.ErrCodecDecode.FastGenByArgs("Pulsar decoder returned an empty DML message")
					}
					if err := c.buffer.queueDML(ctx, message.ToDMLEvent(), []*inputRecord{record}, p); err != nil {
						return nil, err
					}
				case codecCommon.MessageTypeDDL:
					ddl := p.decoder.NextDDLEvent()
					if ddl == nil || ddl.Query == "" {
						return nil, errors.ErrCodecDecode.FastGenByArgs("Pulsar decoder returned an empty DDL event")
					}
					key := schemaKey{schema: ddl.SchemaName, table: ddl.TableName, version: ddl.FinishedTs}
					if !p.schemas[key] {
						if err := c.buffer.memory.reserve(ctx, 128); err != nil {
							return nil, err
						}
						p.schemas[key] = true
						c.buffer.memory.readBytes.Add(128)
					}
					if err := c.buffer.queueDDL(ctx, ddl, record); err != nil {
						return nil, err
					}
				case codecCommon.MessageTypeResolved:
					watermark := p.decoder.NextResolvedEvent()
					c.buffer.memory.effects.Add(1)
					record.pending.Add(1)
					c.watermark = max(c.watermark, watermark)
					c.hasWatermark = true
					c.pendingWatermarks = append(c.pendingWatermarks, &readResult{watermark: watermark, hasWatermark: true, onFlush: func() {
						record.pending.Add(-1)
						c.buffer.memory.effects.Add(-1)
					}})
				default:
					return nil, errors.ErrCodecDecode.FastGenByArgs("Pulsar decoder returned an unknown message type")
				}
			}
			released := record.bytes.Swap(256) - 256
			c.buffer.memory.readBytes.Add(-released)
			c.buffer.memory.release(released)
			record.pending.Add(-1)
			c.buffer.memory.effects.Add(-1)
			select {
			case c.buffer.memory.completed <- struct{}{}:
			default:
			}
		}
	}
}

func (c *pulsarReader) Confirm(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.buffer.partitions {
		count := 0
		for _, record := range p.records {
			pending := record.pending.Load()
			if pending < 0 {
				return errors.ErrInternalCheckFailed.FastGenByArgs("Pulsar input completed more than once")
			}
			if pending != 0 {
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
		if err := c.consumer.AckIDCumulative(c.messageIDs[p.records[count-1]]); err != nil {
			return errors.WrapError(errors.ErrInternalCheckFailed, err, "confirm Pulsar messages")
		}
		for _, record := range p.records[:count] {
			size := record.bytes.Load()
			c.buffer.memory.release(size)
			c.buffer.memory.readBytes.Add(-size)
			c.buffer.memory.records.Add(-1)
			delete(c.messageIDs, record)
		}
		copy(p.records, p.records[count:])
		clear(p.records[len(p.records)-count:])
		p.records = p.records[:len(p.records)-count]
		c.buffer.memory.confirmed.Add(int64(count))
	}
	return nil
}

func (c *pulsarReader) BufferedBytes() int64 {
	return c.buffer.memory.readBytes.Load() + int64(len(c.consumer.Chan()))*maxRecordBytes
}

func (c *pulsarReader) Close() error {
	c.consumer.Close()
	c.client.Close()
	return nil
}
