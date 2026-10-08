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
	"math"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/auth"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	pulsarutil "github.com/pingcap/ticdc/pkg/sink/pulsar"
	putil "github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

type pulsarWatermark struct {
	watermark uint64
	record    *messageRecord
}

type pulsarConsumer struct {
	client            pulsar.Client
	consumer          pulsar.Consumer
	writer            *eventWriter
	partitionIDs      map[string]int32
	messageIDs        map[*messageRecord]pulsar.MessageID
	pendingWatermarks []pulsarWatermark
	watermark         uint64
	hasWatermark      bool
}

func runPulsarConsumer(ctx context.Context, wg *sync.WaitGroup, upstreamURI *url.URL, downstreamURI, consumerID, timezone string, replicaConfig *config.ReplicaConfig) error {
	parentCtx := ctx
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	writeCtx, cancelWrite := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancelWrite(nil)
	wg.Go(func() {
		select {
		case <-parentCtx.Done():
			log.Info("consumer stopping", zap.Duration("shutdownTimeout", shutdownTimeout))
		case <-writeCtx.Done():
			return
		}
		shutdownCtx, cancelShutdown := context.WithTimeout(writeCtx, shutdownTimeout)
		defer cancelShutdown()
		<-shutdownCtx.Done()
		if shutdownCtx.Err() == context.DeadlineExceeded {
			cancelWrite(errors.ErrInternalCheckFailed.FastGenByArgs("consumer shutdown drain timed out"))
		}
	})
	consumer, err := newPulsarConsumer(ctx, writeCtx, upstreamURI, downstreamURI, consumerID, timezone, replicaConfig)
	if err != nil {
		return err
	}
	defer consumer.client.Close()
	defer consumer.consumer.Close()
	defer consumer.writer.downstream.Close()
	sinkDone := make(chan bool)
	defer func() {
		cancelWrite(nil)
		<-sinkDone
	}()
	wg.Go(func() {
		defer close(sinkDone)
		if err := consumer.writer.downstream.Run(writeCtx); err != nil {
			cancelWrite(err)
			cancel(err)
			return
		}
		if writeCtx.Err() == nil {
			err := errors.ErrInternalCheckFailed.FastGenByArgs("downstream sink stopped unexpectedly")
			cancelWrite(err)
			cancel(err)
		}
	})
	err = consumer.run(ctx, writeCtx)
	if writeErr := context.Cause(writeCtx); writeErr != nil {
		return writeErr
	}
	if parentCtx.Err() == nil || !errors.Is(err, context.Canceled) {
		return err
	}
	drainCtx, cancelDrain := context.WithTimeout(writeCtx, shutdownTimeout)
	defer cancelDrain()
	if consumer.hasWatermark {
		if err := consumer.writer.flushReady(drainCtx, drainCtx, consumer.watermark, true); err != nil {
			return err
		}
	}
	for len(consumer.writer.inFlight) != 0 {
		if err := consumer.writer.waitBatch(drainCtx, consumer.writer.inFlight[0]); err != nil {
			return err
		}
	}
	if err := consumer.confirmCompleted(drainCtx); err != nil {
		return err
	}
	return context.Cause(writeCtx)
}

func newPulsarConsumer(ctx, writeCtx context.Context, upstreamURI *url.URL, downstreamURI, consumerID, timezone string, replicaConfig *config.ReplicaConfig) (*pulsarConsumer, error) {
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
	codecConfig := codeccommon.NewConfig(protocol)
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
	if err := codecConfig.Validate(); err != nil {
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
	if len(topics) == 0 || len(topics) > maxSchemas {
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
	partitions := make(map[int32]*messagePartition, len(topics))
	partitionIDs := make(map[string]int32, len(topics))
	schemas := make(map[schemaKey]bool)
	schemaPointers := make(map[*common.TableInfo]bool)
	for index, partitionTopic := range topics {
		partitionIDs[partitionTopic] = int32(index)
		partitions[int32(index)] = &messagePartition{decoder: decoder, schemas: schemas, schemaPointers: schemaPointers}
	}
	replicaConfig.Sink.TiDBSourceID = 1
	changefeedID := common.NewChangeFeedIDWithName("consumer", common.DefaultKeyspaceName)
	downstream, err := sink.New(writeCtx, &config.ChangefeedConfig{
		ChangefeedID: changefeedID, SinkURI: downstreamURI, SinkConfig: replicaConfig.Sink,
		CaseSensitive: putil.GetOrZero(replicaConfig.CaseSensitive), EnableTableAcrossNodes: putil.GetOrZero(replicaConfig.Scheduler.EnableTableAcrossNodes),
	}, changefeedID, common.DefaultKeyspaceID)
	if err != nil {
		consumer.Close()
		client.Close()
		return nil, err
	}
	writer := &eventWriter{downstream: downstream, protocol: protocol, partitions: partitions, ddls: make(map[ddlKey]*pendingDDL), mutations: make(map[mutationKey]*mutation)}
	c := &pulsarConsumer{client: client, consumer: consumer, writer: writer, partitionIDs: partitionIDs, messageIDs: make(map[*messageRecord]pulsar.MessageID)}
	writer.confirm = c.confirmCompleted
	log.Info("Pulsar consumer initialized", zap.String("topic", topic), zap.Int("partitionCount", len(partitions)))
	return c, nil
}

func (c *pulsarConsumer) run(ctx, writeCtx context.Context) error {
	tick := time.Tick(batchLinger)
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if err := c.writer.finishBatches(); err != nil {
			return err
		}
		if err := c.confirmCompleted(ctx); err != nil {
			return err
		}
		if time.Since(c.writer.lastProgressLog) >= progressLogInterval {
			c.writer.lastProgressLog = time.Now()
			log.Info("consumer progress", zap.Uint64("watermark", c.watermark), zap.Bool("hasWatermark", c.hasWatermark),
				zap.Int64("receivedInputs", c.writer.receivedInputs), zap.Int64("decodedRows", c.writer.decodedRows),
				zap.Int64("writtenRows", c.writer.writtenRows), zap.Int64("completedInputs", c.writer.completedInputs),
				zap.Int("pendingDMLCount", len(c.writer.pendingDML)), zap.Int("pendingDDLCount", len(c.writer.pendingDDL)),
				zap.Int("inFlightBatches", len(c.writer.inFlight)), zap.Int64("inFlightBytes", c.writer.inFlightBytes),
				zap.Int("uncompletedInputs", c.writer.recordCount), zap.Int64("bufferedBytes", c.writer.bufferedBytes()))
		}
		idle := false
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-tick:
			idle = true
		case message, ok := <-c.consumer.Chan():
			if !ok {
				return errors.ErrInternalCheckFailed.FastGenByArgs("Pulsar consumer stopped unexpectedly")
			}
			if err := c.processMessage(message.Message); err != nil {
				return err
			}
			idle = len(c.consumer.Chan()) == 0
		}
		if err := c.writer.flushReady(ctx, writeCtx, c.watermark, idle || c.writer.bufferedBytes() >= memoryHighWater); err != nil {
			return err
		}
		if c.hasWatermark {
			c.writer.advanceReplay(c.watermark)
		}
		if err := c.confirmCompleted(ctx); err != nil {
			return err
		}
	}
}

func (c *pulsarConsumer) processMessage(message pulsar.Message) error {
	partitionID, ok := c.partitionIDs[message.Topic()]
	if !ok {
		return errors.ErrInternalCheckFailed.FastGenByArgs("Pulsar message belongs to an unknown partition")
	}
	bytes := int64(len(message.Key()) + len(message.Payload()) + len(message.ID().Serialize()) + 256)
	if bytes > maxRecordBytes || c.writer.recordCount >= maxRecords || c.writer.bufferedBytes()+bytes > maxBufferedBytes {
		return errors.ErrInternalCheckFailed.FastGenByArgs("consumer input exceeds its buffer limit; Pulsar message remains unconfirmed")
	}
	state := &messageRecord{partition: partitionID, bytes: bytes}
	c.messageIDs[state] = message.ID()
	c.writer.inputBytes += bytes
	c.writer.recordCount++
	c.writer.receivedInputs++
	partition := c.writer.partitions[partitionID]
	partition.records = append(partition.records, state)
	partition.decoder.AddKeyValue([]byte(message.Key()), message.Payload())
	for {
		messageType, hasNext := partition.decoder.HasNext()
		if !hasNext {
			break
		}
		switch messageType {
		case codeccommon.MessageTypeRow:
			message := partition.decoder.NextDMLMessage()
			if message == nil {
				return errors.ErrCodecDecode.FastGenByArgs("Pulsar decoder returned an empty DML message")
			}
			state.remaining++
			c.writer.effectCount++
			if err := c.writer.queueDML(message, state, nil); err != nil {
				return err
			}
		case codeccommon.MessageTypeDDL:
			ddl := partition.decoder.NextDDLEvent()
			if ddl == nil || ddl.Query == "" {
				return errors.ErrCodecDecode.FastGenByArgs("Pulsar decoder returned an empty DDL event")
			}
			key := schemaKey{schema: ddl.SchemaName, table: ddl.TableName, version: ddl.FinishedTs}
			if !partition.schemas[key] {
				if c.writer.schemaCount >= maxSchemas || c.writer.schemaBytes+128 > maxSchemaBytes {
					return errors.ErrInternalCheckFailed.FastGenByArgs("consumer schema cache exceeds its resource limit; Pulsar message remains unconfirmed")
				}
				partition.schemas[key] = true
				c.writer.schemaCount++
				c.writer.schemaBytes += 128
			}
			// Pulsar has one logical control stream; its physical placement
			// does not select the DDL that will be submitted to the sink.
			if err := c.writer.queueDDL(ddl, state, 0, true); err != nil {
				return err
			}
		case codeccommon.MessageTypeResolved:
			watermark := partition.decoder.NextResolvedEvent()
			state.remaining++
			c.writer.effectCount++
			c.writer.controlBytes += 32
			c.watermark = max(c.watermark, watermark)
			c.hasWatermark = true
			c.pendingWatermarks = append(c.pendingWatermarks, pulsarWatermark{watermark: watermark, record: state})
		default:
			return errors.ErrCodecDecode.FastGenByArgs("Pulsar decoder returned an unknown message type")
		}
		if c.writer.effectCount > maxEffects || c.writer.bufferedBytes() > maxBufferedBytes {
			return errors.ErrInternalCheckFailed.FastGenByArgs("consumer decoded input exceeds its buffer limit; Pulsar message remains unconfirmed")
		}
	}
	state.complete = state.remaining == 0
	return nil
}

func (c *pulsarConsumer) confirmCompleted(ctx context.Context) error {
	unfinished := uint64(math.MaxUint64)
	for _, item := range c.writer.pendingDML {
		unfinished = min(unfinished, item.event.CommitTs)
	}
	for _, batch := range c.writer.inFlight {
		for _, item := range batch.items {
			unfinished = min(unfinished, item.event.CommitTs)
		}
	}
	for _, ddl := range c.writer.pendingDDL {
		unfinished = min(unfinished, ddl.key.commitTs)
	}
	remaining := c.pendingWatermarks[:0]
	hasUnfinished := len(c.writer.pendingDML) != 0 || len(c.writer.inFlight) != 0 || len(c.writer.pendingDDL) != 0
	for _, control := range c.pendingWatermarks {
		if control.watermark > c.watermark || (hasUnfinished && control.watermark >= unfinished) {
			remaining = append(remaining, control)
			continue
		}
		if err := c.writer.finishRecordEffect(control.record); err != nil {
			return err
		}
		c.writer.controlBytes -= 32
	}
	clear(c.pendingWatermarks[len(remaining):])
	c.pendingWatermarks = remaining
	for _, partition := range c.writer.partitions {
		count := 0
		for _, record := range partition.records {
			if !record.complete {
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
		id := c.messageIDs[partition.records[count-1]]
		if err := c.consumer.AckIDCumulative(id); err != nil {
			return errors.WrapError(errors.ErrInternalCheckFailed, err, "confirm Pulsar messages")
		}
		for _, record := range partition.records[:count] {
			c.writer.inputBytes -= record.bytes
			delete(c.messageIDs, record)
		}
		c.writer.recordCount -= count
		c.writer.completedInputs += int64(count)
		copy(partition.records, partition.records[count:])
		clear(partition.records[len(partition.records)-count:])
		partition.records = partition.records[:len(partition.records)-count]
	}
	return nil
}
