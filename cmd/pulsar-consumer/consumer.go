// Copyright 2025 PingCAP, Inc.
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

package main

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/auth"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	tpulsar "github.com/pingcap/ticdc/pkg/sink/pulsar"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type consumer struct {
	pulsarConsumer pulsar.Consumer
	client         pulsar.Client
	writer         *writer
}

// newConsumer creates a pulsar consumer
func newConsumer(ctx context.Context, option *option) *consumer {
	var pulsarURL string
	if len(option.ca) != 0 {
		pulsarURL = "pulsar+ssl" + "://" + option.address[0]
	} else {
		pulsarURL = "pulsar" + "://" + option.address[0]
	}
	topicName := option.topic
	subscriptionName := "pulsar-test-subscription"

	clientOption := pulsar.ClientOptions{
		URL:    pulsarURL,
		Logger: tpulsar.NewPulsarLogger(log.L()),
	}
	if len(option.ca) != 0 {
		clientOption.TLSTrustCertsFilePath = option.ca
		clientOption.TLSCertificateFile = option.cert
		clientOption.TLSKeyFilePath = option.key
	}

	var authentication pulsar.Authentication
	if len(option.oauth2PrivateKey) != 0 {
		authentication = pulsar.NewAuthenticationOAuth2(map[string]string{
			auth.ConfigParamIssuerURL: option.oauth2IssuerURL,
			auth.ConfigParamAudience:  option.oauth2Audience,
			auth.ConfigParamKeyFile:   option.oauth2PrivateKey,
			auth.ConfigParamClientID:  option.oauth2ClientID,
			auth.ConfigParamScope:     option.oauth2Scope,
			auth.ConfigParamType:      auth.ConfigParamTypeClientCredentials,
		})
		log.Info("oauth2 authentication is enabled", zap.String("issuerUrl", option.oauth2IssuerURL))
		clientOption.Authentication = authentication
	}
	if len(option.mtlsAuthTLSCertificatePath) != 0 {
		authentication = pulsar.NewAuthenticationTLS(option.mtlsAuthTLSCertificatePath, option.mtlsAuthTLSPrivateKeyPath)
		log.Info("mtls authentication is enabled",
			zap.String("cert", option.mtlsAuthTLSCertificatePath),
			zap.String("key", option.mtlsAuthTLSPrivateKeyPath),
		)
		clientOption.Authentication = authentication
	}

	client, err := pulsar.NewClient(clientOption)
	if err != nil {
		log.Fatal("can't create pulsar client", zap.Error(err))
	}

	consumerConfig := pulsar.ConsumerOptions{
		Topic:                       topicName,
		SubscriptionName:            subscriptionName,
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	}

	c, err := client.Subscribe(consumerConfig)
	if err != nil {
		log.Fatal("can't create pulsar consumer", zap.Error(err))
	}
	return &consumer{
		pulsarConsumer: c,
		client:         client,
		writer:         newWriter(ctx, option),
	}
}

func (c *consumer) readMessage(ctx context.Context) error {
	msgChan := c.pulsarConsumer.Chan()
	defer func() {
		c.pulsarConsumer.Close()
		c.client.Close()
	}()
	for {
		select {
		case <-ctx.Done():
			log.Info("terminating: context cancelled")
			return errors.Trace(ctx.Err())
		case consumerMsg := <-msgChan:
			log.Debug("Received message", zap.Stringer("msgId", consumerMsg.ID()), zap.ByteString("content", consumerMsg.Payload()))
			needCommit := c.writer.WriteMessage(ctx, consumerMsg)
			if !needCommit {
				continue
			}
			err := c.pulsarConsumer.AckID(consumerMsg.Message.ID())
			if err != nil {
				log.Panic("Error ack message", zap.Error(err))
			}
		}
	}
}

// Run the consumer, read data and write to the downstream target.
func (c *consumer) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return c.writer.run(ctx)
	})
	g.Go(func() error {
		return c.readMessage(ctx)
	})
	return g.Wait()
}
