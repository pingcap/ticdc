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
	"context"
	"strings"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/pingcap/ticdc/pkg/version"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type consumer struct {
	reader    sourceReader
	decoder   *decodeGroup
	conductor *conductor
	writer    *writer
}

func newConsumer(ctx context.Context, options *options) *consumer {
	if err := logger.InitLogger(&logger.Config{
		Level: "info",
		File:  options.logFile,
	}); err != nil {
		log.Panic("consumer logger initialization failed", zap.Error(err))
	}
	version.LogVersionInfo("consumer")

	upstreamURI := parseRequiredURI(options.upstreamURI)
	sourceType := parseSourceType(upstreamURI)

	consumerID := options.consumerID
	if strings.TrimSpace(consumerID) == "" {
		consumerID = "consumer_" + uuid.NewString()
	}
	replicaConfig := loadReplicaConfig(options.configFile, upstreamURI)
	codecConfig := newCodecConfig(upstreamURI, replicaConfig)
	logConsumerConfig(sourceType, consumerID, upstreamURI, options.downstreamURI, replicaConfig, codecConfig)

	reader := newSourceReader(ctx, sourceOptions{
		UpstreamURI:   upstreamURI,
		ConsumerID:    consumerID,
		ReplicaConfig: replicaConfig,
		CodecConfig:   codecConfig,
	})
	c := &consumer{
		reader:    reader,
		decoder:   newDecodeGroup(ctx, decodeOptions{CodecConfig: codecConfig, ReplicaConfig: replicaConfig, Streams: reader.Streams(), UpstreamURI: upstreamURI}),
		conductor: newConductor(reader),
		writer: newWriter(ctx, writerOptions{
			DownstreamURI: options.downstreamURI,
			ReplicaConfig: replicaConfig,
			CodecConfig:   codecConfig,
		}),
	}
	c.conductor.OnCheckpoint(c.decoder.ReleaseSchemas)
	return c
}

func (c *consumer) start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return c.readInputs(ctx)
	})
	g.Go(func() error {
		return c.decoder.Run(ctx)
	})
	g.Go(func() error {
		return c.conductor.Start(ctx, c.decoder.Output(), c.writer)
	})
	g.Go(func() error {
		return c.writer.Start(ctx)
	})
	err := g.Wait()
	c.reader.Close()
	c.decoder.Close()
	return err
}

func (c *consumer) readInputs(ctx context.Context) error {
	for {
		inputs, err := c.reader.Read(ctx)
		if err != nil {
			return err
		}
		if err := c.decoder.AddEvents(ctx, inputs); err != nil {
			return err
		}
	}
}
