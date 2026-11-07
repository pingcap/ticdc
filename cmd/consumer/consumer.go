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
	"net/http"
	_ "net/http/pprof" //nolint:gosec // pprof is served on 127.0.0.1 only and gated by --enable-profiling.
	"strings"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/consumer/budget"
	"github.com/pingcap/ticdc/cmd/consumer/conductor"
	"github.com/pingcap/ticdc/cmd/consumer/decode"
	"github.com/pingcap/ticdc/cmd/consumer/source"
	"github.com/pingcap/ticdc/cmd/consumer/writer"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/pingcap/ticdc/pkg/version"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type consumer struct {
	reader        source.Reader
	decoder       *decode.Group
	conductor     *conductor.Conductor
	writer        *writer.Writer
	profileServer *http.Server
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
	sourceType := source.TypeOf(upstreamURI)

	consumerID := options.consumerID
	if strings.TrimSpace(consumerID) == "" {
		consumerID = "consumer_" + uuid.NewString()
	}
	replicaConfig := loadReplicaConfig(options.configFile, upstreamURI)
	codecConfig := newCodecConfig(upstreamURI, replicaConfig)
	logConsumerConfig(sourceType, consumerID, upstreamURI, options.downstreamURI, replicaConfig, codecConfig)

	memory := budget.New()
	reader := source.Open(ctx, source.Options{
		UpstreamURI:   upstreamURI,
		ConsumerID:    consumerID,
		ReplicaConfig: replicaConfig,
		CodecConfig:   codecConfig,
		Budget:        memory,
	})
	c := &consumer{
		reader:    reader,
		decoder:   decode.NewGroup(ctx, decode.Options{CodecConfig: codecConfig, ReplicaConfig: replicaConfig, Streams: reader.Streams(), UpstreamURI: upstreamURI, Budget: memory}),
		conductor: conductor.New(memory, reader),
		writer: writer.New(ctx, writer.Options{
			DownstreamURI: options.downstreamURI,
			ReplicaConfig: replicaConfig,
			CodecConfig:   codecConfig,
			Budget:        memory,
		}),
		profileServer: newProfileServer(options.enableProfiling),
	}
	c.conductor.OnCheckpoint(c.decoder.ReleaseSchemas)
	return c
}

func (c *consumer) start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	if c.profileServer != nil {
		g.Go(c.serveProfile)
		g.Go(func() error {
			return c.stopProfile(ctx)
		})
	}
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
