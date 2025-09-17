// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	upstreamURIStr string
	configFile     string
	consumerOption = newConsumerOption()
)

func main() {
	debug.SetMemoryLimit(14 * 1024 * 1024 * 1024)
	cmd := &cobra.Command{
		Use: "pulsar consumer",
		Run: run,
	}
	// Flags for the root command
	cmd.Flags().StringVar(&configFile, "config", "", "config file for changefeed")
	cmd.Flags().StringVar(&upstreamURIStr, "upstream-uri", "", "pulsar uri")
	cmd.Flags().StringVar(&consumerOption.downstreamURI, "downstream-uri", "", "downstream sink uri")
	cmd.Flags().StringVar(&consumerOption.timezone, "tz", "System", "Specify time zone of pulsar consumer")
	cmd.Flags().StringVar(&consumerOption.ca, "ca", "", "CA certificate path for pulsar SSL connection")
	cmd.Flags().StringVar(&consumerOption.cert, "cert", "", "Certificate path for pulsar SSL connection")
	cmd.Flags().StringVar(&consumerOption.key, "key", "", "Private key path for pulsar SSL connection")
	cmd.Flags().StringVar(&consumerOption.logPath, "log-file", "cdc_pulsar_consumer.log", "log file path")
	cmd.Flags().StringVar(&consumerOption.logLevel, "log-level", "info", "log file path")
	cmd.Flags().StringVar(&consumerOption.oauth2PrivateKey, "oauth2-private-key", "", "oauth2 private key path")
	cmd.Flags().StringVar(&consumerOption.oauth2IssuerURL, "oauth2-issuer-url", "", "oauth2 issuer url")
	cmd.Flags().StringVar(&consumerOption.oauth2ClientID, "oauth2-client-id", "", "oauth2 client id")
	cmd.Flags().StringVar(&consumerOption.oauth2Audience, "oauth2-scope", "", "oauth2 scope")
	cmd.Flags().StringVar(&consumerOption.oauth2Audience, "oauth2-audience", "", "oauth2 audience")
	cmd.Flags().StringVar(&consumerOption.mtlsAuthTLSCertificatePath, "auth-tls-certificate-path", "", "mtls certificate path")
	cmd.Flags().StringVar(&consumerOption.mtlsAuthTLSPrivateKeyPath, "auth-tls-private-key-path", "", "mtls private key path")
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
	}
}

func run(_ *cobra.Command, _ []string) {
	err := logger.InitLogger(&logger.Config{
		Level: consumerOption.logLevel,
		File:  consumerOption.logPath,
	})
	if err != nil {
		log.Error("init logger failed", zap.Error(err))
		return
	}

	version.LogVersionInfo("pulsar consumer")

	upstreamURI, err := url.Parse(upstreamURIStr)
	if err != nil {
		log.Panic("invalid upstream-uri", zap.Error(err))
	}
	scheme := strings.ToLower(upstreamURI.Scheme)
	if !config.IsPulsarScheme(scheme) {
		log.Panic("invalid upstream-uri scheme, the scheme of upstream-uri must be pulsar schema",
			zap.String("schema", scheme),
			zap.String("upstreamURI", upstreamURIStr))
	}

	consumerOption.Adjust(upstreamURI, configFile)

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	consumer := newConsumer(ctx, consumerOption)
	g.Go(func() error {
		return consumer.Run(ctx)
	})

	g.Go(func() error {
		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-ctx.Done():
			log.Info("terminating: context cancelled")
		case <-sigterm:
			log.Info("terminating: via signal")
		}
		cancel()
		return nil
	})
	err = g.Wait()
	if err != nil {
		log.Error("pulsar consumer exited with error", zap.Error(err))
	} else {
		log.Info("pulsar consumer exited")
	}
}
