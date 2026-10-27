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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const profileAddress = "127.0.0.1:6060"

type options struct {
	upstreamURI     string
	downstreamURI   string
	configFile      string
	consumerID      string
	timezone        string
	logFile         string
	logLevel        string
	ca, cert, key   string
	enableProfiling bool

	schemaRegistryURI      string
	upstreamTiDBDSN        string
	oauth2                 config.OAuth2
	authTLSCertificatePath string
	authTLSPrivateKeyPath  string
	fileIndexWidth         int
	flushInterval          time.Duration
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := newCommand().ExecuteContext(ctx); err != nil {
		log.Error("consumer command failed", zap.Error(err))
	}
}

func newCommand() *cobra.Command {
	options := &options{}
	command := &cobra.Command{
		Use:           "cdc_consumer",
		Short:         "Consume TiCDC events and write them to a downstream sink",
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return start(command.Context(), options)
		},
	}
	command.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return errors.WrapError(errors.ErrInvalidReplicaConfig, err, "parse consumer flags")
	})
	flags := command.Flags()
	flags.StringVar(&options.upstreamURI, "upstream-uri", "", "Kafka, Pulsar, or Storage source URI")
	flags.StringVar(&options.downstreamURI, "downstream-uri", "", "downstream sink URI")
	flags.StringVar(&options.configFile, "config", "", "consumer configuration file")
	flags.StringVar(&options.consumerID, "consumer-id", "", "Kafka group ID (random if omitted) or Pulsar subscription name (default pulsar-test-subscription)")
	flags.StringVar(&options.consumerID, "consumer-group-id", "", "Kafka consumer group ID")
	flags.StringVar(&options.consumerID, "subscription-name", "", "Pulsar subscription name (default pulsar-test-subscription)")
	flags.StringVar(&options.schemaRegistryURI, "schema-registry-uri", "", "Avro schema registry URI")
	flags.StringVar(&options.upstreamTiDBDSN, "upstream-tidb-dsn", "", "upstream TiDB DSN")
	flags.StringVar(&options.timezone, "tz", "System", "consumer time zone")
	flags.StringVar(&options.logFile, "log-file", "cdc_consumer.log", "log file path")
	flags.StringVar(&options.logLevel, "log-level", "info", "log level")
	flags.StringVar(&options.ca, "ca", "", "CA certificate path for Kafka or Pulsar TLS connection")
	flags.StringVar(&options.cert, "cert", "", "Client certificate path for Kafka or Pulsar TLS connection")
	flags.StringVar(&options.key, "key", "", "Client private key path for Kafka or Pulsar TLS connection")
	flags.StringVar(&options.oauth2.OAuth2PrivateKey, "oauth2-private-key", "", "Pulsar OAuth2 private key path")
	flags.StringVar(&options.oauth2.OAuth2IssuerURL, "oauth2-issuer-url", "", "Pulsar OAuth2 issuer URL")
	flags.StringVar(&options.oauth2.OAuth2ClientID, "oauth2-client-id", "", "Pulsar OAuth2 client ID")
	flags.StringVar(&options.oauth2.OAuth2Scope, "oauth2-scope", "", "Pulsar OAuth2 scope")
	flags.StringVar(&options.oauth2.OAuth2Audience, "oauth2-audience", "", "Pulsar OAuth2 audience")
	flags.StringVar(&options.authTLSCertificatePath, "auth-tls-certificate-path", "", "Pulsar authentication certificate path")
	flags.StringVar(&options.authTLSPrivateKeyPath, "auth-tls-private-key-path", "", "Pulsar authentication private key path")
	flags.IntVar(&options.fileIndexWidth, "file-index-width", config.DefaultFileIndexWidth, "Storage file index width")
	flags.DurationVar(&options.flushInterval, "flush-interval", storageScanInterval, "Storage scan interval")
	flags.BoolVar(&options.enableProfiling, "enable-profiling", false, "enable pprof on "+profileAddress)
	return command
}

func start(ctx context.Context, options *options) error {
	ctx, cancel := context.WithCancel(ctx)
	c := newConsumer(ctx, options)
	err := c.consume(ctx)
	cancel()
	c.stop()
	return err
}
