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
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	cmdutil "github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/logger"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const profileAddress = "127.0.0.1:6060"

type sourceType string

const (
	sourceKafka   sourceType = "kafka"
	sourcePulsar  sourceType = "pulsar"
	sourceStorage sourceType = "storage"
)

type options struct {
	upstreamURI     string
	downstreamURI   string
	configFile      string
	consumerID      string
	timezone        string
	logFile         string
	logLevel        string
	enableProfiling bool
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	command := newCommand()
	err := command.ExecuteContext(ctx)
	if ctx.Err() != nil && errors.Is(err, context.Canceled) {
		err = nil
	}
	if err != nil {
		log.Error("consumer exited with error", zap.Error(err))
		_, _ = fmt.Fprintln(os.Stderr, err)
	} else if ctx.Err() != nil {
		log.Info("consumer stopped")
	}
	_ = log.Sync()
	if err != nil {
		os.Exit(1)
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
	flags.StringVar(&options.consumerID, "consumer-id", "", "Kafka group ID or Pulsar subscription name (randomly generated if omitted)")
	flags.StringVar(&options.timezone, "tz", "System", "consumer time zone")
	flags.StringVar(&options.logFile, "log-file", "cdc_consumer.log", "log file path")
	flags.StringVar(&options.logLevel, "log-level", "info", "log level")
	flags.BoolVar(&options.enableProfiling, "enable-profiling", false, "enable pprof on "+profileAddress)
	return command
}

func start(ctx context.Context, options *options) (err error) {
	if err := logger.InitLogger(&logger.Config{Level: options.logLevel, File: options.logFile}); err != nil {
		return errors.WrapError(errors.ErrInternalCheckFailed, err, "initialize consumer logger")
	}
	version.LogVersionInfo("consumer")

	upstreamURI, err := parseRequiredURI("upstream-uri", options.upstreamURI)
	if err != nil {
		return err
	}
	source, err := sourceTypeFromURI(upstreamURI)
	if err != nil {
		return err
	}
	if _, err := parseRequiredURI("downstream-uri", options.downstreamURI); err != nil {
		return err
	}
	consumerID := options.consumerID
	if source != sourceStorage && strings.TrimSpace(consumerID) == "" {
		consumerID = "ticdc_consumer_" + uuid.NewString()
	}
	replicaConfig := config.GetDefaultReplicaConfig()
	if options.configFile != "" {
		if err := cmdutil.StrictDecodeFile(options.configFile, "consumer", replicaConfig); err != nil {
			return errors.WrapError(errors.ErrInvalidReplicaConfig, err, "decode consumer config")
		}
		if _, err := filter.VerifyTableRules(replicaConfig.Filter); err != nil {
			return errors.WrapError(errors.ErrInvalidReplicaConfig, err, "verify consumer filter rules")
		}
	}
	log.Info("consumer configuration loaded", zap.String("sourceType", string(source)), zap.String("consumerID", consumerID))

	ctx, cancel := context.WithCancelCause(ctx)
	c, err := newConsumer(ctx, upstreamURI, options.downstreamURI, consumerID, options.timezone, replicaConfig)
	if err != nil {
		cancel(err)
		return cmp.Or(context.Cause(ctx), err)
	}
	var profileServer *http.Server
	defer func() {
		err = c.stop(cancel, profileServer, err)
	}()
	if options.enableProfiling {
		listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", profileAddress)
		if err != nil {
			return errors.WrapError(errors.ErrInternalCheckFailed, err, "listen for consumer profiling")
		}
		mux := http.NewServeMux()
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
		profileServer = &http.Server{Addr: profileAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		c.wg.Go(func() {
			if err := profileServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				cancel(errors.WrapError(errors.ErrInternalCheckFailed, err, "serve consumer profiling"))
			}
		})
	}
	results := make(chan *writeEvent, 64)
	c.wg.Go(func() {
		defer close(results)
		cancel(c.read(ctx, results))
	})
	c.wg.Go(func() {
		if err := c.writer.downstream.Run(ctx); err != nil {
			cancel(err)
			return
		}
		if ctx.Err() == nil {
			cancel(errors.ErrInternalCheckFailed.FastGenByArgs("downstream sink stopped unexpectedly"))
		}
	})
	err = c.write(ctx, results)
	return cmp.Or(context.Cause(ctx), err)
}

func parseRequiredURI(name, rawURI string) (*url.URL, error) {
	if rawURI == "" {
		return nil, errors.ErrInvalidReplicaConfig.FastGenByArgs(name + " is required")
	}
	uri, err := url.Parse(rawURI)
	if err != nil {
		return nil, errors.WrapError(errors.ErrInvalidReplicaConfig, putil.MaskSensitiveDataInURLError(err), "invalid "+name)
	}
	uri.Scheme = strings.ToLower(uri.Scheme)
	if uri.Scheme == "" {
		return nil, errors.ErrInvalidReplicaConfig.FastGenByArgs(name + " must include a URI scheme")
	}
	return uri, nil
}

func sourceTypeFromURI(uri *url.URL) (sourceType, error) {
	switch uri.Scheme {
	case config.KafkaScheme, config.KafkaSSLScheme:
		return sourceKafka, nil
	case config.PulsarScheme, config.PulsarSSLScheme, config.PulsarHTTPScheme, config.PulsarHTTPSScheme:
		return sourcePulsar, nil
	case config.FileScheme, config.S3Scheme, config.GCSScheme, config.GSScheme,
		config.AzblobScheme, config.AzureScheme:
		return sourceStorage, nil
	default:
		return "", errors.ErrInvalidReplicaConfig.FastGenByArgs("unsupported upstream-uri scheme " + uri.Scheme)
	}
}
