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
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

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
	if err := command.ExecuteContext(ctx); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
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
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 0 {
				return errors.ErrInvalidReplicaConfig.FastGenByArgs("consumer does not accept positional arguments")
			}
			return nil
		},
		RunE: func(command *cobra.Command, _ []string) error {
			return runConsumer(command.Context(), options)
		},
	}
	command.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return errors.WrapError(errors.ErrInvalidReplicaConfig, err, "parse consumer flags")
	})
	flags := command.Flags()
	flags.StringVar(&options.upstreamURI, "upstream-uri", "", "Kafka, Pulsar, or Storage source URI")
	flags.StringVar(&options.downstreamURI, "downstream-uri", "", "downstream sink URI")
	flags.StringVar(&options.configFile, "config", "", "consumer configuration file")
	flags.StringVar(&options.consumerID, "consumer-id", "", "Kafka group ID or Pulsar subscription name")
	flags.StringVar(&options.timezone, "tz", "System", "consumer time zone")
	flags.StringVar(&options.logFile, "log-file", "cdc_consumer.log", "log file path")
	flags.StringVar(&options.logLevel, "log-level", "info", "log level")
	flags.BoolVar(&options.enableProfiling, "enable-profiling", false, "enable pprof on "+profileAddress)
	return command
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

func validateSourceAddress(source sourceType, uri *url.URL) error {
	switch source {
	case sourceKafka, sourcePulsar:
		if uri.Host == "" {
			return errors.ErrInvalidReplicaConfig.FastGenByArgs(string(source) + " upstream-uri must include an endpoint")
		}
		if strings.Trim(uri.Path, "/") == "" {
			return errors.ErrInvalidReplicaConfig.FastGenByArgs(string(source) + " upstream-uri must include a topic")
		}
	case sourceStorage:
		if uri.Scheme == config.FileScheme {
			if uri.Path == "" {
				return errors.ErrInvalidReplicaConfig.FastGenByArgs("file upstream-uri must include a path")
			}
		} else if uri.Host == "" {
			return errors.ErrInvalidReplicaConfig.FastGenByArgs("object storage upstream-uri must include a bucket")
		}
	}
	return nil
}

func validateTimezone(name string) error {
	switch strings.ToLower(name) {
	case "", "system", "local":
		return nil
	default:
		if _, err := time.LoadLocation(name); err != nil {
			return errors.WrapError(errors.ErrConfigInvalidTimezone, err, name)
		}
		return nil
	}
}

func runConsumer(ctx context.Context, options *options) error {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	upstreamURI, err := parseRequiredURI("upstream-uri", options.upstreamURI)
	if err != nil {
		return err
	}
	source, err := sourceTypeFromURI(upstreamURI)
	if err != nil {
		return err
	}
	if err := validateSourceAddress(source, upstreamURI); err != nil {
		return err
	}
	if _, err := parseRequiredURI("downstream-uri", options.downstreamURI); err != nil {
		return err
	}
	if err := validateTimezone(options.timezone); err != nil {
		return err
	}
	if source != sourceStorage && strings.TrimSpace(options.consumerID) == "" {
		return errors.ErrInvalidReplicaConfig.FastGenByArgs("consumer-id is required for " + string(source) + " sources")
	}
	if options.configFile != "" {
		replicaConfig := config.GetDefaultReplicaConfig()
		if err := cmdutil.StrictDecodeFile(options.configFile, "consumer", replicaConfig); err != nil {
			return errors.WrapError(errors.ErrInvalidReplicaConfig, err, "decode consumer config")
		}
		if _, err := filter.VerifyTableRules(replicaConfig.Filter); err != nil {
			return errors.WrapError(errors.ErrInvalidReplicaConfig, err, "verify consumer filter rules")
		}
	}

	if err := logger.InitLogger(&logger.Config{Level: options.logLevel, File: options.logFile}); err != nil {
		return errors.WrapError(errors.ErrInternalCheckFailed, err, "initialize consumer logger")
	}

	version.LogVersionInfo("consumer")
	log.Info("consumer configuration loaded", zap.String("sourceType", string(source)))

	if options.enableProfiling {
		listener, err := net.Listen("tcp", profileAddress)
		if err != nil {
			return errors.WrapError(errors.ErrInternalCheckFailed, err, "listen for consumer profiling")
		}
		server := &http.Server{Addr: profileAddress, ReadHeaderTimeout: 5 * time.Second}
		wg.Go(func() {
			if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
				log.Error("consumer profiling server exited", zap.Error(err))
			}
		})
		wg.Go(func() {
			<-ctx.Done()
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			if err := server.Shutdown(shutdownCtx); err != nil {
				log.Error("consumer profiling server shutdown failed", zap.Error(err))
				if err := server.Close(); err != nil {
					log.Error("consumer profiling server close failed", zap.Error(err))
				}
			}
		})
	}

	err = errors.ErrInternalCheckFailed.FastGenByArgs(string(source) + " source is not implemented")
	log.Error("consumer exited with error", zap.Error(err))
	return err
}
