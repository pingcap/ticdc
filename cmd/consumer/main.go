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
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/log"
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
	switch {
	case err != nil:
		log.Error("consumer exited with error", zap.Error(err))
		_, _ = fmt.Fprintln(os.Stderr, err)
	case ctx.Err() != nil:
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c, err := newConsumer(ctx, options)
	if err != nil {
		return err
	}
	defer func() {
		cancel()
		if stopErr := c.stop(); stopErr != nil {
			err = stopErr
		}
	}()
	return c.consume(ctx)
}
