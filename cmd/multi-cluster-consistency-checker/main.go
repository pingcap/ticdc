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
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	cfgPath string
	dryRun  bool
)

// Exit codes for multi-cluster-consistency-checker.
//
//	0 – clean shutdown (normal exit or graceful signal handling)
//	1 – transient error, safe to restart (network, I/O, temporary failures)
//	2 – invalid configuration (missing required flags / fields)
//	3 – configuration decode failure (malformed config file)
//	4 – checkpoint corruption, requires manual intervention
//	5 – unrecoverable internal error
const (
	ExitCodeTransient            = 1
	ExitCodeInvalidConfig        = 2
	ExitCodeDecodeConfigFailed   = 3
	ExitCodeCheckpointCorruption = 4
	ExitCodeUnrecoverable        = 5
)

// ExitError wraps an error with a process exit code so that callers higher in
// the stack can translate domain errors into the correct exit status.
type ExitError struct {
	Code int
	Err  error
}

func (e *ExitError) Error() string { return e.Err.Error() }
func (e *ExitError) Unwrap() error { return e.Err }

// exitCodeFromError extracts the exit code from an error.
// If the error is an *ExitError the embedded code is returned;
// otherwise the fallback code is returned.
func exitCodeFromError(err error, fallback int) int {
	var ee *ExitError
	if errors.As(err, &ee) {
		return ee.Code
	}
	return fallback
}

const (
	FlagConfig = "config"
	FlagDryRun = "dry-run"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "multi-cluster-consistency-checker",
		Short: "A tool to check consistency across multiple TiCDC clusters",
		Long:  "A tool to check consistency across multiple TiCDC clusters by comparing data from different clusters' S3 sink locations",
		Run:   run,
	}

	rootCmd.Flags().StringVarP(&cfgPath, FlagConfig, "c", "", "configuration file path (required)")
	rootCmd.MarkFlagRequired(FlagConfig)
	rootCmd.Flags().BoolVar(&dryRun, FlagDryRun, false, "validate config and connectivity without running the checker")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(ExitCodeUnrecoverable)
	}
}

func run(cmd *cobra.Command, args []string) {
	if cfgPath == "" {
		fmt.Fprintln(os.Stderr, "error: --config flag is required")
		os.Exit(ExitCodeInvalidConfig)
	}

	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(ExitCodeDecodeConfigFailed)
	}

	// Initialize logger with configured log level
	logLevel := cfg.GlobalConfig.LogLevel
	if logLevel == "" {
		logLevel = "info" // default log level
	}
	loggerConfig := &logger.Config{
		Level: logLevel,
	}
	err = logger.InitLogger(loggerConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init logger: %v\n", err)
		os.Exit(ExitCodeUnrecoverable)
	}
	log.Info("Logger initialized", zap.String("level", logLevel))

	// Create a context that can be cancelled by signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start the task in a goroutine
	errChan := make(chan error, 1)
	go func() {
		err := runTask(ctx, cfg, dryRun)
		if err != nil {
			log.Error("task error", zap.Error(err))
		}
		errChan <- err
	}()

	// Wait for either a signal or task completion
	select {
	case sig := <-sigChan:
		fmt.Fprintf(os.Stdout, "\nReceived signal: %v, shutting down gracefully...\n", sig)
		cancel()
		// Wait for the task to finish
		if err := <-errChan; err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(os.Stderr, "task error during shutdown: %v\n", err)
			code := exitCodeFromError(err, ExitCodeTransient)
			os.Exit(code)
		}
		fmt.Fprintf(os.Stdout, "Shutdown complete\n")
	case err := <-errChan:
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to run task: %v\n", err)
			code := exitCodeFromError(err, ExitCodeTransient)
			os.Exit(code)
		}
	}
}
