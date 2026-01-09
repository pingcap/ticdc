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
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	cfgPath string
)

const (
	ExitCodeExecuteFailed      = 1
	ExitCodeInvalidConfig      = 2
	ExitCodeDecodeConfigFailed = 3
)

const (
	FlagConfig = "config"
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

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(ExitCodeExecuteFailed)
	}
}

func run(cmd *cobra.Command, args []string) {
	if cfgPath == "" {
		fmt.Fprintln(os.Stderr, "error: --config flag is required")
		os.Exit(ExitCodeInvalidConfig)
	}

	cfg, err := loadConfig(cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(ExitCodeDecodeConfigFailed)
	}

	fmt.Printf("Loaded configuration with %d cluster(s)\n", len(cfg.Clusters))
	for name, cluster := range cfg.Clusters {
		fmt.Printf("  Cluster: %s\n", name)
		fmt.Printf("    PD Address: %s\n", cluster.PDAddr)
		fmt.Printf("    CDC Address: %s\n", cluster.CDCAddr)
		fmt.Printf("    S3 Sink URI: %s\n", cluster.S3SinkURI)
	}

	// TODO: Implement actual consistency checking logic
	fmt.Println("\nConsistency checking logic will be implemented here")
}
