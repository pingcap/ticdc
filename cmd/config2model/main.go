// Copyright 2019 PingCAP, Inc.
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
	"encoding/json"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	v2 "github.com/pingcap/ticdc/api/v2"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/spf13/cobra"
)

var (
	cfgPath string
)

const (
	ExitExecuteFailed = 255 - iota
	ExitCodeNoFilePath
	ExitCodeDecodeTomlFailed
	ExitCodeMarshalJson
)

const (
	FlagConfig = "config"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "config2model -c [path]",
		Short: "A tool to convert config from toml to json",
		Run:   runConvert,
	}
	rootCmd.Flags().StringVarP(&cfgPath, FlagConfig, "c", "", "changefeed config file path")
	rootCmd.MarkFlagRequired(FlagConfig)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(ExitExecuteFailed)
	}
}

func runConvert(cmd *cobra.Command, args []string) {
	if cfgPath == "" {
		fmt.Fprintln(os.Stderr, "please specify the config file path")
		os.Exit(ExitCodeNoFilePath)
		return
	}

	cfg := &config.ReplicaConfig{}
	_, err := toml.DecodeFile(cfgPath, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "decode config file error: %v\n", err)
		os.Exit(ExitCodeDecodeTomlFailed)
		return
	}

	model := v2.ToAPIReplicaConfig(cfg)

	data, err := json.MarshalIndent(model, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal config error: %v\n", err)
		os.Exit(ExitCodeMarshalJson)
		return
	}
	fmt.Printf("%s\n", data)
}
