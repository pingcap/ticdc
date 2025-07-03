// Copyright 2025 PingCAP, Inc.
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
	cfgPath   string
	modelPath string
)

const (
	ExitCodeExecuteFailed = 1 + iota
	ExitCodeDecodeTomlFailed
	ExitCodeMarshalJson
	ExitCodeDecodeJsonFailed
	ExitCodeMarshalTomlFailed
	ExitCodeInvalidFlag
)

const (
	FlagConfig = "config"
	FlagModel  = "model"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "config-converter",
		Short: "A tool to convert between config and model",
		Run:   run,
	}
	// If FlagConfig and FlagModel are both set, FlagModel will be ignored
	rootCmd.Flags().StringVarP(&cfgPath, FlagConfig, "c", "", "changefeed config file path, convert to model")
	rootCmd.Flags().StringVarP(&modelPath, FlagModel, "m", "", "changefeed model file path, convert to config, if both config and model are set, model will be ignored")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(ExitCodeExecuteFailed)
	}
}

func run(cmd *cobra.Command, args []string) {
	if cfgPath != "" && modelPath != "" {
		fmt.Fprintln(os.Stderr, "can't specify both config and model")
		os.Exit(ExitCodeInvalidFlag)
	}

	if cfgPath != "" {
		runConfig2Model()
		return
	}

	if modelPath != "" {
		runModel2Config()
		return
	}

	fmt.Fprintln(os.Stderr, "must specify either config or model")
	os.Exit(ExitCodeInvalidFlag)
}

func runConfig2Model() {
	cfg := &config.ReplicaConfig{}
	_, err := toml.DecodeFile(cfgPath, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "decode config file error: %v\n", err)
		os.Exit(ExitCodeDecodeTomlFailed)
	}

	model := v2.ToAPIReplicaConfig(cfg)

	data, err := json.MarshalIndent(model, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal config error: %v\n", err)
		os.Exit(ExitCodeMarshalJson)
	}
	fmt.Printf("%s\n", data)
}

func runModel2Config() {
	data, err := os.ReadFile(modelPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read model file error: %v\n", err)
		os.Exit(ExitCodeExecuteFailed)
	}
	model := &v2.ReplicaConfig{}
	err = json.Unmarshal(data, model)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unmarshal model error: %v\n", err)
		os.Exit(ExitCodeDecodeJsonFailed)
	}

	cfg := model.ToInternalReplicaConfig()

	err = toml.NewEncoder(os.Stdout).Encode(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal config error: %v\n ", err)
		os.Exit(ExitCodeMarshalTomlFailed)
	}
}
