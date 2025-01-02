// Copyright 2024 PingCAP, Inc.
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
	"os"
	"slices"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/cli"
	"github.com/pingcap/ticdc/cmd/server"
	"github.com/pingcap/ticdc/cmd/version"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tidb/pkg/util/collate"
	tiflowCmd "github.com/pingcap/tiflow/pkg/cmd"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// NewCmd creates the root command.
func NewCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cdc",
		Short: "CDC",
		Long:  `Change Data Capture`,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}
}

func addNewArchCommandTo(cmd *cobra.Command) {
	cmd.AddCommand(server.NewCmdServer())
	cmd.AddCommand(cli.NewCmdCli())
	cmd.AddCommand(version.NewCmdVersion())
}

func isNewArchEnabledByConfig(serverConfigFilePath string) bool {

	cfg := config.GetDefaultServerConfig()
	if len(serverConfigFilePath) > 0 {
		// strict decode config file, but ignore debug item
		if err := util.StrictDecodeFile(serverConfigFilePath, "TiCDC server", cfg, config.DebugConfigurationItem); err != nil {
			log.Error("failed to parse server configuration, please check the config file for errors and try again.", zap.Error(err))
			return false
		}
	}

	return cfg.Newarch
}

// Utility to remove a flag from os.Args
func removeFlagFromArgs(flag string) []string {
	result := []string{os.Args[0]} // keep the command name
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] != flag {
			result = append(result, os.Args[i])
		}
	}
	return result
}

func parseConfigFlagFromOSArgs() string {
	var serverConfigFilePath string
	for i, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "--config=") {
			serverConfigFilePath = strings.SplitN(arg, "=", 2)[1]
		} else if arg == "--config" && i+2 < len(os.Args) {
			serverConfigFilePath = os.Args[i+2]
		}
	}

	// If the command is `cdc cli changefeed`, means it's not a server config file.
	if slices.Contains(os.Args, "cli") && slices.Contains(os.Args, "changefeed") {
		serverConfigFilePath = ""
	}

	return serverConfigFilePath
}

func parseNewarchFlagFromOSArgs() bool {
	newarch := false
	for _, arg := range os.Args[1:] {
		if arg == "--newarch" {
			newarch = true
			os.Args = removeFlagFromArgs("--newarch")
		} else if arg == "-x" {
			newarch = true
			os.Args = removeFlagFromArgs("-x")
		}
	}
	return newarch
}

// Run runs the root command.
func main() {
	cmd := NewCmd()

	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)

	newarch := false
	var serverConfigFilePath string
	cmd.PersistentFlags().BoolVarP(&newarch, "newarch", "x", false, "Run the new architecture of TiCDC (experimental feature)")
	cmd.ParseFlags(os.Args[1:])

	// Double check to aviod some corner cases
	serverConfigFilePath = parseConfigFlagFromOSArgs()
	newarch = parseNewarchFlagFromOSArgs() || (os.Getenv("TICDC_NEWARCH") == "true")

	if newarch || isNewArchEnabledByConfig(serverConfigFilePath) {
		cmd.Println("=== Command to ticdc(new arch).")
		addNewArchCommandTo(cmd)
	} else {
		cmd.Println("=== Command to ticdc(tiflow).")
		tiflowCmd.AddTiCDCCommandTo(cmd)
	}

	setNewCollationEnabled()
	if err := cmd.Execute(); err != nil {
		cmd.PrintErrln(err)
		os.Exit(1)
	}
}

// When the upstream doesn't enable new collation and there is a table with cluster index,
// tidb will not encode the pk column in the value part.
// So we will rely on the function `tablecodec.DecodeHandleToDatumMap` to decode pk value from the key.
// But this function only works when the global variable `newCollationEnabled` in tidb package is set to false.
//
// Previouly, this global variable is set to false in tidb package,
// but it was removed as described in https://github.com/pingcap/tidb/pull/52191#issuecomment-2024836481.
// So we need to manully set it to false here.
func setNewCollationEnabled() {
	collate.SetNewCollationEnabledForTest(false)
}
