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
	"os"

	"github.com/pingcap/ticdc/cmd/cdc/cli"
	"github.com/pingcap/ticdc/cmd/cdc/server"
	"github.com/pingcap/ticdc/cmd/cdc/version"
	"github.com/pingcap/tidb/pkg/util/collate"
	tiflowRedo "github.com/pingcap/tiflow/pkg/cmd/redo"
	"github.com/spf13/cobra"
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

// Run runs the root command.
func main() {
	cmd := NewCmd()

	cmd.SetOut(os.Stdout)
	cmd.SetErr(os.Stderr)

	cmd.AddCommand(server.NewCmdServer())
	cmd.AddCommand(cli.NewCmdCli())
	cmd.AddCommand(version.NewCmdVersion())
	cmd.AddCommand(tiflowRedo.NewCmdRedo())

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
