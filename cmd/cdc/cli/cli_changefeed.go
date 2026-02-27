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

package cli

import (
	"os"

	"github.com/pingcap/ticdc/cmd/cdc/factory"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/spf13/cobra"
)

// newCmdChangefeed creates the `cli changefeed` command.
func newCmdChangefeed(f factory.Factory) *cobra.Command {
	cmds := &cobra.Command{
		Use:   "changefeed",
		Short: "Manage changefeed (changefeed is a replication task)",
		Args:  cobra.NoArgs,
	}
	cmds.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		if err := util.CheckKeyspaceFlag(cmd); err != nil {
			cmd.PrintErr(err)
			os.Exit(1)
		}
	}

	cmds.AddCommand(newCmdCreateChangefeed(f))
	cmds.AddCommand(newCmdUpdateChangefeed(f))
	cmds.AddCommand(newCmdStatisticsChangefeed(f))
	cmds.AddCommand(newCmdListChangefeed(f))
	cmds.AddCommand(newCmdPauseChangefeed(f))
	cmds.AddCommand(newCmdQueryChangefeed(f))
	cmds.AddCommand(newCmdRemoveChangefeed(f))
	cmds.AddCommand(newCmdResumeChangefeed(f))
	cmds.AddCommand(newCmdMoveTable(f))
	cmds.AddCommand(newCmdMoveSplitTable(f))
	cmds.AddCommand(newCmdSplitTableByRegionCount(f))
	cmds.AddCommand(newCmdMergeTable(f))

	return cmds
}
