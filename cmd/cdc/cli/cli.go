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
	"context"
	"github.com/pingcap/ticdc/cmd/cdc/factory"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/spf13/cobra"
	"os"
)

// NewCmdCli creates the `cli` command.
func NewCmdCli() *cobra.Command {
	// Bind the certificate and log options.
	cf := factory.NewClientFlags()

	cmds := &cobra.Command{
		Use:   "cli",
		Short: "Manage replication task and TiCDC cluster",
		Args:  cobra.NoArgs,
	}

	// Binding the `cli` command flags.
	cf.AddFlags(cmds)
	cmds.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		// Here we will initialize the logging configuration and set the current default context.
		err := logger.InitLogger(&logger.Config{Level: cf.GetLogLevel()})
		if err != nil {
			cmd.Printf("init logger error %v\n", errors.Trace(err))
			os.Exit(1)
		}
		_, cancel := context.WithCancel(context.Background())
		defer cancel()
		util.LogHTTPProxies()
		// A notify that complete immediately, it skips the second signal essentially.
		doneNotify := func() <-chan struct{} {
			done := make(chan struct{})
			close(done)
			return done
		}
		util.InitSignalHandling(doneNotify, cancel)

		util.CheckErr(cf.CompleteClientAuthParameters(cmd))
	}

	// Construct the client construction factory.
	f := factory.NewFactory(cf)

	// Add subcommands.
	cmds.AddCommand(newCmdChangefeed(f))
	cmds.AddCommand(newCmdCapture(f))
	cmds.AddCommand(newCmdTso(f))
	cmds.AddCommand(newCmdUnsafe(f))

	return cmds
}
