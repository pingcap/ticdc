// Copyright 2021 PingCAP, Inc.
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
	"github.com/pingcap/ticdc/cmd/cdc/factory"
	"github.com/spf13/cobra"
)

// newCmdCapture creates the `cli capture` command.
func newCmdCapture(f factory.Factory) *cobra.Command {
	cmds := &cobra.Command{
		Use:   "capture",
		Short: "Manage capture (capture is a CDC server instance)",
		Args:  cobra.NoArgs,
	}
	cmds.AddCommand(
		newCmdListCapture(f),
		// TODO: add resign owner command
	)

	return cmds
}
