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

package cli

import (
	"context"

	"github.com/pingcap/ticdc/cmd/cdc/factory"
	"github.com/pingcap/ticdc/cmd/util"
	apiv2client "github.com/pingcap/ticdc/pkg/api/v2"
	"github.com/spf13/cobra"
)

// moveTableChangefeedOptions defines common flags for the `cli changefeed move table` command.
type moveSplitTableChangefeedOptions struct {
	apiClientV2 apiv2client.APIV2Interface

	changefeedID string
	namespace    string
	tableId      int64
	targetNodeID string
}

// newCreateChangefeedOptions creates new options for the `cli changefeed create` command.
func newMoveSplitTableChangefeedOptions() *moveSplitTableChangefeedOptions {
	return &moveSplitTableChangefeedOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *moveSplitTableChangefeedOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&o.namespace, "namespace", "n", "default", "Replication task (changefeed) Namespace")
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmd.PersistentFlags().Int64VarP(&o.tableId, "table-id", "t", 0, "the id of table to move")
	cmd.PersistentFlags().StringVarP(&o.targetNodeID, "target-node-id", "d", "", "the dest for the table to move")
	_ = cmd.MarkPersistentFlagRequired("changefeed-id")
	_ = cmd.MarkPersistentFlagRequired("table-id")
	_ = cmd.MarkPersistentFlagRequired("target-node-id")
}

// complete adapts from the command line args to the data and client required.
func (o *moveSplitTableChangefeedOptions) complete(f factory.Factory) error {
	clientV2, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.apiClientV2 = clientV2
	return nil
}

// run the `cli changefeed move table` command.
// return success or error message.
func (o *moveSplitTableChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := context.Background()

	err := o.apiClientV2.Changefeeds().MoveSplitTable(ctx, o.namespace, o.changefeedID, o.tableId, o.targetNodeID)
	var errStr string
	if err != nil {
		errStr = err.Error()
	}
	response := &response{
		Success: err == nil,
		Error:   errStr,
	}
	return util.JSONPrint(cmd, response)
}

// newCmdMoveSplitTable creates the `cli changefeed move split table` command.
// `cli changefeed move split table` command is just for inner test use, not public use.
func newCmdMoveSplitTable(f factory.Factory) *cobra.Command {
	o := newMoveSplitTableChangefeedOptions()

	command := &cobra.Command{
		Use:   "move-split-table",
		Short: "move split table in a changefeed",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}

	o.addFlags(command)

	return command
}
