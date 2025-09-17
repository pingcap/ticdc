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
type splitTableByRegionCountChangefeedOptions struct {
	apiClientV2 apiv2client.APIV2Interface

	changefeedID string
	keyspace     string
	tableId      int64
	mode         int64
}

// newCreateChangefeedOptions creates new options for the `cli changefeed create` command.
func newSplitTableByRegionCountChangefeedOptions() *splitTableByRegionCountChangefeedOptions {
	return &splitTableByRegionCountChangefeedOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *splitTableByRegionCountChangefeedOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&o.keyspace, "keyspace", "k", "default", "Replication task (changefeed) Keyspace")
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmd.PersistentFlags().Int64VarP(&o.tableId, "table-id", "t", 0, "the id of table to move")
	cmd.PersistentFlags().Int64Var(&o.mode, "mode", 0, "enable redo when mode is 1")
	_ = cmd.MarkPersistentFlagRequired("changefeed-id")
	_ = cmd.MarkPersistentFlagRequired("table-id")
}

// complete adapts from the command line args to the data and client required.
func (o *splitTableByRegionCountChangefeedOptions) complete(f factory.Factory) error {
	clientV2, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.apiClientV2 = clientV2
	return nil
}

// run the `cli changefeed move table` command.
// return success or error message.
func (o *splitTableByRegionCountChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := context.Background()

	err := o.apiClientV2.Changefeeds().SplitTableByRegionCount(ctx, o.keyspace, o.changefeedID, o.tableId, o.mode)
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

// newCmdsplitTableByRegionCount creates the `cli changefeed move split table` command.
// `cli changefeed move split table` command is just for inner test use, not public use.
func newCmdSplitTableByRegionCount(f factory.Factory) *cobra.Command {
	o := newSplitTableByRegionCountChangefeedOptions()

	command := &cobra.Command{
		Use:   "split-table-by-region-count",
		Short: "split table by region count in a changefeed",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}

	o.addFlags(command)

	return command
}
