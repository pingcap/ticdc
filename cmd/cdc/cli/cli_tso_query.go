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
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

// queryTsoOptions defines flags for the `cli tso query` command.
type queryTsoOptions struct {
	pdClient pd.Client
}

// newQueryTsoOptions creates new queryTsoOptions for the `cli tso query` command.
func newQueryTsoOptions() *queryTsoOptions {
	return &queryTsoOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *queryTsoOptions) complete(f factory.Factory) error {
	pdClient, err := f.PdClient()
	if err != nil {
		return err
	}
	o.pdClient = pdClient

	return nil
}

// run runs the `cli tso query` command.
func (o *queryTsoOptions) run(cmd *cobra.Command) error {
	ctx := context.Background()

	ts, logic, err := o.pdClient.GetTS(ctx)
	if err != nil {
		return err
	}

	cmd.Println(oracle.ComposeTS(ts, logic))

	return nil
}

// newCmdQueryTso creates the `cli tso query` command.
func newCmdQueryTso(f factory.Factory) *cobra.Command {
	o := newQueryTsoOptions()

	command := &cobra.Command{
		Use:   "query",
		Short: "Get tso from PD (should use --pd to specify PD cluster addresses)",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}

	return command
}
