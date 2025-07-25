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

	"github.com/pingcap/errors"
	v2 "github.com/pingcap/ticdc/api/v2"
	"github.com/pingcap/ticdc/cmd/cdc/factory"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/api"
	apiv2client "github.com/pingcap/ticdc/pkg/api/v2"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/spf13/cobra"
)

// cfMeta holds changefeed info and changefeed status.
type cfMeta struct {
	UpstreamID     uint64                     `json:"upstream_id"`
	ID             string                     `json:"id"`
	Namespace      string                     `json:"namespace"`
	SinkURI        string                     `json:"sink_uri"`
	Config         *v2.ReplicaConfig          `json:"config"`
	CreateTime     api.JSONTime               `json:"create_time"`
	StartTs        uint64                     `json:"start_ts"`
	ResolvedTs     uint64                     `json:"resolved_ts"`
	TargetTs       uint64                     `json:"target_ts"`
	CheckpointTSO  uint64                     `json:"checkpoint_tso"`
	CheckpointTime api.JSONTime               `json:"checkpoint_time"`
	Engine         config.SortEngine          `json:"sort_engine,omitempty"`
	FeedState      config.FeedState           `json:"state"`
	RunningError   *config.RunningError       `json:"error,omitempty"`
	ErrorHis       []int64                    `json:"error_history,omitempty"`
	CreatorVersion string                     `json:"creator_version"`
	TaskStatus     []config.CaptureTaskStatus `json:"task_status,omitempty"`
}

// queryChangefeedOptions defines flags for the `cli changefeed query` command.
type queryChangefeedOptions struct {
	apiClientV2  apiv2client.APIV2Interface
	changefeedID string
	simplified   bool
	namespace    string
}

// newQueryChangefeedOptions creates new options for the `cli changefeed query` command.
func newQueryChangefeedOptions() *queryChangefeedOptions {
	return &queryChangefeedOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *queryChangefeedOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&o.namespace, "namespace", "n", "default", "Replication task (changefeed) Namespace")
	cmd.PersistentFlags().BoolVarP(&o.simplified, "simple", "s", false, "Output simplified replication status")
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	_ = cmd.MarkPersistentFlagRequired("changefeed-id")
}

// complete adapts from the command line args to the data and client required.
func (o *queryChangefeedOptions) complete(f factory.Factory) error {
	clientV2, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.apiClientV2 = clientV2
	return nil
}

// run the `cli changefeed query` command.
func (o *queryChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := context.Background()
	if o.simplified {
		infos, err := o.apiClientV2.Changefeeds().List(ctx, o.namespace, "all")
		if err != nil {
			return errors.Trace(err)
		}
		for _, info := range infos {
			if info.ID == o.changefeedID {
				return util.JSONPrint(cmd, info)
			}
		}
		return cerror.ErrChangeFeedNotExists.GenWithStackByArgs(o.changefeedID)
	}

	detail, err := o.apiClientV2.Changefeeds().Get(ctx, o.namespace, o.changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return err
	}
	meta := &cfMeta{
		UpstreamID:     detail.UpstreamID,
		ID:             detail.ID,
		Namespace:      detail.Namespace,
		SinkURI:        detail.SinkURI,
		Config:         detail.Config,
		CreateTime:     api.JSONTime(detail.CreateTime),
		StartTs:        detail.StartTs,
		ResolvedTs:     detail.ResolvedTs,
		TargetTs:       detail.TargetTs,
		CheckpointTSO:  detail.CheckpointTs,
		CheckpointTime: detail.CheckpointTime,
		FeedState:      detail.State,
		RunningError:   detail.Error,
		CreatorVersion: detail.CreatorVersion,
		TaskStatus:     detail.TaskStatus,
	}
	return util.JSONPrint(cmd, meta)
}

// newCmdQueryChangefeed creates the `cli changefeed query` command.
func newCmdQueryChangefeed(f factory.Factory) *cobra.Command {
	o := newQueryChangefeedOptions()

	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a replication task (changefeed)",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}

	o.addFlags(command)

	return command
}
