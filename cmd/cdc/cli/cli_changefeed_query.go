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
	"bytes"
	"encoding/json"

	"github.com/BurntSushi/toml"
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

// cfMeta holds changefeed info and changefeed status for JSON output.
type cfMeta struct {
	UpstreamID     uint64                     `json:"upstream_id"`
	ID             string                     `json:"id"`
	Keyspace       string                     `json:"keyspace"`
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

// cfMetaSimplifiedTOML holds simplified changefeed info for TOML output.
type cfMetaSimplifiedTOML struct {
	UpstreamID     uint64               `toml:"upstream-id"`
	ID             string               `toml:"id"`
	Keyspace       string               `toml:"keyspace"`
	FeedState      config.FeedState     `toml:"state"`
	CheckpointTSO  uint64               `toml:"checkpoint-tso"`
	CheckpointTime api.JSONTime         `toml:"checkpoint-time"`
	RunningError   *config.RunningError `toml:"error,omitempty"`
}

// cfMetaTOML holds changefeed info for TOML output. Config is interface{}
// because it holds a map[string]interface{} produced by BurntSushi/toml
// encoding (which correctly handles time.Duration as human-readable strings).
type cfMetaTOML struct {
	UpstreamID     uint64                     `toml:"upstream-id"`
	ID             string                     `toml:"id"`
	Keyspace       string                     `toml:"keyspace"`
	SinkURI        string                     `toml:"sink-uri"`
	Config         interface{}                `toml:"config"`
	CreateTime     api.JSONTime               `toml:"create-time"`
	StartTs        uint64                     `toml:"start-ts"`
	ResolvedTs     uint64                     `toml:"resolved-ts"`
	TargetTs       uint64                     `toml:"target-ts"`
	CheckpointTSO  uint64                     `toml:"checkpoint-tso"`
	CheckpointTime api.JSONTime               `toml:"checkpoint-time"`
	Engine         config.SortEngine          `toml:"sort-engine,omitempty"`
	FeedState      config.FeedState           `toml:"state"`
	RunningError   *config.RunningError       `toml:"error,omitempty"`
	ErrorHis       []int64                    `toml:"error-history,omitempty"`
	CreatorVersion string                     `toml:"creator-version"`
	TaskStatus     []config.CaptureTaskStatus `toml:"task-status,omitempty"`
}

// queryChangefeedOptions defines flags for the `cli changefeed query` command.
type queryChangefeedOptions struct {
	apiClientV2  apiv2client.APIV2Interface
	changefeedID string
	simplified   bool
	keyspace     string
	outputFormat string
}

// newQueryChangefeedOptions creates new options for the `cli changefeed query` command.
func newQueryChangefeedOptions() *queryChangefeedOptions {
	return &queryChangefeedOptions{
		outputFormat: "json",
	}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *queryChangefeedOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&o.keyspace, "keyspace", "k", "default", "Replication task (changefeed) Keyspace")
	cmd.PersistentFlags().BoolVarP(&o.simplified, "simple", "s", false, "Output simplified replication status")
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmd.PersistentFlags().StringVarP(&o.outputFormat, "output", "o", "json", "Output format (json|toml)")
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
	ctx := cmd.Context()

	format, err := util.ParseOutputFormat(o.outputFormat)
	if err != nil {
		return err
	}

	if o.simplified {
		infos, err := o.apiClientV2.Changefeeds().List(ctx, o.keyspace, "all")
		if err != nil {
			return errors.Trace(err)
		}
		for _, info := range infos {
			if info.ID == o.changefeedID {
				if format == util.OutputFormatTOML {
					simplified := &cfMetaSimplifiedTOML{
						UpstreamID:     info.UpstreamID,
						ID:             info.ID,
						Keyspace:       info.Keyspace,
						FeedState:      info.FeedState,
						CheckpointTSO:  info.CheckpointTSO,
						CheckpointTime: info.CheckpointTime,
						RunningError:   info.RunningError,
					}
					return util.TOMLPrint(cmd, simplified)
				}
				return util.JSONPrint(cmd, info)
			}
		}
		return cerror.ErrChangeFeedNotExists.GenWithStackByArgs(o.changefeedID)
	}

	detail, err := o.apiClientV2.Changefeeds().Get(ctx, o.keyspace, o.changefeedID)
	if err != nil {
		return err
	}

	if format == util.OutputFormatTOML {
		cfgMap, err := configToMap(detail.Config, format)
		if err != nil {
			return errors.Annotate(err, "marshal changefeed config")
		}
		meta := &cfMetaTOML{
			UpstreamID:     detail.UpstreamID,
			ID:             detail.ID,
			Keyspace:       detail.Keyspace,
			SinkURI:        detail.SinkURI,
			Config:         cfgMap,
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
		return util.TOMLPrint(cmd, meta)
	}

	meta := &cfMeta{
		UpstreamID:     detail.UpstreamID,
		ID:             detail.ID,
		Keyspace:       detail.Keyspace,
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

// configToMap serializes a ReplicaConfig to a map[string]interface{} using
// either JSON or TOML encoding. The encoding determines the map's key naming
// convention: JSON produces snake_case keys, TOML produces kebab-case keys.
//
// The TOML path converts through the internal config.ReplicaConfig (which has
// toml struct tags) and encodes with BurntSushi/toml, keeping TOML-specific
// concerns out of the API model.
func configToMap(cfg *v2.ReplicaConfig, format util.OutputFormat) (map[string]interface{}, error) {
	if cfg == nil {
		return nil, nil
	}
	if format == util.OutputFormatTOML {
		internalCfg := cfg.ToInternalReplicaConfig()
		var buf bytes.Buffer
		if err := toml.NewEncoder(&buf).Encode(internalCfg); err != nil {
			return nil, errors.Trace(err)
		}
		var m map[string]interface{}
		if _, err := toml.NewDecoder(&buf).Decode(&m); err != nil {
			return nil, errors.Trace(err)
		}
		return m, nil
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, errors.Trace(err)
	}
	return m, nil
}
