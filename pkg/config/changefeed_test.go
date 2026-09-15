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

package config

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestChangeFeedInfoRmUnusedFieldsKeepsTableRouting(t *testing.T) {
	for _, sinkURI := range []string{"mysql://127.0.0.1:3306", "file:///tmp/cdc"} {
		t.Run(sinkURI, func(t *testing.T) {
			cfg := GetDefaultReplicaConfig()
			cfg.Sink.DispatchRules = []*DispatchRule{
				nil,
				{
					Matcher:        []string{"sales.*", "!sales.tmp"},
					TargetSchema:   "archive",
					TargetTable:    "{schema}_{table}",
					DispatcherRule: "ts",
					PartitionRule:  "index-value",
					IndexName:      "primary",
					Columns:        []string{"id"},
					TopicRule:      "sales-events",
				},
			}
			info := &ChangeFeedInfo{SinkURI: sinkURI, Config: cfg}

			info.RmUnusedFields()

			require.Equal(t, []*DispatchRule{
				nil,
				{
					Matcher:      []string{"sales.*", "!sales.tmp"},
					TargetSchema: "archive",
					TargetTable:  "{schema}_{table}",
				},
			}, info.Config.Sink.DispatchRules)
		})
	}
}
