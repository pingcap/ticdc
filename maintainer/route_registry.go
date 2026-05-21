// Copyright 2026 PingCAP, Inc.
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

package maintainer

import (
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/routing"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

func (c *Controller) rebuildTargetTableRegistry(tables []commonEvent.Table) error {
	if c.replicaConfig == nil || c.replicaConfig.Sink == nil {
		c.targetTableRegistry = nil
		return nil
	}
	rules := c.replicaConfig.Sink.DispatchRules
	if len(rules) == 0 {
		c.targetTableRegistry = nil
		return nil
	}

	router, err := routing.NewRouter(c.changefeedID, util.GetOrZero(c.replicaConfig.CaseSensitive), rules)
	if err != nil {
		return err
	}

	registry := routing.NewTargetTableRegistry(c.changefeedID, len(tables))
	for _, table := range tables {
		if table.SchemaTableName == nil {
			return errors.ErrInternalCheckFailed.FastGenByArgs(fmt.Sprintf(
				"schema table name is nil when rebuilding target table registry, tableID: %d",
				table.TableID))
		}
		binding, err := router.Route(table.SchemaTableName.SchemaName, table.SchemaTableName.TableName)
		if err != nil {
			return err
		}
		if err := registry.Add(binding); err != nil {
			return err
		}
	}

	c.targetTableRegistry = registry
	log.Info("target table registry rebuilt",
		zap.Stringer("changefeed", c.changefeedID),
		zap.Int("sourceTableCount", len(tables)))
	return nil
}
