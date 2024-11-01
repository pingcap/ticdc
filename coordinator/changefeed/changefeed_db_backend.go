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

package changefeed

import (
	"context"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tiflow/cdc/model"
)

// Backend is the metastore for the changefeed
type Backend interface {
	// GetAllChangefeeds returns all changefeeds from the backend db, include stopped and failed changefeeds
	GetAllChangefeeds(ctx context.Context) (map[model.ChangeFeedID]*ChangefeedMetaWrapper, error)
	// CreateChangefeed saves changefeed info and status to db
	CreateChangefeed(ctx context.Context, info *config.ChangeFeedInfo) error
	// UpdateChangefeed updates changefeed info  to db
	UpdateChangefeed(ctx context.Context, info *config.ChangeFeedInfo) error
	// PauseChangefeed persists the pause status to db for a changefeed
	PauseChangefeed(ctx context.Context, id model.ChangeFeedID) error
	// DeleteChangefeed removes all related info of a changefeed from db
	DeleteChangefeed(ctx context.Context, id model.ChangeFeedID) error
	// ResumeChangefeed persists the resumed status to db for a changefeed
	ResumeChangefeed(ctx context.Context, id model.ChangeFeedID, newCheckpointTs uint64) error
	// UpdateChangefeedCheckpointTs persists the checkpoints for changefeeds
	UpdateChangefeedCheckpointTs(ctx context.Context, cps map[model.ChangeFeedID]uint64) error
}

// ChangefeedMetaWrapper is a wrapper for the changefeed load from the DB
type ChangefeedMetaWrapper struct {
	Info   *config.ChangeFeedInfo
	Status *model.ChangeFeedStatus
}
