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

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
)

// EpochBumpOptions carries metadata persisted together with a changefeed epoch bump.
type EpochBumpOptions struct {
	CheckpointTs uint64
	Progress     config.Progress
	// UpdateStatus controls whether CheckpointTs and Progress overwrite the
	// persisted status read by the bump transaction.
	UpdateStatus bool
	State        *config.FeedState
	Error        *config.RunningError
	// UpdateError controls whether Error overwrites the persisted runtime error.
	UpdateError bool
}

// Backend is the metastore for the changefeed
type Backend interface {
	// GetAllChangefeeds returns all changefeeds from the backend db, include stopped and failed changefeeds
	GetAllChangefeeds(ctx context.Context) (map[common.ChangeFeedID]*ChangefeedMetaWrapper, error)
	// GetChangefeedInfo returns the latest persisted changefeed info from the backend db.
	GetChangefeedInfo(ctx context.Context, id common.ChangeFeedID) (*config.ChangeFeedInfo, error)
	// CreateChangefeed saves changefeed info and status to db
	CreateChangefeed(ctx context.Context, info *config.ChangeFeedInfo) error
	// UpdateChangefeed updates changefeed info  to db
	UpdateChangefeed(ctx context.Context, info *config.ChangeFeedInfo, checkpointTs uint64, progress config.Progress) error
	// BumpChangefeedEpoch persists a strictly newer epoch using the latest stored
	// ChangeFeedInfo. It only reads and updates stored status when UpdateStatus is set.
	BumpChangefeedEpoch(ctx context.Context, id common.ChangeFeedID, candidateEpoch uint64, options EpochBumpOptions) (*config.ChangeFeedInfo, error)
	// PauseChangefeed persists the pause status to db for a changefeed
	PauseChangefeed(ctx context.Context, id common.ChangeFeedID) error
	// DeleteChangefeed removes all related info of a changefeed from db
	DeleteChangefeed(ctx context.Context, id common.ChangeFeedID) error
	// SetChangefeedProgress persists the operation progress status to db for a changefeed
	SetChangefeedProgress(ctx context.Context, id common.ChangeFeedID, progress config.Progress) error
	// UpdateChangefeedCheckpointTs persists the checkpointTs for changefeeds
	UpdateChangefeedCheckpointTs(ctx context.Context, checkpointTs map[common.ChangeFeedID]uint64) error
}

// ChangefeedMetaWrapper is a wrapper for the changefeed load from the DB
type ChangefeedMetaWrapper struct {
	Info   *config.ChangeFeedInfo
	Status *config.ChangeFeedStatus
}
