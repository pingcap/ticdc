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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package franz

import (
	"context"

	"github.com/pingcap/ticdc/pkg/common"
)

type Factory struct {
	changefeedID common.ChangeFeedID
	config       Config
}

func NewFactory(config Config, changefeedID common.ChangeFeedID) *Factory {
	return &Factory{changefeedID: changefeedID, config: config}
}

func (f *Factory) Admin(ctx context.Context) (*Admin, error) {
	return NewAdmin(ctx, f.changefeedID, f.config)
}

func (f *Factory) SyncProducer(ctx context.Context) (*SyncProducer, error) {
	producer, err := NewSyncProducer(ctx, f.changefeedID, f.config, newMetricsHook(f.changefeedID))
	if err != nil {
		CleanupMetrics(f.changefeedID)
	}
	return producer, err
}

func (f *Factory) AsyncProducer(ctx context.Context) (*AsyncProducer, error) {
	producer, err := NewAsyncProducer(ctx, f.changefeedID, f.config, newMetricsHook(f.changefeedID))
	if err != nil {
		CleanupMetrics(f.changefeedID)
	}
	return producer, err
}

func (f *Factory) CleanupMetrics() { CleanupMetrics(f.changefeedID) }
