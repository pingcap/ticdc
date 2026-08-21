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

package kafka

import (
	"context"

	"github.com/pingcap/ticdc/pkg/common"
)

// NewFactory selects the Kafka client for one changefeed without automatic fallback.
func NewFactory(ctx context.Context, o *options, changefeedID common.ChangeFeedID) (Factory, error) {
	if o.Client == KafkaClientSarama {
		return NewSaramaFactory(ctx, o, changefeedID)
	}
	return NewFranzFactory(ctx, o, changefeedID)
}

// CleanupFactoryMetrics removes metrics owned directly by a client factory.
func CleanupFactoryMetrics(factory Factory) {
	if cleaner, ok := factory.(interface{ CleanupMetrics() }); ok {
		cleaner.CleanupMetrics()
	}
}
