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

package kafka

import (
	"context"
	"strings"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
)

// NewFactory selects a Kafka client implementation based on options.
func NewFactory(
	ctx context.Context,
	o *options,
	changefeedID common.ChangeFeedID,
) (Factory, error) {
	switch strings.ToLower(strings.TrimSpace(o.KafkaClient)) {
	case "", "franz":
		return NewFranzFactory(ctx, o, changefeedID)
	case "sarama":
		return NewSaramaFactory(ctx, o, changefeedID)
	default:
		return nil, errors.ErrKafkaInvalidConfig.GenWithStack("unsupported kafka client %s", o.KafkaClient)
	}
}
