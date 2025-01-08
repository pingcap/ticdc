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

package codec

import (
	"context"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/canal"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/encoder"
	"github.com/pingcap/ticdc/pkg/sink/codec/open"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

func NewEventEncoder(ctx context.Context, cfg *common.Config) (encoder.EventEncoder, error) {
	switch cfg.Protocol {
	case config.ProtocolDefault, config.ProtocolOpen:
		return open.NewBatchEncoder(ctx, cfg)
	// case config.ProtocolAvro:
	// 	return avro.NewAvroEncoder(ctx, cfg)
	case config.ProtocolCanalJSON:
		return canal.NewJSONRowEventEncoder(ctx, cfg)
	// case config.ProtocolDebezium:
	// 	return debezium.NewBatchEncoder(cfg, config.GetGlobalServerConfig().ClusterID), nil
	// case config.ProtocolSimple:
	// 	return simple.NewEncoder(ctx, cfg)
	default:
		return nil, cerror.ErrSinkUnknownProtocol.GenWithStackByArgs(cfg.Protocol)
	}
}
