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

package parser

import (
	"context"

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/csv"
)

func defaultCsvCodecConfig(protocol config.Protocol) *codecCommon.Config {
	codecConfig := codecCommon.NewConfig(protocol)
	codecConfig.Delimiter = ","
	codecConfig.Quote = "\""
	codecConfig.NullString = "NULL"
	codecConfig.IncludeCommitTs = true
	codecConfig.Terminator = "\r\n"
	return codecConfig
}

type csvDecoder struct {
	codecConfig *codecCommon.Config
}

func NewCsvDecoder() *csvDecoder {
	codecConfig := defaultCsvCodecConfig(config.ProtocolCsv)
	return &csvDecoder{
		codecConfig: codecConfig,
	}
}

func (d *csvDecoder) NewDecoder(ctx context.Context, tableInfo *commonType.TableInfo, content []byte) (codecCommon.Decoder, error) {
	decoder, err := csv.NewDecoder(ctx, d.codecConfig, tableInfo, content)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return decoder, nil
}
