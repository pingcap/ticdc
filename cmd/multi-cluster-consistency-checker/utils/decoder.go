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

package utils

import (
	"context"

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/canal"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/csv"
)

func defaultCsvCodecConfig(protocol config.Protocol) *codecCommon.Config {
	codecConfig := codecCommon.NewConfig(protocol)
	codecConfig.Delimiter = config.Comma
	codecConfig.Quote = string(config.DoubleQuoteChar)
	codecConfig.NullString = config.NULL
	codecConfig.IncludeCommitTs = true
	codecConfig.Terminator = config.CRLF
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

func defaultCanalJSONCodecConfig(protocol config.Protocol) *codecCommon.Config {
	codecConfig := codecCommon.NewConfig(protocol)
	// Always enable tidb extension for canal-json protocol
	// because we need to get the commit ts from the extension field.
	codecConfig.EnableTiDBExtension = true
	codecConfig.Terminator = config.CRLF
	return codecConfig
}

type canalJSONDecoder struct {
	codecConfig *codecCommon.Config
}

func NewCanalJSONDecoder() *canalJSONDecoder {
	codecConfig := defaultCanalJSONCodecConfig(config.ProtocolCanalJSON)
	return &canalJSONDecoder{
		codecConfig: codecConfig,
	}
}

func (d *canalJSONDecoder) NewDecoder(ctx context.Context, tableInfo *commonType.TableInfo, content []byte) (codecCommon.Decoder, error) {
	// For S3 sink with canal-json format, use NewTxnDecoder
	// which is designed for batch decoding from storage
	decoder := canal.NewTxnDecoder(d.codecConfig)
	decoder.AddKeyValue(nil, content)
	return decoder, nil
}
