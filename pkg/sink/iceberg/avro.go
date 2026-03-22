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

package iceberg

import (
	"bytes"

	"github.com/linkedin/goavro/v2"
)

func wrapUnion(avroType string, value any) any {
	if value == nil {
		return nil
	}
	return map[string]any{avroType: value}
}

func writeOCF(schema string, meta map[string][]byte, compressionName string, records []any) ([]byte, error) {
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(nil)
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               buf,
		Codec:           codec,
		CompressionName: compressionName,
		MetaData:        meta,
	})
	if err != nil {
		return nil, err
	}

	if len(records) > 0 {
		if err := writer.Append(records); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
