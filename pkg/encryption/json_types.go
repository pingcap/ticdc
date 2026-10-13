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

package encryption

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// ByteArray supports decoding either:
// - a JSON string (base64-encoded bytes), or
// - a JSON array of uint8 values (TiKV status API style).
type ByteArray []byte

func (b *ByteArray) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || string(data) == "null" {
		*b = nil
		return nil
	}

	switch data[0] {
	case '"':
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		decoded, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return err
		}
		*b = decoded
		return nil
	case '[':
		var ints []int
		if err := json.Unmarshal(data, &ints); err != nil {
			return err
		}
		out := make([]byte, len(ints))
		for i, v := range ints {
			if v < 0 || v > 255 {
				return fmt.Errorf("byte value out of range: %d", v)
			}
			out[i] = byte(v)
		}
		*b = out
		return nil
	default:
		return fmt.Errorf("unsupported JSON type for bytes: %s", string(data))
	}
}
