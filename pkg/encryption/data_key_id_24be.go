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

import cerrors "github.com/pingcap/ticdc/pkg/errors"

func encodeDataKeyID24BE(id uint32) (string, error) {
	if id > 0xFFFFFF {
		return "", cerrors.ErrInvalidDataKeyID.GenWithStackByArgs("data key ID exceeds 24-bit range")
	}
	b := [3]byte{byte(id >> 16), byte(id >> 8), byte(id)}
	return string(b[:]), nil
}

func decodeDataKeyID24BE(id string) (uint32, error) {
	if len(id) != 3 {
		return 0, cerrors.ErrInvalidDataKeyID.GenWithStackByArgs("data key ID must be 3 bytes")
	}
	b := []byte(id)
	return uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2]), nil
}
