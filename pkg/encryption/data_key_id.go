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

import "github.com/pingcap/errors"

// DataKeyID represents a 3-byte data key identifier in the encryption header.
type DataKeyID [3]byte

// ToString converts DataKeyID to string.
func (id DataKeyID) ToString() string {
	return string(id[:])
}

// DataKeyIDFromString creates DataKeyID from string (must be 3 bytes).
func DataKeyIDFromString(s string) (DataKeyID, error) {
	if len(s) != 3 {
		return DataKeyID{}, errors.New("data key ID must be exactly 3 bytes")
	}
	var id DataKeyID
	copy(id[:], s)
	return id, nil
}
