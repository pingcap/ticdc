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

package errors

import (
	"strings"

	"github.com/pingcap/kvproto/pkg/pdpb"
)

// IsKeyspaceNotExistError returns true the error is caused by keyspace not exists.
func IsKeyspaceNotExistError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), pdpb.ErrorType_ENTRY_NOT_FOUND.String())
}

func IsGCBarrierTSBehindTxnSafePointError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "ErrGCBarrierTSBehindTxnSafePoint")
}
