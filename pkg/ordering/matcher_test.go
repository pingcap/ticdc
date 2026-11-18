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

package ordering

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestTxnOrderMatcher(t *testing.T) {
	for _, caseSensitive := range []bool{false, true} {
		matcher, err := NewTxnOrderMatcher([]string{"APP.orders", "app.ledger_*", "!app.ledger_skip"}, caseSensitive)
		require.NoError(t, err)
		require.True(t, matcher.Match("APP", "orders"))
		require.Equal(t, !caseSensitive, matcher.Match("app", "orders"))
		require.True(t, matcher.Match("app", "ledger_1"))
		require.False(t, matcher.Match("app", "ledger_skip"))
		require.False(t, matcher.Match("app", "other"))
	}
	for _, rules := range [][]string{nil, {}} {
		matcher, err := NewTxnOrderMatcher(rules, false)
		require.NoError(t, err)
		require.Nil(t, matcher)
		require.False(t, matcher.Match("app", "orders"))
	}
	require.False(t, (&TxnOrderMatcher{}).Match("app", "orders"))
	matcher, err := NewTxnOrderMatcher([]string{"invalid"}, false)
	require.Nil(t, matcher)
	require.ErrorIs(t, err, errors.ErrInvalidReplicaConfig)
	require.ErrorContains(t, err, "txn-order-tables")
}
