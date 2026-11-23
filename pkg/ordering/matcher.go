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
	"github.com/pingcap/ticdc/pkg/errors"
	tfilter "github.com/pingcap/tidb/pkg/util/table-filter"
)

// TxnOrderMatcher selects tables that require complete, ordered transactions.
// It is immutable and safe for concurrent use.
type TxnOrderMatcher struct {
	filter tfilter.Filter
}

// NewTxnOrderMatcher compiles ordering rules. Empty rules disable ordering.
func NewTxnOrderMatcher(rules []string, caseSensitive bool) (*TxnOrderMatcher, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	f, err := tfilter.Parse(rules)
	if err != nil {
		return nil, errors.WrapError(errors.ErrInvalidReplicaConfig, err,
			"invalid sink.mysql-config.txn-order-tables")
	}
	if !caseSensitive {
		f = tfilter.CaseInsensitive(f)
	}
	return &TxnOrderMatcher{filter: f}, nil
}

// Match reports whether a table requires transaction ordering.
// A nil matcher or its zero value matches no tables.
func (m *TxnOrderMatcher) Match(schema, table string) bool {
	return m != nil && m.filter != nil && m.filter.MatchTable(schema, table)
}
