// Copyright 2024 PingCAP, Inc.
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

package sink

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsTransactionAtomicEnabled(t *testing.T) {
	tests := []struct {
		name     string
		sinkURI  string
		expected bool
	}{
		{
			name:     "enable-transaction-atomic=true",
			sinkURI:  "mysql://user:pass@localhost:3306/test?enable-transaction-atomic=true",
			expected: true,
		},
		{
			name:     "enable-transaction-atomic=false",
			sinkURI:  "mysql://user:pass@localhost:3306/test?enable-transaction-atomic=false",
			expected: false,
		},
		{
			name:     "enable-transaction-atomic not set",
			sinkURI:  "mysql://user:pass@localhost:3306/test",
			expected: false,
		},
		{
			name:     "enable-transaction-atomic with invalid value",
			sinkURI:  "mysql://user:pass@localhost:3306/test?enable-transaction-atomic=invalid",
			expected: false,
		},
		{
			name:     "enable-transaction-atomic=1",
			sinkURI:  "mysql://user:pass@localhost:3306/test?enable-transaction-atomic=1",
			expected: true,
		},
		{
			name:     "enable-transaction-atomic=0",
			sinkURI:  "mysql://user:pass@localhost:3306/test?enable-transaction-atomic=0",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedURI, err := url.Parse(tt.sinkURI)
			require.NoError(t, err)

			result := isTransactionAtomicEnabled(parsedURI)
			require.Equal(t, tt.expected, result)
		})
	}
}
