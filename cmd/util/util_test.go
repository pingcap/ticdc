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

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaskProxyForLog(t *testing.T) {
	for _, proxy := range []string{
		"http://user:password@proxy.example.com:8080/path?token=secret&region=west",
		"user:password@proxy.example.com:8080/path?token=secret&region=west",
	} {
		logged := maskProxyForLog(proxy)
		require.Equal(t, "http://user:xxxxx@proxy.example.com:8080/path?region=west&token=xxxxx", logged)
		require.NotContains(t, logged, "password")
		require.NotContains(t, logged, "secret")
	}
}
