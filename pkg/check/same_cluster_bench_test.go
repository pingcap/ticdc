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

package check

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
)

func BenchmarkValidateSameClusterRouting(b *testing.B) {
	for _, tc := range []struct {
		name    string
		length  int
		rules   int
		derived bool
	}{
		{name: "fixedSchema16", length: 16, rules: 1},
		{name: "fixedSchema32", length: 32, rules: 1},
		{name: "fixedSchema48", length: 48, rules: 1},
		{name: "derivedSchema16", length: 16, rules: 1, derived: true},
		{name: "fourRules8", length: 8, rules: 4},
	} {
		b.Run(tc.name, func(b *testing.B) {
			cfg := &config.ChangefeedConfig{
				AllowSameCluster: true,
				Filter:           &config.FilterConfig{},
				SinkConfig:       &config.SinkConfig{},
			}
			for i := range tc.rules {
				prefix := strings.Repeat("a", tc.length)
				if tc.rules > 1 {
					prefix = fmt.Sprintf("db%d%s", i, prefix)
				}
				pattern := prefix + "*." + prefix + "*"
				rule := &config.DispatchRule{Matcher: []string{pattern}}
				rule.TargetSchema = strings.Repeat("b", tc.length)
				if tc.derived {
					rule.TargetSchema += "{schema}"
				}
				cfg.Filter.Rules = append(cfg.Filter.Rules, pattern)
				cfg.SinkConfig.DispatchRules = append(cfg.SinkConfig.DispatchRules, rule)
			}
			b.ReportAllocs()
			for b.Loop() {
				if err := ValidateSameClusterRouting(cfg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
