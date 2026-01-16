// Copyright 2022 PingCAP, Inc.
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

package spanz

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
)

func TestArrayToSpan(t *testing.T) {
	t.Parallel()

	in := []heartbeatpb.Table{
		{TableID: 1},
		{TableID: 2},
		{TableID: 3},
	}
	out := ArrayToSpan(in)
	require.Equal(t, []heartbeatpb.TableSpan{
		{TableID: 1},
		{TableID: 2},
		{TableID: 3},
	}, out)
}

func TestSort(t *testing.T) {
	t.Parallel()

	spans := []heartbeatpb.TableSpan{
		{TableID: 3},
		{TableID: 1},
		{TableID: 2},
	}
	Sort(spans)
	require.Equal(t, []heartbeatpb.TableSpan{
		{TableID: 1},
		{TableID: 2},
		{TableID: 3},
	}, spans)
}
