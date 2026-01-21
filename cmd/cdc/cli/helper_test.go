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

package cli

import (
	"testing"

	v2 "github.com/pingcap/ticdc/api/v2"
	"github.com/stretchr/testify/require"
)

func TestFormatTableNames(t *testing.T) {
	t.Parallel()

	require.Equal(t, "[]", formatTableNames(nil))
	require.Equal(t, "[]", formatTableNames([]v2.TableName{}))

	require.Equal(t,
		"[test.t1]",
		formatTableNames([]v2.TableName{{Schema: "test", Table: "t1", TableID: 110}}),
	)

	require.Equal(t,
		"[test.t1, test.t2]",
		formatTableNames([]v2.TableName{
			{Schema: "test", Table: "t1", TableID: 110},
			{Schema: "test", Table: "t2", TableID: 111, IsPartition: true},
		}),
	)
}
