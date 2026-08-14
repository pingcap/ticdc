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

package factory

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/redo/testutil"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/stretchr/testify/require"
)

func TestNewRedoWriters(t *testing.T) {
	t.Parallel()

	cfg, err := writer.NewConfig(
		common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName),
		testutil.NewConsistentConfig("blackhole://"),
	)
	require.NoError(t, err)

	dmlWriter, err := NewRedoDMLWriter(context.Background(), cfg)
	require.NoError(t, err)
	require.Implements(t, (*writer.RedoDMLWriter)(nil), dmlWriter)

	ddlWriter, err := NewRedoDDLWriter(context.Background(), cfg)
	require.NoError(t, err)
	require.Implements(t, (*writer.RedoDDLWriter)(nil), ddlWriter)
}
