//  Copyright 2026 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package memory

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	writertest "github.com/pingcap/ticdc/pkg/redo/writer/testutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestNewEncodingWorkerGroup(t *testing.T) {
	t.Parallel()

	changefeed := common.NewChangeFeedIDWithName("test-cf", common.DefaultKeyspaceName)
	cfg := writertest.NewConsistentConfig("nfs:///tmp/redo")
	cfg.EncodingWorkerNum = util.AddressOf(3)
	writerCfg, err := writer.NewConfig(changefeed, cfg)
	require.NoError(t, err)
	g := newEncodingWorkerGroup(writerCfg)
	require.Equal(t, 3, g.workerNum)
	require.Len(t, g.inputChs, 3)

	defaultCfg, err := writer.NewConfig(changefeed, writertest.NewConsistentConfig("nfs:///tmp/redo"))
	require.NoError(t, err)
	g = newEncodingWorkerGroup(defaultCfg)
	require.Equal(t, redo.DefaultEncodingWorkerNum, g.workerNum)
	require.Len(t, g.inputChs, redo.DefaultEncodingWorkerNum)
}
