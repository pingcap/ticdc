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
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestNewEncodingWorkerGroup(t *testing.T) {
	t.Parallel()

	changefeed := common.NewChangeFeedIDWithName("test-cf", common.DefaultKeyspaceName)
	cfg := &writer.LogWriterConfig{
		ConsistentConfig: config.ConsistentConfig{
			EncodingWorkerNum: util.AddressOf(3),
		},
		ChangeFeedID: changefeed,
	}
	g := newEncodingWorkerGroup(cfg)
	require.Equal(t, 3, g.workerNum)
	require.Len(t, g.inputChs, 3)

	cfg.EncodingWorkerNum = util.AddressOf(0)
	g = newEncodingWorkerGroup(cfg)
	require.Equal(t, redo.DefaultEncodingWorkerNum, g.workerNum)
	require.Len(t, g.inputChs, redo.DefaultEncodingWorkerNum)
}
