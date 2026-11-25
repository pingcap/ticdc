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

package writer

import (
	"testing"

	"github.com/golang/mock/gomock"
	sinkmock "github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/stretchr/testify/require"
)

func TestWriterAdvanceCheckpointNotifiesTarget(t *testing.T) {
	ctrl := gomock.NewController(t)
	target := sinkmock.NewMockSink(ctrl)
	gomock.InOrder(
		target.EXPECT().AddCheckpointTs(uint64(100)).Times(1),
		target.EXPECT().AddCheckpointTs(uint64(149)).Times(1),
	)

	w := &Writer{
		target:    target,
		mutations: make(map[mutationKey]bool),
		written:   make(map[int64]uint64),
	}

	w.Advance(Progress{Watermark: 100})
	w.advanceCheckpoint()
	// Calling again without progress must not re-emit the same checkpoint.
	w.advanceCheckpoint()

	w.Advance(Progress{Watermark: 200, Pending: 150})
	w.advanceCheckpoint()

	require.Equal(t, uint64(149), w.Checkpoint())
}
