// Copyright 2023 PingCAP, Inc.
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

package producer

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"

	"github.com/stretchr/testify/require"
)

func TestPulsarSyncAsyncSendMessage(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := NewMockPulsarDMLProducer()
	err := p.AsyncSendMessage(ctx, "test", 0, &common.Message{
		Value:        []byte("this value for test input data"),
		PartitionKey: str2Pointer("test_key"),
	})
	require.NoError(t, err)
}
