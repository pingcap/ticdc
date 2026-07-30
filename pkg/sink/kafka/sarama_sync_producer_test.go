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

package kafka

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	"go.uber.org/atomic"
)

func TestSyncProducerClose(t *testing.T) {
	tests := []struct {
		name           string
		clientCloseErr error
	}{
		{
			name: "closes client and producer",
		},
		{
			name:           "still closes producer when client close fails",
			clientCloseErr: errors.New("boom"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := NewMocksaramaSyncClient(ctrl)
			producer := NewMocksaramaSyncProducerClient(ctrl)
			gomock.InOrder(
				client.EXPECT().Close().Return(test.clientCloseErr),
				producer.EXPECT().Close().Return(nil),
			)

			p := &saramaSyncProducer{
				id:       common.NewChangeFeedIDWithName("test", "default"),
				client:   client,
				producer: producer,
				closed:   atomic.NewBool(false),
			}

			p.Close()
		})
	}
}
