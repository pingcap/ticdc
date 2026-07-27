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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestAdminClientClose(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*gomock.Controller) *saramaAdminClient
	}{
		{
			name: "uses admin close",
			setup: func(ctrl *gomock.Controller) *saramaAdminClient {
				client := NewMocksaramaClient(ctrl)
				admin := NewMocksaramaClusterAdmin(ctrl)
				admin.EXPECT().Close().Return(nil)
				client.EXPECT().Close().Times(0)
				return &saramaAdminClient{
					changefeed: common.NewChangeFeedIDWithName("test", "default"),
					client:     client,
					admin:      admin,
				}
			},
		},
		{
			name: "falls back to client when admin is nil",
			setup: func(ctrl *gomock.Controller) *saramaAdminClient {
				client := NewMocksaramaClient(ctrl)
				client.EXPECT().Close().Return(nil)
				return &saramaAdminClient{
					changefeed: common.NewChangeFeedIDWithName("test", "default"),
					client:     client,
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			adminClient := test.setup(ctrl)

			require.NotPanics(t, func() { adminClient.Close() })
		})
	}
}
