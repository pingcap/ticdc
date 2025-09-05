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
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	v2 "github.com/pingcap/ticdc/api/v2"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/api/v2/mock"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestChangefeedListCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cf := mock.NewMockChangefeedInterface(ctrl)
	f := &mockFactory{changefeeds: cf}
	cmd := newCmdListChangefeed(f)
	b := bytes.NewBufferString("")
	cmd.SetOut(b)

	cf.EXPECT().List(gomock.Any(), gomock.Any(), gomock.Any()).Return([]v2.ChangefeedCommonInfo{
		{
			UpstreamID:     1,
			Keyspace:       "default",
			ID:             "pending-1",
			CheckpointTime: api.JSONTime{},
			RunningError:   nil,
			FeedState:      config.StateWarning,
		},
		{
			UpstreamID:     1,
			Keyspace:       "default",
			ID:             "normal-2",
			CheckpointTime: api.JSONTime{},
			RunningError:   nil,
			FeedState:      config.StateNormal,
		},
		{
			UpstreamID:     1,
			Keyspace:       "default",
			ID:             "failed-3",
			CheckpointTime: api.JSONTime{},
			RunningError:   nil,
			FeedState:      config.StateFailed,
		},
		{
			UpstreamID:     1,
			Keyspace:       "default",
			ID:             "removed-4",
			CheckpointTime: api.JSONTime{},
			RunningError:   nil,
			FeedState:      config.StateRemoved,
		},
		{
			UpstreamID:     1,
			Keyspace:       "default",
			ID:             "finished-5",
			CheckpointTime: api.JSONTime{},
			RunningError:   nil,
			FeedState:      config.StateFinished,
		},
		{
			UpstreamID:     1,
			Keyspace:       "default",
			ID:             "stopped-6",
			CheckpointTime: api.JSONTime{},
			RunningError:   nil,
			FeedState:      config.StateStopped,
		},
		{
			UpstreamID:     1,
			Keyspace:       "default",
			ID:             "warning-7",
			CheckpointTime: api.JSONTime{},
			RunningError:   nil,
			FeedState:      config.StateStopped,
		},
	}, nil).Times(2)
	// when --all=false, should contains StateNormal, StateWarning, StateFailed, StateStopped changefeed
	os.Args = []string{"list", "--all=false", "--keyspace=default"}
	require.Nil(t, cmd.Execute())
	out, err := io.ReadAll(b)
	require.Nil(t, err)
	require.Contains(t, string(out), "pending-1")
	require.Contains(t, string(out), "normal-2")
	require.Contains(t, string(out), "stopped-6")
	require.Contains(t, string(out), "failed-3")
	require.Contains(t, string(out), "warning-7")

	// when --all=true, should contains all changefeed
	os.Args = []string{"list", "--all=true", "--keyspace=default"}
	require.Nil(t, cmd.Execute())
	out, err = io.ReadAll(b)
	require.Nil(t, err)
	require.Contains(t, string(out), "pending-1")
	require.Contains(t, string(out), "normal-2")
	require.Contains(t, string(out), "failed-3")
	require.Contains(t, string(out), "removed-4")
	require.Contains(t, string(out), "finished-5")
	require.Contains(t, string(out), "stopped-6")
	require.Contains(t, string(out), "warning-7")

	cf.EXPECT().List(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("changefeed list test error"))
	o := newListChangefeedOptions()
	require.NoError(t, o.complete(f))
	require.Contains(t, o.run(cmd).Error(), "changefeed list test error")
}
