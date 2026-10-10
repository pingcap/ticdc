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
	v2 "github.com/pingcap/ticdc/api/v2"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/api/v2/mock"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestChangefeedQueryCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfV2 := mock.NewMockChangefeedInterface(ctrl)

	f := &mockFactory{changefeeds: cfV2}

	o := newQueryChangefeedOptions()
	o.complete(f)
	cmd := newCmdQueryChangefeed(f)

	cfV2.EXPECT().List(gomock.Any(), gomock.Any(), "all").Return([]v2.ChangefeedCommonInfo{
		{
			UpstreamID:     1,
			Keyspace:       "default",
			ID:             "abc",
			CheckpointTime: api.JSONTime{},
			RunningError:   nil,
		},
	}, nil)

	o.simplified = true
	o.changefeedID = "abc"
	require.Nil(t, o.run(cmd))
	cfV2.EXPECT().List(gomock.Any(), gomock.Any(), "all").Return([]v2.ChangefeedCommonInfo{
		{
			UpstreamID:     1,
			Keyspace:       "default",
			ID:             "abc",
			CheckpointTime: api.JSONTime{},
			RunningError:   nil,
		},
	}, nil)

	o.simplified = true
	o.changefeedID = "abcd"
	require.NotNil(t, o.run(cmd))

	cfV2.EXPECT().List(gomock.Any(), gomock.Any(), "all").Return(nil, errors.New("test"))
	o.simplified = true
	o.changefeedID = "abcd"
	require.NotNil(t, o.run(cmd))

	// query success
	info := &v2.ChangeFeedInfo{
		SinkURI: "kafka://user:uri-secret-sentinel@host/topic",
		Config: &v2.ReplicaConfig{Sink: &v2.SinkConfig{KafkaConfig: &v2.KafkaConfig{
			SASLPassword: new("password-sentinel"),
		}}},
	}
	cfV2.EXPECT().Get(gomock.Any(), gomock.Any(), "bcd").Return(info, nil)

	o.simplified = false
	o.changefeedID = "bcd"
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	require.Nil(t, o.run(cmd))
	out, err := io.ReadAll(b)
	require.Nil(t, err)
	// make sure config is printed
	require.Contains(t, string(out), "config")
	require.NotContains(t, string(out), "sentinel")
	require.Contains(t, string(out), "******")
	require.Equal(t, "password-sentinel", *info.Config.Sink.KafkaConfig.SASLPassword)

	// query failed
	cfV2.EXPECT().Get(gomock.Any(), gomock.Any(), "bcd").Return(nil, errors.New("test"))
	os.Args = []string{"query", "--simple=false", "--changefeed-id=bcd"}
	require.NotNil(t, o.run(cmd))
}
