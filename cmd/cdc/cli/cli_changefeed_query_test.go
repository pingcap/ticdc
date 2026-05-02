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
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	v2 "github.com/pingcap/ticdc/api/v2"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/api/v2/mock"
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
	cfV2.EXPECT().Get(gomock.Any(), gomock.Any(), "bcd").Return(&v2.ChangeFeedInfo{}, nil)

	o.simplified = false
	o.changefeedID = "bcd"
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	require.Nil(t, o.run(cmd))
	out, err := io.ReadAll(b)
	require.Nil(t, err)
	// make sure config is printed
	require.Contains(t, string(out), "config")

	// query failed
	cfV2.EXPECT().Get(gomock.Any(), gomock.Any(), "bcd").Return(nil, errors.New("test"))
	os.Args = []string{"query", "--simple=false", "--changefeed-id=bcd"}
	require.NotNil(t, o.run(cmd))
}

func TestChangefeedQueryTOMLOutput(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfV2 := mock.NewMockChangefeedInterface(ctrl)
	f := &mockFactory{changefeeds: cfV2}
	o := newQueryChangefeedOptions()
	o.complete(f)
	cmd := newCmdQueryChangefeed(f)

	caseSensitive := true
	workerNum := 8
	syncInterval := newJSONDuration(t, 10*time.Minute)
	cfV2.EXPECT().Get(gomock.Any(), gomock.Any(), "test-toml").Return(&v2.ChangeFeedInfo{
		UpstreamID: 1,
		ID:         "test-toml",
		Keyspace:   "default",
		SinkURI:    "blackhole://",
		Config: &v2.ReplicaConfig{
			CaseSensitive:     &caseSensitive,
			SyncPointInterval: &syncInterval,
			Mounter:           &v2.MounterConfig{WorkerNum: &workerNum},
		},
	}, nil)

	o.simplified = false
	o.changefeedID = "test-toml"
	o.outputFormat = "toml"
	buf := bytes.NewBufferString("")
	cmd.SetOut(buf)
	require.Nil(t, o.run(cmd))
	out := buf.String()

	require.Contains(t, out, "sink-uri = 'blackhole://'")
	require.Contains(t, out, "id = 'test-toml'")
	require.Contains(t, out, "[config]")
	require.Contains(t, out, "case-sensitive = true")
	require.Contains(t, out, "worker-num = 8")
	require.NotContains(t, out, "case_sensitive")
	require.NotContains(t, out, "worker_num")
}

func TestChangefeedQueryTOMLDurationFormat(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfV2 := mock.NewMockChangefeedInterface(ctrl)
	f := &mockFactory{changefeeds: cfV2}
	o := newQueryChangefeedOptions()
	o.complete(f)
	cmd := newCmdQueryChangefeed(f)

	syncInterval := newJSONDuration(t, 10*time.Minute)
	syncRetention := newJSONDuration(t, 24*time.Hour)
	enableSync := true
	cfV2.EXPECT().Get(gomock.Any(), gomock.Any(), "test-dur").Return(&v2.ChangeFeedInfo{
		UpstreamID: 1,
		ID:         "test-dur",
		SinkURI:    "blackhole://",
		Config: &v2.ReplicaConfig{
			EnableSyncPoint:    &enableSync,
			SyncPointInterval:  &syncInterval,
			SyncPointRetention: &syncRetention,
		},
	}, nil)

	o.simplified = false
	o.changefeedID = "test-dur"
	o.outputFormat = "toml"
	buf := bytes.NewBufferString("")
	cmd.SetOut(buf)
	require.Nil(t, o.run(cmd))
	out := buf.String()

	require.Contains(t, out, "sync-point-interval = '10m0s'")
	require.Contains(t, out, "sync-point-retention = '24h0m0s'")
	require.NotContains(t, out, "600000000000")
	require.NotContains(t, out, "86400000000000")
}

func TestConfigToMap_TOMLKeyNaming(t *testing.T) {
	caseSensitive := true
	workerNum := 4
	cfg := &v2.ReplicaConfig{
		CaseSensitive: &caseSensitive,
		Mounter:       &v2.MounterConfig{WorkerNum: &workerNum},
	}

	tomlMap, err := configToMap(cfg, "toml")
	require.NoError(t, err)
	require.Contains(t, tomlMap, "case-sensitive")
	require.NotContains(t, tomlMap, "case_sensitive")
	mounter, ok := tomlMap["mounter"].(map[string]interface{})
	require.True(t, ok)
	require.Contains(t, mounter, "worker-num")

	jsonMap, err := configToMap(cfg, "json")
	require.NoError(t, err)
	require.Contains(t, jsonMap, "case_sensitive")
	require.NotContains(t, jsonMap, "case-sensitive")
}

func TestConfigToMapNil(t *testing.T) {
	m, err := configToMap(nil, "toml")
	require.NoError(t, err)
	require.Nil(t, m)

	m, err = configToMap(nil, "json")
	require.NoError(t, err)
	require.Nil(t, m)
}

func TestChangefeedQueryTOMLTimestamp(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfV2 := mock.NewMockChangefeedInterface(ctrl)
	f := &mockFactory{changefeeds: cfV2}
	o := newQueryChangefeedOptions()
	o.complete(f)
	cmd := newCmdQueryChangefeed(f)

	createTime := time.Date(2026, 4, 18, 9, 35, 0, 139000000, time.UTC)
	cfV2.EXPECT().Get(gomock.Any(), gomock.Any(), "test-ts").Return(&v2.ChangeFeedInfo{
		UpstreamID: 1,
		ID:         "test-ts",
		SinkURI:    "blackhole://",
		CreateTime: createTime,
		Config:     &v2.ReplicaConfig{},
	}, nil)

	o.simplified = false
	o.changefeedID = "test-ts"
	o.outputFormat = "toml"
	buf := bytes.NewBufferString("")
	cmd.SetOut(buf)
	require.Nil(t, o.run(cmd))
	out := buf.String()

	require.Contains(t, out, "create-time = '2026-04-18 09:35:00.139'")
}

func TestChangefeedQuerySimplifiedTOML(t *testing.T) {
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
	o.outputFormat = "toml"
	buf := bytes.NewBufferString("")
	cmd.SetOut(buf)
	require.Nil(t, o.run(cmd))
	out := buf.String()

	require.Contains(t, out, "id = 'abc'")
	require.Contains(t, out, "keyspace = 'default'")
	require.Contains(t, out, "upstream-id = 1")
	require.Contains(t, out, "checkpoint-tso")
	// Must use kebab-case, not Go field names
	require.NotContains(t, out, "UpstreamID")
	require.NotContains(t, out, "FeedState")
	require.NotContains(t, out, "CheckpointTSO")
}

func TestChangefeedQueryTOMLNilConfig(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cfV2 := mock.NewMockChangefeedInterface(ctrl)
	f := &mockFactory{changefeeds: cfV2}
	o := newQueryChangefeedOptions()
	o.complete(f)
	cmd := newCmdQueryChangefeed(f)

	cfV2.EXPECT().Get(gomock.Any(), gomock.Any(), "nil-cfg").Return(&v2.ChangeFeedInfo{
		UpstreamID: 1,
		ID:         "nil-cfg",
		SinkURI:    "blackhole://",
		Config:     nil,
	}, nil)

	o.simplified = false
	o.changefeedID = "nil-cfg"
	o.outputFormat = "toml"
	buf := bytes.NewBufferString("")
	cmd.SetOut(buf)
	require.Nil(t, o.run(cmd))
	out := buf.String()

	require.Contains(t, out, "id = 'nil-cfg'")
}

func TestJSONTimeMarshalUnmarshalText(t *testing.T) {
	original := api.JSONTime(time.Date(2026, 4, 18, 9, 35, 0, 139000000, time.UTC))

	text, err := original.MarshalText()
	require.NoError(t, err)
	require.Equal(t, "2026-04-18 09:35:00.139", string(text))

	var parsed api.JSONTime
	require.NoError(t, parsed.UnmarshalText(text))
	require.Equal(t, original, parsed)
}

func TestJSONTimeUnmarshalTextError(t *testing.T) {
	var parsed api.JSONTime
	err := parsed.UnmarshalText([]byte("not-a-timestamp"))
	require.Error(t, err)
}

// newJSONDuration constructs a v2.JSONDuration via JSON round-trip since the
// duration field is unexported.
func newJSONDuration(t *testing.T, d time.Duration) v2.JSONDuration {
	t.Helper()
	data := []byte(`"` + d.String() + `"`)
	var jd v2.JSONDuration
	require.NoError(t, jd.UnmarshalJSON(data))
	return jd
}

func TestParseOutputFormat(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{"json", "json", false},
		{"JSON", "json", false},
		{"toml", "toml", false},
		{"TOML", "toml", false},
		{"yaml", "", true},
		{"", "", true},
	}
	for _, tt := range tests {
		format, err := util.ParseOutputFormat(tt.input)
		if tt.wantErr {
			require.Error(t, err, "input: %s", tt.input)
		} else {
			require.NoError(t, err, "input: %s", tt.input)
			require.Equal(t, tt.want, string(format))
		}
	}
}
