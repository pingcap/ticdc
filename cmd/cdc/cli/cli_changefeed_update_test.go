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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/log"
	v2 "github.com/pingcap/ticdc/api/v2"
	"github.com/pingcap/ticdc/pkg/config"
<<<<<<< HEAD
	putil "github.com/pingcap/ticdc/pkg/util"
=======
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
>>>>>>> ce44c4dde (api,cli: keep credential redaction at display boundaries (#6464))
	"github.com/stretchr/testify/require"
)

func TestApplyChanges(t *testing.T) {
	t.Parallel()

	cmd := NewCmdCli()
	commonChangefeedOptions := newChangefeedCommonOptions()
	o := newUpdateChangefeedOptions(commonChangefeedOptions)
	o.addFlags(cmd)

	// Test normal update.
	oldInfo := &v2.ChangeFeedInfo{SinkURI: "blackhole://"}
	require.Nil(t, cmd.ParseFlags([]string{"--sink-uri=mysql://root@downstream-tidb:4000"}))
	newInfo, err := o.applyChanges(oldInfo, cmd)
	require.Nil(t, err)
	require.Equal(t, "mysql://root@downstream-tidb:4000", newInfo.SinkURI)

	// Test for cli command flags that should be ignored.
	oldInfo = &v2.ChangeFeedInfo{SinkURI: "blackhole://"}
	require.Nil(t, cmd.ParseFlags([]string{"--log-level=debug"}))
	_, err = o.applyChanges(oldInfo, cmd)
	require.Nil(t, err)

	oldInfo = &v2.ChangeFeedInfo{SinkURI: "blackhole://"}
	require.Nil(t, cmd.ParseFlags([]string{"--pd=http://127.0.0.1:2379"}))
	_, err = o.applyChanges(oldInfo, cmd)
	require.Nil(t, err)

	dir := t.TempDir()
	filename := filepath.Join(dir, "log.txt")
	reset, err := initTestLogger(filename)
	defer reset()
	require.Nil(t, err)

	// Test for flag that cannot be updated.
	oldInfo = &v2.ChangeFeedInfo{SinkURI: "blackhole://"}
	require.Nil(t, cmd.ParseFlags([]string{"--sort-dir=/home"}))
	_, err = o.applyChanges(oldInfo, cmd)
	require.Nil(t, err)
	file, err := os.ReadFile(filename)
	require.Nil(t, err)
	require.True(t, strings.Contains(string(file), "this flag cannot be updated and will be ignored"))

	// Test schema registry update
	oldInfo = &v2.ChangeFeedInfo{Config: v2.ToAPIReplicaConfig(config.GetDefaultReplicaConfig())}
	require.True(t, oldInfo.Config.Sink.SchemaRegistry == nil)
	require.Nil(t, cmd.ParseFlags([]string{"--schema-registry=https://username:password@localhost:8081"}))
	newInfo, err = o.applyChanges(oldInfo, cmd)
	require.Nil(t, err)
	require.Equal(t,
		putil.AddressOf("https://username:password@localhost:8081"),
		newInfo.Config.Sink.SchemaRegistry)
}

func initTestLogger(filename string) (func(), error) {
	logConfig := &log.Config{
		File: log.FileLogConfig{
			Filename: filename,
		},
	}

	logger, props, err := log.InitLogger(logConfig)
	if err != nil {
		return nil, err
	}
	log.ReplaceGlobals(logger, props)

	return func() {
		conf := &log.Config{Level: "info", File: log.FileLogConfig{}}
		logger, props, _ := log.InitLogger(conf)
		log.ReplaceGlobals(logger, props)
	}, nil
}

func TestChangefeedUpdateCli(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	f := newMockFactory(ctrl)
	o := newUpdateChangefeedOptions(newChangefeedCommonOptions())
	o.complete(f)
	cmd := newCmdUpdateChangefeed(f)
	f.changefeeds.EXPECT().Get(gomock.Any(), gomock.Any(), "abc").Return(nil, errors.New("test"))
	os.Args = []string{"update", "--no-confirm=true", "--changefeed-id=abc"}
	o.commonChangefeedOptions.noConfirm = true
	o.changefeedID = "abc"
	require.NotNil(t, o.run(cmd))

<<<<<<< HEAD
	f.changefeeds.EXPECT().Get(gomock.Any(), gomock.Any(), "abc").
		Return(&v2.ChangeFeedInfo{
			ID: "abc",
			Config: &v2.ReplicaConfig{
				Sink: &v2.SinkConfig{},
			},
		}, nil)
	f.changefeeds.EXPECT().GetAllTables(gomock.Any(), gomock.Any(), "ks").
		Return(&v2.Tables{}, nil)
	f.changefeeds.EXPECT().Update(gomock.Any(), gomock.Any(), "ks", "abc").
		Return(&v2.ChangeFeedInfo{}, nil)
=======
	oldInfo := &v2.ChangeFeedInfo{
		ID:      "abc",
		SinkURI: "kafka://user:xxxxx@127.0.0.1:9092/topic",
		Config:  v2.ToAPIReplicaConfig(config.GetDefaultReplicaConfig()),
	}
	oldInfo.Config.Sink.KafkaConfig = &v2.KafkaConfig{SASLPassword: new("stored-password-sentinel")}
	f.changefeeds.EXPECT().Get(gomock.Any(), gomock.Any(), "abc").Return(oldInfo, nil)
	f.changefeeds.EXPECT().GetAllTables(gomock.Any(), gomock.Any(), "ks").
		Return(&v2.Tables{}, nil)
	f.changefeeds.EXPECT().Update(gomock.Any(), gomock.Any(), "ks", "abc").
		DoAndReturn(func(_ context.Context, cfg *v2.ChangefeedConfig, _, _ string) (*v2.ChangeFeedInfo, error) {
			require.Contains(t, cfg.SinkURI, "update-password-sentinel")
			require.Equal(t, "stored-password-sentinel", *cfg.ReplicaConfig.Sink.KafkaConfig.SASLPassword)
			return &v2.ChangeFeedInfo{
				ID:      "abc",
				SinkURI: "kafka://user:xxxxx@127.0.0.1:9092/topic",
				Config:  cfg.ReplicaConfig,
			}, nil
		})
>>>>>>> ce44c4dde (api,cli: keep credential redaction at display boundaries (#6464))
	dir := t.TempDir()
	configPath := filepath.Join(dir, "cf.toml")
	err := os.WriteFile(configPath, []byte(""), 0o644)
	require.Nil(t, err)
	os.Args = []string{
		"update",
		"--config=" + configPath,
		"--no-confirm=false",
		"--target-ts=10",
		"--sink-uri=abcd",
		"--schema-registry=a",
		"--sort-engine=memory",
		"--changefeed-id=abc",
		"--keyspace=ks",
		"--sort-dir=a",
		"--upstream-pd=pd",
		"--upstream-ca=ca",
		"--upstream-cert=cer",
		"--upstream-key=key",
	}

	path := filepath.Join(dir, "confirm.txt")
	err = os.WriteFile(path, []byte("y"), 0o644)
	require.Nil(t, err)
	file, err := os.Open(path)
	require.Nil(t, err)
	stdin := os.Stdin
	os.Stdin = file
	defer func() {
		os.Stdin = stdin
	}()
	require.Nil(t, cmd.Execute())
<<<<<<< HEAD
=======
	require.NotContains(t, output.String(), "update-password-sentinel")
	require.NotContains(t, output.String(), "stored-password-sentinel")
	require.Contains(t, output.String(), "xxxxx")
	require.Contains(t, output.String(), "******")
	require.Equal(t, "stored-password-sentinel", *oldInfo.Config.Sink.KafkaConfig.SASLPassword)
>>>>>>> ce44c4dde (api,cli: keep credential redaction at display boundaries (#6464))

	// no diff
	cmd = newCmdUpdateChangefeed(f)
	f.changefeeds.EXPECT().Get(gomock.Any(), gomock.Any(), "abc").
		Return(&v2.ChangeFeedInfo{}, nil)
	os.Args = []string{"update", "--no-confirm=true", "-c", "abc"}
	require.Nil(t, cmd.Execute())

	cmd = newCmdUpdateChangefeed(f)
	f.changefeeds.EXPECT().Get(gomock.Any(), "ks", "abcd").
		Return(&v2.ChangeFeedInfo{ID: "abcd"}, errors.New("test"))
	o.commonChangefeedOptions.noConfirm = true
	o.commonChangefeedOptions.sortEngine = "unified"
	o.changefeedID = "abcd"
	o.keyspace = "ks"
	require.NotNil(t, o.run(cmd))
}

func TestChangefeedUpdateReplicaCredentials(t *testing.T) {
	for _, tc := range []struct {
		name     string
		config   string
		password *string
	}{
		{name: "memory quota", config: "memory-quota = 2097152\n"},
		{name: "password", config: "[sink.kafka-config]\nsasl-password = \"new-password-sentinel\"\n", password: new("new-password-sentinel")},
		{name: "empty password", config: "[sink.kafka-config]\nsasl-password = \"\"\n", password: new("")},
		{name: "literal masked password", config: "[sink.kafka-config]\nsasl-password = \"******\"\n", password: new("******")},
		{name: "pulsar TLS", config: "[sink.pulsar-config]\ntls-key-file-path = \"client.key\"\ntls-certificate-file = \"client.crt\"\n"},
		{name: "clear glue token", config: "[sink.kafka-config.glue-schema-registry-config]\ntoken = \"\"\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			f := newMockFactory(ctrl)
			oldInfo := &v2.ChangeFeedInfo{
				ID:      "abc",
				SinkURI: "kafka://127.0.0.1:9092/topic?protocol=open-protocol&sasl-password=xxxxx",
				Config:  v2.ToAPIReplicaConfig(config.GetDefaultReplicaConfig()),
			}
			oldInfo.Config.Sink.KafkaConfig = &v2.KafkaConfig{
				SASLUser:                 new("alice"),
				LargeMessageHandle:       &v2.LargeMessageHandleConfig{},
				GlueSchemaRegistryConfig: &v2.GlueSchemaRegistryConfig{RegistryName: "registry"},
			}
			oldInfo.Config.Sink.PulsarConfig = &v2.PulsarConfig{OAuth2: &v2.PulsarOAuth2{OAuth2ClientID: "client"}}
			oldInfo.Config.Consistent.Storage = nil
			f.changefeeds.EXPECT().Get(gomock.Any(), "default", "abc").Return(oldInfo, nil)
			f.changefeeds.EXPECT().GetAllTables(gomock.Any(), gomock.Any(), "default").Return(&v2.Tables{}, nil)
			f.changefeeds.EXPECT().Update(gomock.Any(), gomock.Any(), "default", "abc").
				DoAndReturn(func(_ context.Context, cfg *v2.ChangefeedConfig, _, _ string) (*v2.ChangeFeedInfo, error) {
					require.Empty(t, cfg.SinkURI)
					require.Equal(t, tc.password, cfg.ReplicaConfig.Sink.KafkaConfig.SASLPassword)
					require.Nil(t, cfg.ReplicaConfig.Consistent.Storage)
					require.Nil(t, cfg.ReplicaConfig.Sink.KafkaConfig.LargeMessageHandle.ClaimCheckStorageURI)
					glue := cfg.ReplicaConfig.Sink.KafkaConfig.GlueSchemaRegistryConfig
					require.Nil(t, glue.AccessKey)
					require.Nil(t, glue.SecretAccessKey)
					if tc.name == "clear glue token" {
						require.Equal(t, new(""), glue.Token)
					} else {
						require.Nil(t, glue.Token)
					}
					require.Nil(t, cfg.ReplicaConfig.Sink.PulsarConfig.OAuth2.OAuth2PrivateKey)
					require.Nil(t, cfg.ReplicaConfig.Sink.PulsarConfig.OAuth2.OAuth2IssuerURL)
					require.Equal(t, "alice", *cfg.ReplicaConfig.Sink.KafkaConfig.SASLUser)
					if tc.name == "memory quota" {
						require.Equal(t, uint64(2097152), *cfg.ReplicaConfig.MemoryQuota)
					}
					if tc.name == "pulsar TLS" {
						require.Equal(t, "client.key", *cfg.ReplicaConfig.Sink.PulsarConfig.TLSKeyFilePath)
						require.Equal(t, "client.crt", *cfg.ReplicaConfig.Sink.PulsarConfig.TLSCertificateFile)
					}
					return &v2.ChangeFeedInfo{ID: "abc", SinkURI: oldInfo.SinkURI, Config: cfg.ReplicaConfig}, nil
				})

			configPath := filepath.Join(t.TempDir(), "cf.toml")
			require.NoError(t, os.WriteFile(configPath, []byte(tc.config), 0o644))
			cmd := newCmdUpdateChangefeed(f)
			cmd.SetContext(t.Context())
			cmd.SetArgs([]string{"--changefeed-id=abc", "--no-confirm=true", "--config=" + configPath})
			output := new(bytes.Buffer)
			cmd.SetOut(output)

			require.NoError(t, cmd.Execute())
			require.Nil(t, oldInfo.Config.Sink.KafkaConfig.SASLPassword)
			require.NotContains(t, output.String(), "sentinel")
			if tc.password != nil && *tc.password != "" {
				require.Contains(t, output.String(), "******")
			}
		})
	}
}

func TestChangefeedUpdateExplicitSinkURI(t *testing.T) {
	for _, tc := range []struct {
		name      string
		updateErr error
	}{
		{name: "success"},
		{name: "authentication failure", updateErr: errors.ErrSinkURIInvalid.GenWithStackByArgs("authentication failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			f := newMockFactory(ctrl)
			const sinkURI = "kafka://127.0.0.1:9092/topic?protocol=open-protocol&sasl-mechanism=SCRAM-SHA-256&sasl-password=xxxxx&sasl-user=alice"
			oldInfo := &v2.ChangeFeedInfo{
				ID:      "abc",
				SinkURI: sinkURI,
				Config:  v2.ToAPIReplicaConfig(config.GetDefaultReplicaConfig()),
			}
			f.changefeeds.EXPECT().Get(gomock.Any(), "default", "abc").Return(oldInfo, nil)
			f.changefeeds.EXPECT().GetAllTables(gomock.Any(), gomock.Any(), "default").Return(&v2.Tables{}, nil)
			f.changefeeds.EXPECT().Update(gomock.Any(), gomock.Any(), "default", "abc").
				DoAndReturn(func(_ context.Context, cfg *v2.ChangefeedConfig, _, _ string) (*v2.ChangeFeedInfo, error) {
					require.Equal(t, sinkURI, cfg.SinkURI)
					if tc.updateErr != nil {
						return nil, tc.updateErr
					}
					return &v2.ChangeFeedInfo{ID: "abc", SinkURI: cfg.SinkURI, Config: cfg.ReplicaConfig}, nil
				})

			o := newUpdateChangefeedOptions(newChangefeedCommonOptions())
			require.NoError(t, o.complete(f))
			cmd := NewCmdCli()
			o.addFlags(cmd)
			cmd.SetContext(t.Context())
			require.NoError(t, cmd.ParseFlags([]string{
				"--changefeed-id=abc", "--no-confirm=true", "--sink-uri=" + sinkURI,
			}))
			output := new(bytes.Buffer)
			cmd.SetOut(output)

			err := o.run(cmd)
			if tc.updateErr != nil {
				require.ErrorIs(t, err, tc.updateErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, sinkURI, oldInfo.SinkURI)
			require.NotContains(t, output.String(), "do nothing")
		})
	}
}
