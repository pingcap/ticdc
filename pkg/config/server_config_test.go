// Copyright 2021 PingCAP, Inc.
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

package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

<<<<<<< HEAD
=======
func TestServerConfigMarshal(t *testing.T) {
	t.Parallel()
	conf := GetDefaultServerConfig()
	conf.Addr = "192.155.22.33:8887"
	b, err := conf.Marshal()
	require.NoError(t, err)
	conf2 := new(ServerConfig)
	err = conf2.Unmarshal([]byte(b))
	require.NoError(t, err)
	require.Equal(t, conf, conf2)
}

func TestServerConfigStringMasksKMSCredentials(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })
	conf := GetDefaultServerConfig()
	conf.Encryption.KMS.AWS.AccessKey = "aws-access-secret"
	conf.Encryption.KMS.AWS.SecretAccessKey = "aws-private-secret"
	conf.Encryption.KMS.AWS.SessionToken = "aws-session-secret"
	conf.Encryption.KMS.AWS.Endpoint = "https://kms.example.com/path?authorization=Bearer%20aws-authorization-secret&region=west"
	conf.Encryption.KMS.GCP.CredentialsJSON = `{"private_key":"gcp-private-secret"}`

	for _, mode := range []string{errors.RedactLogDisable, errors.RedactLogMarker, errors.RedactLogEnable} {
		errors.RedactLogEnabled.Store(mode)
		logged := conf.String()
		for _, secret := range []string{
			"aws-access-secret", "aws-private-secret", "aws-session-secret",
			"aws-authorization-secret", "gcp-private-secret",
		} {
			require.NotContains(t, logged, secret)
		}
		require.Contains(t, logged, `"secret_access_key":"xxxxx"`)
		require.Contains(t, logged, `"credentials_json":"xxxxx"`)
		require.Contains(t, logged, `authorization=xxxxx`)
		require.Contains(t, logged, `region=west`)
	}
	require.Equal(t, "aws-private-secret", conf.Encryption.KMS.AWS.SecretAccessKey)
	require.Equal(t, `{"private_key":"gcp-private-secret"}`, conf.Encryption.KMS.GCP.CredentialsJSON)
}

func TestServerConfigDecodeEnableLegacySafePoint(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "server.toml")
	configContent := strings.TrimSpace(`
enable-legacy-safepoint = true
`)
	require.NoError(t, os.WriteFile(configPath, []byte(configContent), 0o644))

	cfg := GetDefaultServerConfig()
	require.False(t, cfg.EnableLegacySafePoint)

	metaData, err := toml.DecodeFile(configPath, cfg)
	require.NoError(t, err)
	require.Empty(t, metaData.Undecoded())
	require.True(t, cfg.EnableLegacySafePoint)
}

>>>>>>> 6ed23c60f (*: keep credentials out of logs (#6716))
func TestServerConfigDecodeEnableKafkaFranzGo(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		content string
		enabled bool
	}{
		{name: "default"},
		{name: "disabled", content: "[debug]\nenable-kafka-franz-go = false\n"},
		{name: "enabled", content: "[debug]\nenable-kafka-franz-go = true\n", enabled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := GetDefaultServerConfig()
			metaData, err := toml.Decode(test.content, cfg)
			require.NoError(t, err)
			require.Empty(t, metaData.Undecoded())
			require.Equal(t, test.enabled, cfg.Debug.EnableKafkaFranzGo)
			require.Equal(t, test.enabled, cfg.Clone().Debug.EnableKafkaFranzGo)
		})
	}
}

func TestServerConfigLargeTxnThresholdDefault(t *testing.T) {
	t.Parallel()
	conf := GetDefaultServerConfig()
	require.Equal(t, int64(1024*1024), conf.Debug.EventService.LargeTxnThresholdInBytes)
}

func TestServerConfigExposeEncryptionAtRoot(t *testing.T) {
	t.Parallel()

	serverConfigType := reflect.TypeOf(ServerConfig{})
	encryptionField, ok := serverConfigType.FieldByName("Encryption")
	require.True(t, ok)
	require.Equal(t, "encryption", encryptionField.Tag.Get("toml"))

	debugConfigType := reflect.TypeOf(DebugConfig{})
	_, ok = debugConfigType.FieldByName("Encryption")
	require.False(t, ok)
}

func TestServerConfigDecodeEncryptionConfigAtRoot(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "server.toml")
	configContent := strings.TrimSpace(`
[encryption]
enable-encryption = true
meta-refresh-interval = "2h"
meta-cache-ttl = "30m"
allow-degrade-on-error = false

  [encryption.kms.aws]
  region = "us-west-2"
`)
	require.NoError(t, os.WriteFile(configPath, []byte(configContent), 0o644))

	cfg := GetDefaultServerConfig()
	metaData, err := toml.DecodeFile(configPath, cfg)
	require.NoError(t, err)
	require.Empty(t, metaData.Undecoded())

	encryptionValue := reflect.ValueOf(cfg).Elem().FieldByName("Encryption")
	require.True(t, encryptionValue.IsValid())
	require.False(t, encryptionValue.IsNil())

	encryptionConfig := encryptionValue.Elem()
	require.True(t, encryptionConfig.FieldByName("EnableEncryption").Bool())
	require.Equal(t, TomlDuration(2*time.Hour), encryptionConfig.FieldByName("MetaRefreshInterval").Interface().(TomlDuration))
	require.Equal(t, TomlDuration(30*time.Minute), encryptionConfig.FieldByName("MetaCacheTTL").Interface().(TomlDuration))
	require.False(t, encryptionConfig.FieldByName("AllowDegradeOnError").Bool())

	kmsValue := encryptionConfig.FieldByName("KMS")
	require.False(t, kmsValue.IsNil())
	awsValue := kmsValue.Elem().FieldByName("AWS")
	require.False(t, awsValue.IsNil())
	require.Equal(t, "us-west-2", awsValue.Elem().FieldByName("Region").String())
}

func TestServerConfigEncryptionDefault(t *testing.T) {
	t.Parallel()

	cfg := GetDefaultServerConfig().Clone()
	require.NotNil(t, cfg.Encryption)
	cfg.Encryption = nil
	require.NoError(t, cfg.ValidateAndAdjust())
	require.Equal(t, GetDefaultServerConfig().Encryption, cfg.Encryption)
}
