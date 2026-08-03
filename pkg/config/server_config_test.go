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
	"github.com/stretchr/testify/require"
)

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
