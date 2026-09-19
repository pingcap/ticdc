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

package main

import (
	"context"
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConsumerTopics(t *testing.T) {
	require.Equal(t, []string{"one", "two"}, consumerTopics(&option{topic: " one, ,two, "}))
	require.Empty(t, consumerTopics(&option{topic: " , "}))
}

func TestKafkaOptionsTLS(t *testing.T) {
	certDir := "../../tests/integration_tests/_certificates"
	invalidCA := filepath.Join(t.TempDir(), "invalid.pem")
	require.NoError(t, os.WriteFile(invalidCA, []byte("invalid certificate"), 0o600))
	for _, tc := range []struct {
		name     string
		option   option
		wantTLS  bool
		wantCert bool
		wantErr  bool
	}{
		{name: "plaintext"},
		{name: "CA only", option: option{ca: filepath.Join(certDir, "ca.pem")}, wantTLS: true},
		{name: "mutual TLS", option: option{ca: filepath.Join(certDir, "ca.pem"), cert: filepath.Join(certDir, "client.pem"), key: filepath.Join(certDir, "client-key.pem")}, wantTLS: true, wantCert: true},
		{name: "system roots", option: option{cert: filepath.Join(certDir, "client.pem"), key: filepath.Join(certDir, "client-key.pem")}, wantTLS: true, wantCert: true},
		{name: "invalid CA", option: option{ca: invalidCA}, wantErr: true},
		{name: "missing CA", option: option{ca: filepath.Join(t.TempDir(), "missing.pem")}, wantErr: true},
		{name: "missing key", option: option{cert: filepath.Join(certDir, "client.pem")}, wantErr: true},
		{name: "missing certificate", option: option{key: filepath.Join(certDir, "client-key.pem")}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.option.address = []string{"127.0.0.1:1"}
			opts, err := kafkaOptions(&tc.option)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			client, err := kgo.NewClient(opts...)
			require.NoError(t, err)
			defer client.Close()
			config, _ := client.OptValue(kgo.DialTLSConfig).(*tls.Config)
			if !tc.wantTLS {
				require.Nil(t, config)
				return
			}
			require.NotNil(t, config)
			require.Equal(t, uint16(tls.VersionTLS12), config.MinVersion)
			require.False(t, config.InsecureSkipVerify)
			require.Equal(t, tc.option.ca != "", config.RootCAs != nil)
			require.Equal(t, tc.wantCert, len(config.Certificates) == 1)
		})
	}
}

func TestReadMessageCancellation(t *testing.T) {
	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.ConsumeTopics("test"))
	require.NoError(t, err)
	defer client.Close()
	c := &consumer{client: client}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.readMessage(ctx) }()
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not exit after cancellation")
	}
}
