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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNewRequiredAcks(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		requiredAcks RequiredAcks
		expected     kgo.Acks
	}{
		{name: "all", requiredAcks: -1, expected: kgo.AllISRAcks()},
		{name: "leader", requiredAcks: 1, expected: kgo.LeaderAck()},
		{name: "none", requiredAcks: 0, expected: kgo.NoAck()},
		{name: "invalid fallback all", requiredAcks: 2, expected: kgo.AllISRAcks()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, newRequiredAcks(&options{RequiredAcks: tc.requiredAcks}))
		})
	}
}

func TestNewOptionsRejectsInvalidAssignedVersion(t *testing.T) {
	t.Parallel()

	opts, err := newOptions(context.Background(), &options{
		Version:           "invalid",
		IsAssignedVersion: true,
		sasl:              &saslConfig{},
	}, nil)
	require.Nil(t, opts)
	require.ErrorContains(t, err, "invalid kafka version invalid")
}

func TestNewProducerOptionsUsesProducerBatchMaxBytes(t *testing.T) {
	t.Parallel()

	const producerBatchMaxBytes = 1048588
	o := &options{
		BrokerEndpoints: []string{"127.0.0.1:9092"},
		MaxMessageBytes: producerBatchMaxBytes,
		MaxRetry:        defaultMaxRetry,
		RequiredAcks:    WaitForAll,
		ReadTimeout:     defaultTimeout,
		WriteTimeout:    defaultTimeout,
		sasl:            &saslConfig{},
	}

	opts, err := newOptions(context.Background(), o, nil)
	require.NoError(t, err)
	opts = append(opts, newProducerOptions(o)...)
	client, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	defer client.Close()

	require.Equal(t, int32(producerBatchMaxBytes), client.OptValue(kgo.ProducerBatchMaxBytes))
}

func TestNewCompressionOptionMapsToProducerBatchCompression(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		compression string
		expected    kgo.CompressionCodec
	}{
		{compression: "gzip", expected: kgo.GzipCompression()},
		{compression: "snappy", expected: kgo.SnappyCompression()},
		{compression: "lz4", expected: kgo.Lz4Compression()},
		{compression: "zstd", expected: kgo.ZstdCompression()},
	}

	for _, tc := range testCases {
		t.Run(tc.compression, func(t *testing.T) {
			t.Parallel()

			client, err := kgo.NewClient(
				kgo.SeedBrokers("127.0.0.1:9092"),
				newCompressionOption(&options{Compression: tc.compression}),
			)
			require.NoError(t, err)
			defer client.Close()

			require.Equal(t, []kgo.CompressionCodec{tc.expected}, client.OptValue(kgo.ProducerBatchCompression))
		})
	}
}

func TestNewOauthTokenSourceRejectsInvalidTokenURL(t *testing.T) {
	t.Parallel()

	_, err := newOauthTokenSource(context.Background(), &options{
		sasl: &saslConfig{
			oauth2: oauth2Config{
				clientID:     "client-id",
				clientSecret: "client-secret",
				tokenURL:     "http://test.com/Segment%%2815197306101420000%29",
				scopes:       []string{"scope1", "scope2"},
				grantType:    "client_credentials",
			},
		},
	})
	require.ErrorContains(t, err, "invalid URL escape")
}
