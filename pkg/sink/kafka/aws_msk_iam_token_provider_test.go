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

	"github.com/IBM/sarama"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/stretchr/testify/require"
)

func TestAWSMSKIAMTokenProvider(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	provider := newAWSMSKIAMTokenProvider(t.Context(), security.AWSMSKIAM{
		Region: "us-east-1",
	})
	token, err := provider.Token()
	require.NoError(t, err)
	require.NotEmpty(t, token.Token)
}

func TestCompleteSaramaSASLConfigUsesAWSMSKIAMProvider(t *testing.T) {
	saramaConfig := sarama.NewConfig()
	options := NewOptions()
	options.SASL.SASLMechanism = security.OAuthMechanism
	options.SASL.OAuthProvider = SASLOAuthProviderAWSMSKIAM
	options.SASL.AWSMSKIAM = security.AWSMSKIAM{
		Region:          "ap-northeast-1",
		RoleARN:         "arn:aws:iam::123456789012:role/TiCDCMSKProducer",
		RoleSessionName: "ticdc",
		ExternalID:      "external-id",
	}

	err := completeSaramaSASLConfig(context.Background(), saramaConfig, options)
	require.NoError(t, err)
	provider, ok := saramaConfig.Net.SASL.TokenProvider.(*awsMSKIAMTokenProvider)
	require.True(t, ok)
	require.Equal(t, options.SASL.AWSMSKIAM, provider.config)
}
