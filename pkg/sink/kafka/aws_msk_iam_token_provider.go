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

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
)

type awsMSKIAMTokenProvider struct {
	ctx    context.Context
	config security.AWSMSKIAM
}

var _ sarama.AccessTokenProvider = (*awsMSKIAMTokenProvider)(nil)

func newAWSMSKIAMTokenProvider(
	ctx context.Context, config security.AWSMSKIAM,
) sarama.AccessTokenProvider {
	return &awsMSKIAMTokenProvider{ctx: ctx, config: config}
}

func (p *awsMSKIAMTokenProvider) Token() (*sarama.AccessToken, error) {
	var (
		token string
		err   error
	)
	if p.config.RoleARN == "" {
		token, _, err = signer.GenerateAuthToken(p.ctx, p.config.Region)
	} else {
		token, _, err = signer.GenerateAuthTokenFromRoleWithExternalId(
			p.ctx, p.config.Region, p.config.RoleARN,
			p.config.RoleSessionName, p.config.ExternalID)
	}
	if err != nil {
		return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}
	return &sarama.AccessToken{Token: token}, nil
}
