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
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
)

type awsMSKIAMTokenProvider struct {
	ctx         context.Context
	config      config.AWSMSKIAMConfig
	credentials aws.CredentialsProvider
}

var _ sarama.AccessTokenProvider = (*awsMSKIAMTokenProvider)(nil)

func newAWSMSKIAMTokenProvider(
	ctx context.Context, config config.AWSMSKIAMConfig,
) (*awsMSKIAMTokenProvider, error) {
	// LoadDefaultConfig wraps the default credential chain in a concurrency-safe cache.
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(config.Region))
	if err != nil {
		return nil, wrapAWSMSKIAMError("load AWS configuration", err)
	}
	credentials := cfg.Credentials
	if config.RoleARN != "" {
		credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(
			sts.NewFromConfig(cfg), config.RoleARN, func(o *stscreds.AssumeRoleOptions) {
				o.Duration = time.Hour
				o.RoleSessionName = config.RoleSessionName
				if o.RoleSessionName == "" {
					o.RoleSessionName = signer.DefaultSessionName
				}
				if config.ExternalID != "" {
					o.ExternalID = aws.String(config.ExternalID)
				}
			}))
	}
	return &awsMSKIAMTokenProvider{ctx: ctx, config: config, credentials: credentials}, nil
}

func (p *awsMSKIAMTokenProvider) Token() (*sarama.AccessToken, error) {
	token, err := p.token(p.ctx)
	if err != nil {
		return nil, err
	}
	return &sarama.AccessToken{Token: token}, nil
}

func (p *awsMSKIAMTokenProvider) token(ctx context.Context) (string, error) {
	token, _, err := signer.GenerateAuthTokenFromCredentialsProvider(ctx, p.config.Region, p.credentials)
	if err != nil {
		return "", wrapAWSMSKIAMError("generate AWS MSK IAM token", err)
	}
	return token, nil
}

// AWS errors can embed credential_process output, signed URLs, or response bodies.
// Retain only bounded diagnostic fields and never keep the original error as a cause.
type awsMSKIAMError struct {
	operation  string
	statusCode int
	code       string
}

func (e *awsMSKIAMError) Error() string {
	return fmt.Sprintf("%s failed (HTTP status %d, AWS code %s)", e.operation, e.statusCode, e.code)
}

func wrapAWSMSKIAMError(operation string, err error) error {
	if errors.Is(err, context.Canceled) {
		return errors.WrapError(errors.ErrNewKafkaSink, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.WrapError(errors.ErrNewKafkaSink, context.DeadlineExceeded)
	}
	safeErr := &awsMSKIAMError{operation: operation, code: "unknown"}
	var responseErr interface{ HTTPStatusCode() int }
	if errors.As(err, &responseErr) {
		safeErr.statusCode = responseErr.HTTPStatusCode()
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		// ErrorCode is also supplied by the remote endpoint and may contain credentials.
		switch code := apiErr.ErrorCode(); code {
		case "AccessDenied", "AccessDeniedException", "ExpiredToken", "ExpiredTokenException",
			"InvalidClientTokenId", "Throttling", "ThrottlingException", "RequestLimitExceeded",
			"ServiceUnavailable", "InternalFailure", "InternalError", "RequestTimeout":
			safeErr.code = code
		}
	}
	// Credential services can recover; only static option validation is unretryable.
	return errors.WrapError(errors.ErrNewKafkaSink, safeErr)
}
