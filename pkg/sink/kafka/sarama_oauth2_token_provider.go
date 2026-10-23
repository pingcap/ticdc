// Copyright 2023 PingCAP, Inc.
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
	"golang.org/x/oauth2"
)

// tokenProvider is a user-defined callback for generating
// access tokens for SASL/OAUTHBEARER auth.
type tokenProvider struct {
	tokenSource oauth2.TokenSource
}

var _ sarama.AccessTokenProvider = (*tokenProvider)(nil)

// Token implements the sarama.AccessTokenProvider interface.
// Token returns an access token. The implementation should ensure token
// reuse so that multiple calls at connect time do not create multiple
// tokens. The implementation should also periodically refresh the token in
// order to guarantee that each call returns an unexpired token.  This
// method should not block indefinitely--a timeout error should be returned
// after a short period of inactivity so that the broker connection logic
// can log debugging information and retry.
func (t *tokenProvider) Token() (*sarama.AccessToken, error) {
	token, err := t.tokenSource.Token()
	if err != nil {
		// Errors will result in Sarama retrying the broker connection and logging
		// the transient error, with a Broker connection error surfacing after retry
		// attempts have been exhausted.
		return nil, err
	}

	return &sarama.AccessToken{Token: token.AccessToken}, nil
}

func newTokenProvider(ctx context.Context, o *options) (sarama.AccessTokenProvider, error) {
	tokenSource, err := newOAuthTokenSource(ctx, o.sasl.oauth2)
	if err != nil {
		return nil, err
	}
	return &tokenProvider{tokenSource: tokenSource}, nil
}
