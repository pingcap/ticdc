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
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestNewTokenProvider(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		options     *options
		expectedErr string
	}{
		{
			name: "valid",
			options: &options{
				sasl: &saslConfig{
					oauth2: oauth2Config{
						clientID:     "client-id",
						clientSecret: "client-secret",
						tokenURL:     "http://localhost:8080/oauth2/token",
						scopes:       []string{"scope1", "scope2"},
						grantType:    "client_credentials",
					},
				},
			},
		},
		{
			name: "invalid token URL",
			options: &options{
				sasl: &saslConfig{
					oauth2: oauth2Config{
						clientID:     "client-id",
						clientSecret: "client-secret",
						tokenURL:     "http://test.com/Segment%%2815197306101420000%29",
						scopes:       []string{"scope1", "scope2"},
						grantType:    "client_credentials",
					},
				},
			},
			expectedErr: "invalid URL escape",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := newTokenProvider(t.Context(), test.options)
			if test.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, errors.ErrKafkaInvalidConfig)
				var escapeErr url.EscapeError
				require.ErrorAs(t, err, &escapeErr)
				require.Contains(t, err.Error(), test.expectedErr)
			}
		})
	}
}
