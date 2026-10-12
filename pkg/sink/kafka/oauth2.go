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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"net/http"
	"strconv"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"golang.org/x/oauth2"
)

// OAuth2 servers can echo credentials in error bodies, descriptions and URLs.
// Keep only the status and recognized protocol error codes for diagnostics.
type redactedOAuthTokenSource struct {
	oauth2.TokenSource
}

func (s *redactedOAuthTokenSource) Token() (*oauth2.Token, error) {
	token, err := s.TokenSource.Token()
	if err == nil {
		return token, nil
	}
	var retrieveErr *oauth2.RetrieveError
	if errors.As(err, &retrieveErr) {
		status := retrieveErr.Response.StatusCode
		var errorCode string
		// Only retain the token endpoint error codes defined by RFC 6749 section 5.2.
		// Arbitrary endpoint responses can contain credentials even in the error code.
		switch retrieveErr.ErrorCode {
		case "invalid_request", "invalid_client", "invalid_grant", "unauthorized_client",
			"unsupported_grant_type", "invalid_scope":
			errorCode = retrieveErr.ErrorCode
		}
		err = &oauth2.RetrieveError{
			Response: &http.Response{
				StatusCode: status,
				Status:     strconv.Itoa(status) + " " + http.StatusText(status),
			},
			ErrorCode: errorCode,
		}
	} else {
		err = util.MaskSensitiveDataInURLError(err)
	}
	return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
}
