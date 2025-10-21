// Copyright 2025 PingCAP, Inc.
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

//go:build !nextgen

package middleware

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/stretchr/testify/require"
)

func TestKeyspaceCheckerMiddleware(t *testing.T) {
	tests := []struct {
		name           string
		keyspace       string
		mockKeyspace   *keyspacepb.KeyspaceMeta
		mockError      error
		expectedStatus int
		expectedAbort  bool
	}{
		{
			name:           "default keyspace",
			keyspace:       "",
			expectedStatus: http.StatusOK,
			expectedAbort:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// Create test request
			req := httptest.NewRequestWithContext(ctx, "GET", fmt.Sprintf("/test?%s=%s", api.APIOpVarKeyspace, tt.keyspace), nil)

			// Create gin context
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = req
			KeyspaceCheckerMiddleware()(c)

			require.Equal(t, tt.expectedAbort, c.IsAborted())
			require.Equal(t, tt.expectedStatus, w.Code)
		})
	}
}
