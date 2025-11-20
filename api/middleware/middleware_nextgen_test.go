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

//go:build nextgen

package middleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/ticdc/pkg/api"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/stretchr/testify/require"
)

func TestKeyspaceCheckerMiddleware(t *testing.T) {
	tests := []struct {
		name                 string
		keyspace             string
		init                 func(t *testing.T, mock *keyspace.MockManager)
		expectedStatus       int
		expectedAbort        bool
		expectedBodyContains string
		expectedMeta         *keyspacepb.KeyspaceMeta
	}{
		{
			name:           "default keyspace",
			keyspace:       "",
			expectedStatus: http.StatusBadRequest,
			expectedAbort:  true,
		},
		{
			name:     "keyspace not exist",
			keyspace: "not-exist",
			init: func(t *testing.T, mock *keyspace.MockManager) {
				mock.EXPECT().LoadKeyspace(gomock.Any(), "not-exist").Return(nil, errors.New(pdpb.ErrorType_ENTRY_NOT_FOUND.String()))
			},
			expectedStatus:       http.StatusBadRequest,
			expectedAbort:        true,
			expectedBodyContains: "invalid api parameter",
		},
		{
			name:     "internal server error",
			keyspace: "internal-error",
			init: func(t *testing.T, mock *keyspace.MockManager) {
				mock.EXPECT().LoadKeyspace(gomock.Any(), "internal-error").Return(nil, errors.New("internal error"))
			},
			expectedStatus:       http.StatusInternalServerError,
			expectedAbort:        true,
			expectedBodyContains: "internal error",
		},
		{
			name:     "success",
			keyspace: "success",
			init: func(t *testing.T, mock *keyspace.MockManager) {
				mock.EXPECT().LoadKeyspace(gomock.Any(), "success").Return(&keyspacepb.KeyspaceMeta{
					Id:    1,
					Name:  "kespace1",
					State: keyspacepb.KeyspaceState_ENABLED,
				}, nil)
			},
			expectedStatus:       http.StatusOK,
			expectedAbort:        false,
			expectedBodyContains: "",
			expectedMeta: &keyspacepb.KeyspaceMeta{
				Id:    1,
				Name:  "kespace1",
				State: keyspacepb.KeyspaceState_ENABLED,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mock := keyspace.NewMockManager(ctrl)

			if tt.init != nil {
				tt.init(t, mock)
			}

			ctx := context.Background()
			appcontext.SetService(appcontext.KeyspaceManager, mock)

			// Create test request
			req := httptest.NewRequestWithContext(ctx, "GET", fmt.Sprintf("/test?%s=%s", api.APIOpVarKeyspace, tt.keyspace), nil)

			// Create gin context
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = req
			KeyspaceCheckerMiddleware()(c)

			require.Equal(t, tt.expectedAbort, c.IsAborted())
			require.Equal(t, tt.expectedStatus, w.Code)

			if tt.expectedBodyContains != "" {
				require.NotNil(t, w.Body)
				require.Contains(t, w.Body.String(), tt.expectedBodyContains)
			}

			if tt.expectedMeta != nil {
				require.EqualValues(t, tt.expectedMeta, GetKeyspaceFromContext(c))
			}
		})
	}
}
