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

//go:build nextgen

package v2

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/kvproto/pkg/pdpb"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/ticdc/pkg/server/mock"
	"github.com/stretchr/testify/require"
)

func TestTableInspectionRoutesRejectInvalidKeyspace(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctrl := gomock.NewController(t)
	previous, _ := appcontext.TryGetService[keyspace.Manager](appcontext.KeyspaceManager)
	t.Cleanup(func() { appcontext.SetService(appcontext.KeyspaceManager, previous) })
	keyspaceManager := keyspace.NewMockManager(ctrl)
	keyspaceManager.EXPECT().LoadKeyspace(gomock.Any(), "not-exist").Return(
		nil, errors.New(pdpb.ErrorType_ENTRY_NOT_FOUND.String()),
	).Times(2)
	appcontext.SetService[keyspace.Manager](appcontext.KeyspaceManager, keyspaceManager)

	router := gin.New()
	RegisterOpenAPIV2Routes(router, NewOpenAPIV2(mock.NewMockServer(ctrl)))

	paths := []string{"/api/v2/verify_table", "/api/v2/get_all_tables"}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			for _, query := range []string{"", "?keyspace=not-exist"} {
				request := httptest.NewRequest(http.MethodPost, path+query, nil)
				response := httptest.NewRecorder()

				router.ServeHTTP(response, request)

				require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
			}
		})
	}
}
