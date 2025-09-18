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

package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForwardToServer(t *testing.T) {
	// Setup mock target server
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify received headers
		require.Equal(t, "test-id", r.Header.Get(forwardFrom))
		require.Equal(t, "1", r.Header.Get(forwardTimes))

		// Send back test response
		w.Header().Set("X-Test-Header", "test-value")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test response"))
	}))
	defer targetServer.Close()

	// Setup gin context
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	// Create test request
	req := httptest.NewRequest("GET", "/test-path", nil)
	req.Header.Set("X-Original-Header", "original-value")
	c.Request = req

	// Call the function
	ForwardToServer(c, "test-id", targetServer.Listener.Addr().String())

	// Verify response
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "test-value", w.Header().Get("X-Test-Header"))
	require.Equal(t, "test response", w.Body.String())
}

func TestForwardToServerExceedMaxForwardTimes(t *testing.T) {
	// Setup gin context
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	// Create test request with max forward times exceeded
	req := httptest.NewRequest("GET", "/test-path", nil)
	req.Header.Set(forwardTimes, strconv.Itoa(maxForwardTimes+1))
	c.Request = req

	// Call the function
	ForwardToServer(c, "test-id", "dummy-addr")

	// Verify error is set in context
	require.NotNil(t, c.Errors.Last())
	require.Contains(t, c.Errors.Last().Error(), "TiCDC cluster is unavailable")
}

func TestForwardToCoordinatorMiddleware(t *testing.T) {
	tests := []struct {
		name           string
		isCoordinator  bool
		expectedAbort  bool
		expectedCalled bool
	}{
		{
			name:           "not coordinator should forward request",
			isCoordinator:  false,
			expectedAbort:  true,
			expectedCalled: false,
		},
		{
			name:           "coordinator should process request",
			isCoordinator:  true,
			expectedAbort:  false,
			expectedCalled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock server
			mockServer := &mockServer{
				isCoordinator:   tt.isCoordinator,
				selfInfo:        &node.Info{ID: "test-node-1"},
				coordinatorInfo: &node.Info{ID: "coordinator-1"},
			}

			// Create gin context
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("GET", "/test", nil)

			// Track if next handler was called
			nextCalled := false

			// Create middleware
			middleware := ForwardToCoordinatorMiddleware(mockServer)

			// Execute middleware with a mock next handler
			middleware(c)
			if !tt.expectedAbort {
				c.Next()
				nextCalled = true
			}

			// Verify expectations
			assert.Equal(t, tt.expectedAbort, c.IsAborted())
			assert.Equal(t, tt.expectedCalled, nextCalled)
		})
	}
}

func TestForwardToCoordinator(t *testing.T) {
	tests := []struct {
		name          string
		setupMock     func(s *mockServer, ts *httptest.Server)
		expectedError bool
		ts            *httptest.Server
	}{
		{
			name: "successful forward",
			setupMock: func(s *mockServer, ts *httptest.Server) {
				peer := strings.TrimPrefix(ts.URL, "http://")
				s.selfInfo = &node.Info{ID: "test-node-1"}
				s.coordinatorInfo = &node.Info{
					ID:            "coordinator-1",
					AdvertiseAddr: peer,
				}
			},
			expectedError: false,
			ts: httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})),
		},
		{
			name: "self info error",
			setupMock: func(s *mockServer, ts *httptest.Server) {
				s.selfInfoErr = errors.New("self info error")
			},
			expectedError: true,
		},
		{
			name: "get coordinator error",
			setupMock: func(s *mockServer, ts *httptest.Server) {
				s.selfInfo = &node.Info{ID: "test-node-1"}
				s.coordinatorErr = errors.New("coordinator not found")
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock server
			ms := &mockServer{}
			if tt.setupMock != nil {
				tt.setupMock(ms, tt.ts)
			}

			// Create gin context
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("GET", "/test", nil)

			// Call the function
			ForwardToCoordinator(c, ms)

			// Check if error was recorded as expected
			if tt.expectedError {
				assert.NotNil(t, c.Errors.Last())
			} else {
				assert.Nil(t, c.Errors.Last())
			}

			if tt.ts != nil {
				tt.ts.Close()
			}
		})
	}
}

// mockServer implements server.Server interface for testing
type mockServer struct {
	server.Server
	isCoordinator   bool
	selfInfoErr     error
	coordinatorErr  error
	coordinatorInfo *node.Info
	selfInfo        *node.Info
}

func (s *mockServer) IsCoordinator() bool {
	return s.isCoordinator
}

func (s *mockServer) SelfInfo() (*node.Info, error) {
	return s.selfInfo, s.selfInfoErr
}

func (s *mockServer) GetCoordinatorInfo(ctx context.Context) (*node.Info, error) {
	return s.coordinatorInfo, s.coordinatorErr
}

type mockPDAPIClient struct {
	pdutil.PDAPIClient
	keyspace *keyspacepb.KeyspaceMeta
	err      error
}

func (m *mockPDAPIClient) LoadKeyspace(ctx context.Context, keyspace string) (*keyspacepb.KeyspaceMeta, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.keyspace, nil
}
