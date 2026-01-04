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

package v2

import (
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/pkg/api"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

type FailpointRequest struct {
	Name string `json:"name"`
	Expr string `json:"expr"`
}

type FailpointInfo struct {
	Name string `json:"name"`
	Expr string `json:"expr"`
}

type failpointRegistry struct {
	mu    sync.RWMutex
	items map[string]string
}

func newFailpointRegistry() *failpointRegistry {
	return &failpointRegistry{
		items: make(map[string]string),
	}
}

func (r *failpointRegistry) set(name, expr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items[name] = expr
}

func (r *failpointRegistry) remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.items, name)
}

func (r *failpointRegistry) snapshot() map[string]string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copied := make(map[string]string, len(r.items))
	for k, v := range r.items {
		copied[k] = v
	}
	return copied
}

var failpointState = newFailpointRegistry()

// EnableFailpoint enables a failpoint dynamically.
func (h *OpenAPIV2) EnableFailpoint(c *gin.Context) {
	req := &FailpointRequest{}
	if err := c.BindJSON(req); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid request: %s", err.Error()))
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	req.Expr = strings.TrimSpace(req.Expr)
	if req.Name == "" || req.Expr == "" {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStackByArgs("name and expr are required"))
		return
	}

	if err := failpoint.Enable(req.Name, req.Expr); err != nil {
		c.IndentedJSON(http.StatusInternalServerError, api.NewHTTPError(err))
		return
	}
	failpointState.set(req.Name, req.Expr)
	c.JSON(http.StatusOK, &EmptyResponse{})
}

// DisableFailpoint disables a failpoint dynamically.
func (h *OpenAPIV2) DisableFailpoint(c *gin.Context) {
	name := strings.TrimSpace(c.Query("name"))
	if name == "" {
		req := &FailpointRequest{}
		if err := c.BindJSON(req); err != nil {
			_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid request: %s", err.Error()))
			return
		}
		name = strings.TrimSpace(req.Name)
	}
	if name == "" {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStackByArgs("name is required"))
		return
	}

	if err := failpoint.Disable(name); err != nil {
		c.IndentedJSON(http.StatusInternalServerError, api.NewHTTPError(err))
		return
	}
	failpointState.remove(name)
	c.JSON(http.StatusOK, &EmptyResponse{})
}

// ListFailpoints lists failpoints enabled via the HTTP API.
func (h *OpenAPIV2) ListFailpoints(c *gin.Context) {
	snapshot := failpointState.snapshot()
	names := make([]string, 0, len(snapshot))
	for name := range snapshot {
		names = append(names, name)
	}
	sort.Strings(names)

	resp := make([]FailpointInfo, 0, len(names))
	for _, name := range names {
		resp = append(resp, FailpointInfo{
			Name: name,
			Expr: snapshot[name],
		})
	}
	c.JSON(http.StatusOK, resp)
}
