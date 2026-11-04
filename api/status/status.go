// Copyright 2021 PingCAP, Inc.
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

package status

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/ticdc/api/middleware"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/server"
	"github.com/pingcap/ticdc/pkg/version"
)

// status of cdc server
type status struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
	ID      string `json:"id"`
	Pid     int    `json:"pid"`
	IsOwner bool   `json:"is_owner"`
}

type statusAPI struct {
	server server.Server
}

// RegisterStatusAPIRoutes registers routes for status.
func RegisterStatusAPIRoutes(router *gin.Engine, server server.Server) {
	statusAPI := statusAPI{server: server}
	router.GET("/status", gin.WrapF(statusAPI.handleStatus))
	router.GET("/debug/info", middleware.AuthenticateMiddleware(server), gin.WrapF(statusAPI.handleDebugInfo))
}

func (h *statusAPI) writeEtcdInfo(ctx context.Context, cli etcd.CDCEtcdClient, w io.Writer) error {
	kvs, err := cli.GetAllCDCInfo(ctx)
	if err != nil {
		return err
	}

	for _, kv := range kvs {
		value := string(kv.Value)
		if strings.Contains(string(kv.Key), "/changefeed/info/") {
			value, err = config.MaskChangefeedInfo(kv.Value)
			if err != nil {
				return err
			}
		}
		_, _ = fmt.Fprintf(w, "%s\n\t%s\n\n", string(kv.Key), value)
	}
	return nil
}

// TODO
func (h *statusAPI) handleDebugInfo(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	co, err := h.server.GetCoordinatorInfo(ctx)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	// Prepare diagnostic output before sending the response so errors can set its status.
	var output bytes.Buffer
	fmt.Fprintf(&output, "\n\n*** owner info ***:\n\n")
	fmt.Fprint(&output, co.String())
	self, err := h.server.SelfInfo()
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	fmt.Fprintf(&output, "\n\n*** processors info ***:\n\n")
	fmt.Fprint(&output, self.String())
	maintainers := h.server.GetMaintainerManager().ListMaintainers()
	for _, m := range maintainers {
		changefeedID := common.NewChangefeedIDFromPB(m.GetMaintainerStatus().ChangefeedID)
		fmt.Fprintf(&output, "changefeedID: %s\n", changefeedID)
	}
	fmt.Fprintf(&output, "\n\n*** etcd info ***:\n\n")
	if err := h.writeEtcdInfo(ctx, h.server.GetEtcdClient(), &output); err != nil {
		api.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	_, _ = output.WriteTo(w)
}

func (h *statusAPI) handleStatus(w http.ResponseWriter, _ *http.Request) {
	st := status{
		Version: version.ReleaseVersion,
		GitHash: version.GitHash,
		Pid:     os.Getpid(),
	}

	if h.server != nil {
		info, err := h.server.SelfInfo()
		if err != nil {
			api.WriteError(w, http.StatusInternalServerError, err)
			return
		}
		st.ID = string(info.ID)
		st.IsOwner = h.server.IsCoordinator()
	}
	api.WriteData(w, st)
}
