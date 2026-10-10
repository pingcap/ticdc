// Copyright 2022 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"net/http"
<<<<<<< HEAD
=======
	"strings"

	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/config"
>>>>>>> ce44c4dde (api,cli: keep credential redaction at display boundaries (#6464))
)

func (h *OpenAPIV2) handleDebugInfo(w http.ResponseWriter, req *http.Request) {
	ctx, cli := req.Context(), h.server.GetEtcdClient()
	kvs, err := cli.GetAllCDCInfo(ctx)
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, err)
		return
	}

	var output bytes.Buffer
	for _, kv := range kvs {
<<<<<<< HEAD
		fmt.Fprintf(w, "%s\n\t%s\n\n", string(kv.Key), string(kv.Value))
=======
		value := string(kv.Value)
		if strings.Contains(string(kv.Key), "/changefeed/info/") {
			value, err = config.MaskChangefeedInfo(kv.Value)
			if err != nil {
				api.WriteError(w, http.StatusInternalServerError, err)
				return
			}
		}
		_, _ = fmt.Fprintf(&output, "%s\n\t%s\n\n", string(kv.Key), value)
>>>>>>> ce44c4dde (api,cli: keep credential redaction at display boundaries (#6464))
	}
	_, _ = fmt.Fprintf(w, "\n\n*** etcd info ***:\n\n")
	_, _ = output.WriteTo(w)
}
