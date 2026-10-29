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

package lake

import (
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
)

// All quotas are local to this sink instance. Exclusive is an operator assertion,
// not a distributed lock or permission to automatically fail over an old query.
type options struct {
	endpoint, database, username, password, warehouse, spoolDir string
	workers, batchRows, maxEntries, sourceEvents                int
	batchBytes, spoolBytes, sourceBytes                         int64
	memoryRatio                                                 float64
	flushInterval, queryTimeout                                 time.Duration
	remoteLimit                                                 int64
	timezone                                                    *time.Location
	conflictKey                                                 string
}

func parseOptions(uri *url.URL, cfg *config.ChangefeedConfig) (options, error) {
	o := options{workers: 4, batchRows: 2048, batchBytes: 1 << 20, spoolBytes: 10 << 30, sourceBytes: 256 << 20, sourceEvents: 128, maxEntries: 4096, memoryRatio: 0.02, flushInterval: time.Second, queryTimeout: 2 * time.Minute}
	bad := func() (options, error) {
		return options{}, errors.ErrSinkURIInvalid.GenWithStackByArgs("invalid Lake options; require exclusive=true, bounded positive quotas and supported incremental settings")
	}
	if uri == nil || (config.GetScheme(uri) != config.LakeScheme && config.GetScheme(uri) != config.LakeHTTPScheme) || uri.Host == "" || uri.Fragment != "" {
		return bad()
	}
	q, queryErr := url.ParseQuery(uri.RawQuery)
	if queryErr != nil {
		return bad()
	}
	allowed := map[string]bool{"exclusive": true, "workers": true, "batch-rows": true, "batch-bytes": true, "spool-bytes": true, "source-bytes": true, "source-events": true, "max-entries": true, "memory-ratio": true, "flush-interval": true, "query-timeout": true, "spool-dir": true, "warehouse": true, "remote-bytes-limit": true, "conflict-key": true}
	for key, v := range q {
		if !allowed[key] || len(v) != 1 {
			return bad()
		}
	}
	if q.Get("exclusive") != "true" {
		return bad()
	}
	if q.Has("conflict-key") {
		o.conflictKey = q.Get("conflict-key")
		if strings.TrimSpace(o.conflictKey) == "" || strings.ContainsRune(o.conflictKey, 0) || len(o.conflictKey) > 256 {
			return bad()
		}
	}
	o.timezone = time.UTC
	if cfg != nil {
		if cfg.TimeZone != "" {
			var err error
			o.timezone, err = util.GetTimezone(cfg.TimeZone)
			if err != nil {
				return bad()
			}
		}
		if cfg.EnableTableAcrossNodes || cfg.EnableSyncPoint || cfg.EnableActiveActive || cfg.IsLowLatencyMode() {
			return bad()
		}
		if cfg.Consistent != nil && cfg.Consistent.Level != nil && *cfg.Consistent.Level != "none" && *cfg.Consistent.Level != "" {
			return bad()
		}
		if cfg.SinkConfig != nil {
			if cfg.SinkConfig.TxnAtomicity != nil && *cfg.SinkConfig.TxnAtomicity != "none" && *cfg.SinkConfig.TxnAtomicity != "" {
				return bad()
			}
			for _, r := range cfg.SinkConfig.DispatchRules {
				if r != nil && (r.TargetSchema != "" || r.TargetTable != "") {
					return bad()
				}
			}
			if len(cfg.SinkConfig.ColumnSelectors) > 0 {
				return bad()
			}
		}
	}
	for key, p := range map[string]*int{"workers": &o.workers, "batch-rows": &o.batchRows, "source-events": &o.sourceEvents, "max-entries": &o.maxEntries} {
		if q.Has(key) {
			n, err := strconv.Atoi(q.Get(key))
			if err != nil || n <= 0 {
				return bad()
			}
			*p = n
		}
	}
	for key, p := range map[string]*int64{"batch-bytes": &o.batchBytes, "spool-bytes": &o.spoolBytes, "source-bytes": &o.sourceBytes, "remote-bytes-limit": &o.remoteLimit} {
		if q.Has(key) {
			n, err := strconv.ParseInt(q.Get(key), 10, 64)
			if err != nil || n <= 0 {
				return bad()
			}
			*p = n
		}
	}
	for key, p := range map[string]*time.Duration{"flush-interval": &o.flushInterval, "query-timeout": &o.queryTimeout} {
		if q.Has(key) {
			d, err := time.ParseDuration(q.Get(key))
			if err != nil || d <= 0 {
				return bad()
			}
			*p = d
		}
	}
	if q.Has("memory-ratio") {
		n, err := strconv.ParseFloat(q.Get("memory-ratio"), 64)
		if err != nil || !(n > 0 && n < 1) {
			return bad()
		}
		o.memoryRatio = n
	}
	if o.workers > 64 || o.batchRows > 100000 || o.sourceEvents > 65536 || o.maxEntries > 65536 || o.batchBytes < 1024 || o.batchBytes > 64<<20 || o.spoolBytes < o.batchBytes*2 || o.sourceBytes < o.batchBytes || o.spoolBytes > 1<<50 {
		return bad()
	}
	o.database = strings.Trim(uri.Path, "/")
	if strings.Contains(o.database, "/") || strings.ContainsRune(o.database, 0) {
		return bad()
	}
	o.username = "root"
	if uri.User != nil {
		o.username = uri.User.Username()
		o.password, _ = uri.User.Password()
	}
	endpoint := *uri
	endpoint.Path = ""
	endpoint.User = nil
	endpoint.RawQuery = ""
	endpoint.RawPath = ""
	endpoint.Scheme = "https"
	if config.GetScheme(uri) == config.LakeHTTPScheme {
		host := uri.Hostname()
		ip := net.ParseIP(host)
		if !strings.EqualFold(host, "localhost") && (ip == nil || !ip.IsLoopback()) {
			return bad()
		}
		endpoint.Scheme = "http"
	}
	o.endpoint = endpoint.String()
	o.warehouse = q.Get("warehouse")
	o.spoolDir = q.Get("spool-dir")
	return o, nil
}
