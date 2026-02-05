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
// See the License for the specific language governing permissions and
// limitations under the License.

package check

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/ticdc/pkg/config"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/servicediscovery"
)

const (
	pdTSOUniqueIndexKey = "tso-unique-index"
	pdTSOMaxIndexKey    = "tso-max-index"
)

const showPDConfigQuery = "SHOW CONFIG WHERE type='pd' AND (name='tso-unique-index' OR name='tso-max-index')"

// pdClientForTSOIndexValidation is the minimal PD client surface needed by the
// active-active TSO index validation.
type pdClientForTSOIndexValidation interface {
	GetAllMembers(ctx context.Context) (*pdpb.GetMembersResponse, error)
	GetServiceDiscovery() servicediscovery.ServiceDiscovery
}

type pdConfigFetcher interface {
	GetConfig(ctx context.Context, endpoint string) (map[string]any, error)
	Close()
}

type pdHTTPConfigFetcher struct {
	client pdhttp.Client
}

func (f *pdHTTPConfigFetcher) GetConfig(ctx context.Context, endpoint string) (map[string]any, error) {
	return f.client.WithTargetURL(endpoint).GetConfig(ctx)
}

func (f *pdHTTPConfigFetcher) Close() {
	if f.client != nil {
		f.client.Close()
	}
}

var (
	newPDConfigFetcherFn = newPDHTTPConfigFetcher
	collectPDEndpointsFn = collectPDEndpoints
)

func newPDHTTPConfigFetcher(upPD pdClientForTSOIndexValidation) (pdConfigFetcher, error) {
	if upPD == nil {
		return nil, cerrors.New("pd client is nil")
	}

	discovery := upPD.GetServiceDiscovery()
	opts := make([]pdhttp.ClientOption, 0, 1)
	if sc := config.GetGlobalServerConfig(); sc != nil && sc.Security != nil {
		tlsConf, err := sc.Security.ToTLSConfigWithVerify()
		if err != nil {
			return nil, cerrors.Trace(err)
		}
		if tlsConf != nil {
			opts = append(opts, pdhttp.WithTLSConfig(tlsConf))
		}
	}

	client := pdhttp.NewClientWithServiceDiscovery("cdc", discovery, opts...)
	return &pdHTTPConfigFetcher{client: client}, nil
}

func collectPDEndpoints(ctx context.Context, upPD pdClientForTSOIndexValidation) ([]string, error) {
	resp, err := upPD.GetAllMembers(ctx)
	if err != nil {
		return nil, cerrors.Trace(err)
	}
	endpoints := make([]string, 0, len(resp.Members))
	for _, m := range resp.Members {
		clientURLs := m.GetClientUrls()
		if len(clientURLs) > 0 {
			endpoints = append(endpoints, clientURLs[0])
		}
	}
	return endpoints, nil
}

// ValidateActiveActiveTSOIndexes validates the upstream/downstream PD TSO index
// compatibility when active-active mode is enabled and the downstream is TiDB.
//
// The validation is fail-closed: inability to retrieve or parse values is
// treated as a validation failure.
func ValidateActiveActiveTSOIndexes(
	ctx context.Context,
	upPD pdClientForTSOIndexValidation,
	changefeedCfg *config.ChangefeedConfig,
) error {
	if changefeedCfg == nil {
		return cerrors.ErrActiveActiveTSOIndexIncompatible.GenWithStackByArgs("changefeed config is nil")
	}
	if !changefeedCfg.EnableActiveActive {
		return nil
	}

	sinkURI, err := url.Parse(changefeedCfg.SinkURI)
	if err != nil {
		return cerrors.WrapError(
			cerrors.ErrActiveActiveTSOIndexIncompatible,
			err,
			"failed to parse sink uri",
		)
	}
	if !config.IsMySQLCompatibleScheme(config.GetScheme(sinkURI)) {
		return nil
	}

	downUnique, downMax, readTimeout, err := getDownstreamTSOIndexes(ctx, changefeedCfg, sinkURI)
	if err != nil {
		return cerrors.WrapError(
			cerrors.ErrActiveActiveTSOIndexIncompatible,
			err,
			"failed to read downstream tso index config",
		)
	}

	upUnique, upMax, err := getUpstreamTSOIndexes(ctx, upPD, readTimeout)
	if err != nil {
		return cerrors.WrapError(
			cerrors.ErrActiveActiveTSOIndexIncompatible,
			err,
			fmt.Sprintf("failed to read upstream tso index config, downstream unique=%d, downstream max=%d",
				downUnique, downMax),
		)
	}

	if upUnique == downUnique || upMax != downMax {
		return cerrors.ErrActiveActiveTSOIndexIncompatible.GenWithStackByArgs(
			fmt.Sprintf("active active tso index mismatch, upstream unique=%d, upstream max=%d, downstream unique=%d, downstream max=%d",
				upUnique, upMax, downUnique, downMax),
		)
	}
	return nil
}

func getDownstreamTSOIndexes(
	ctx context.Context,
	changefeedCfg *config.ChangefeedConfig,
	sinkURI *url.URL,
) (unique int64, max int64, readTimeout time.Duration, err error) {
	if changefeedCfg == nil {
		return 0, 0, 0, cerrors.New("changefeed config is nil")
	}

	mysqlCfg, db, err := newMySQLConfigAndDBFn(ctx, changefeedCfg.ChangefeedID, sinkURI, changefeedCfg)
	if err != nil {
		return 0, 0, 0, cerrors.Trace(err)
	}
	defer func() { _ = db.Close() }()

	readTimeout, err = time.ParseDuration(mysqlCfg.ReadTimeout)
	if err != nil {
		return 0, 0, 0, cerrors.Trace(err)
	}

	queryCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()

	rows, err := db.QueryContext(queryCtx, showPDConfigQuery)
	if err != nil {
		return 0, 0, 0, cerrors.Trace(err)
	}
	defer func() { _ = rows.Close() }()

	var (
		uniqueSet bool
		maxSet    bool
	)
	for rows.Next() {
		// Columns: Type | Instance | Name | Value
		var typ, instance, name, value string
		if err := rows.Scan(&typ, &instance, &name, &value); err != nil {
			return 0, 0, 0, cerrors.Trace(err)
		}
		switch name {
		case pdTSOUniqueIndexKey:
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return 0, 0, 0, cerrors.Trace(err)
			}
			if !uniqueSet {
				unique = parsed
				uniqueSet = true
				continue
			}
			if unique != parsed {
				return 0, 0, 0, cerrors.New("downstream TiDB reports inconsistent tso-unique-index across instances")
			}
		case pdTSOMaxIndexKey:
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return 0, 0, 0, cerrors.Trace(err)
			}
			if !maxSet {
				max = parsed
				maxSet = true
				continue
			}
			if max != parsed {
				return 0, 0, 0, cerrors.New("downstream TiDB reports inconsistent tso-max-index across instances")
			}
		default:
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, 0, cerrors.Trace(err)
	}

	if !uniqueSet {
		return 0, 0, 0, cerrors.Errorf("downstream TiDB does not report %s", pdTSOUniqueIndexKey)
	}
	if !maxSet {
		return 0, 0, 0, cerrors.Errorf("downstream TiDB does not report %s", pdTSOMaxIndexKey)
	}
	return unique, max, readTimeout, nil
}

func getUpstreamTSOIndexes(
	ctx context.Context,
	upPD pdClientForTSOIndexValidation,
	readTimeout time.Duration,
) (unique int64, max int64, err error) {
	if upPD == nil {
		return 0, 0, cerrors.New("pd client is nil")
	}

	endpoints, err := collectPDEndpointsFn(ctx, upPD)
	if err != nil {
		return 0, 0, cerrors.Trace(err)
	}
	if len(endpoints) == 0 {
		return 0, 0, cerrors.New("no pd endpoints available")
	}

	fetcher, err := newPDConfigFetcherFn(upPD)
	if err != nil {
		return 0, 0, cerrors.Trace(err)
	}
	defer fetcher.Close()

	var lastErr error
	for _, endpoint := range endpoints {
		attemptCtx, cancel := context.WithTimeout(ctx, readTimeout)
		cfg, err := fetcher.GetConfig(attemptCtx, endpoint)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}

		unique, err = parsePDConfigInt(cfg, pdTSOUniqueIndexKey)
		if err != nil {
			lastErr = err
			continue
		}
		max, err = parsePDConfigInt(cfg, pdTSOMaxIndexKey)
		if err != nil {
			lastErr = err
			continue
		}
		return unique, max, nil
	}
	if lastErr == nil {
		lastErr = cerrors.New("failed to read pd config from all endpoints")
	}
	return 0, 0, cerrors.Trace(lastErr)
}

func parsePDConfigInt(cfg map[string]any, key string) (int64, error) {
	v, ok := cfg[key]
	if !ok {
		return 0, cerrors.Errorf("pd config key not found: %s", key)
	}
	return toInt64(v)
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int64:
		return x, nil
	case uint64:
		if x > uint64(math.MaxInt64) {
			return 0, cerrors.New("value overflows int64")
		}
		return int64(x), nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) || math.Trunc(x) != x {
			return 0, cerrors.New("value is not an integer")
		}
		if x > float64(math.MaxInt64) || x < float64(math.MinInt64) {
			return 0, cerrors.New("value overflows int64")
		}
		return int64(x), nil
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			return 0, cerrors.Trace(err)
		}
		return n, nil
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return 0, cerrors.Trace(err)
		}
		return n, nil
	default:
		return 0, cerrors.Errorf("unsupported value type: %T", v)
	}
}
