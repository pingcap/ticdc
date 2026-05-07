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
	"fmt"
	"math"
	"net/url"
	"strconv"
	"time"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pdutil"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
)

const (
	pdTSOUniqueIndexKey = "tso-unique-index"
	pdTSOMaxIndexKey    = "tso-max-index"
)

const showPDConfigQuery = "SHOW CONFIG WHERE type='pd' AND (name='tso-unique-index' OR name='tso-max-index')"

var newPDHTTPClientFn = func(upPD pd.Client) (pdhttp.Client, error) {
	sc := config.GetGlobalServerConfig()
	if sc == nil {
		return pdutil.NewPDHTTPClient(upPD, nil)
	}
	return pdutil.NewPDHTTPClient(upPD, sc.Security)
}

// ValidateActiveActiveTSOIndexes validates the upstream/downstream PD TSO index
// compatibility when active-active mode is enabled and the downstream is TiDB.
//
// The validation is fail-closed: inability to retrieve or parse values is
// treated as a validation failure.
func ValidateActiveActiveTSOIndexes(
	ctx context.Context,
	upPD pd.Client,
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
		return errors.WrapError(errors.ErrSinkURIInvalid, err, changefeedCfg.SinkURI)
	}
	if !config.IsMySQLCompatibleScheme(config.GetScheme(sinkURI)) {
		return nil
	}

	downUnique, downMax, err := getDownstreamTSOIndexes(ctx, changefeedCfg, sinkURI)
	if err != nil {
		return cerrors.WrapError(
			cerrors.ErrActiveActiveTSOIndexIncompatible,
			err,
			"failed to read downstream tso index config",
		)
	}

	upUnique, upMax, err := getUpstreamTSOIndexes(ctx, upPD)
	if err != nil {
		return cerrors.WrapError(
			cerrors.ErrActiveActiveTSOIndexIncompatible,
			err,
			fmt.Sprintf("failed to read upstream tso index config, downstream unique=%d, downstream max=%d",
				downUnique, downMax),
		)
	}

	// In active-active mode, `tso-unique-index` must be different between upstream and
	// downstream to avoid TSO collisions, while `tso-max-index` must match to guarantee
	// the same logical index range.
	if upUnique == downUnique {
		return cerrors.ErrActiveActiveTSOIndexIncompatible.GenWithStackByArgs(
			fmt.Sprintf("active active tso index mismatch, upstream and downstream share the same tso-unique-index=%d, upstream max=%d, downstream max=%d",
				upUnique, upMax, downMax),
		)
	}
	if upMax != downMax {
		return cerrors.ErrActiveActiveTSOIndexIncompatible.GenWithStackByArgs(
			fmt.Sprintf("active active tso index mismatch, upstream unique=%d, upstream max=%d, downstream unique=%d, downstream max=%d",
				upUnique, upMax, downUnique, downMax),
		)
	}
	return nil
}

// getDownstreamTSOIndexes reads the effective PD TSO index values from the
// downstream TiDB cluster via SQL.
//
// These values are part of PD configuration, and TiDB exposes them through
// `SHOW CONFIG`. The query returns one row per TiDB instance, so this function
// requires all instances to report the same values.
//
// The function is fail-closed: any retrieval, parsing, missing key, or
// cross-instance inconsistency is treated as an error.
func getDownstreamTSOIndexes(
	ctx context.Context,
	changefeedCfg *config.ChangefeedConfig,
	sinkURI *url.URL,
) (unique int64, max int64, err error) {
	if changefeedCfg == nil {
		return 0, 0, cerrors.New("changefeed config is nil")
	}

	mysqlCfg, db, err := newMySQLConfigAndDBFn(ctx, changefeedCfg.ChangefeedID, sinkURI, changefeedCfg)
	if err != nil {
		return 0, 0, cerrors.Trace(err)
	}
	defer func() { _ = db.Close() }()

	readTimeout, err := time.ParseDuration(mysqlCfg.ReadTimeout)
	if err != nil {
		return 0, 0, cerrors.Trace(err)
	}

	// Bound the downstream query by the sink read timeout to keep the validation
	// latency aligned with downstream connectivity expectations.
	queryCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()

	rows, err := db.QueryContext(queryCtx, showPDConfigQuery)
	if err != nil {
		return 0, 0, cerrors.Trace(err)
	}
	defer func() { _ = rows.Close() }()

	var (
		uniqueSet bool
		maxSet    bool
	)
	// SHOW CONFIG returns one row per TiDB instance. Require all instances to
	// report consistent values to avoid validating against a partially rolled-out
	// or inconsistent configuration.
	for rows.Next() {
		// Columns: Type | Instance | Name | Value
		var typ, instance, name, value string
		if err := rows.Scan(&typ, &instance, &name, &value); err != nil {
			return 0, 0, cerrors.Trace(err)
		}
		switch name {
		case pdTSOUniqueIndexKey:
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return 0, 0, cerrors.Trace(err)
			}
			if !uniqueSet {
				unique = parsed
				uniqueSet = true
				continue
			}
			if unique != parsed {
				return 0, 0, cerrors.New("downstream TiDB reports inconsistent tso-unique-index across instances")
			}
		case pdTSOMaxIndexKey:
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return 0, 0, cerrors.Trace(err)
			}
			if !maxSet {
				max = parsed
				maxSet = true
				continue
			}
			if max != parsed {
				return 0, 0, cerrors.New("downstream TiDB reports inconsistent tso-max-index across instances")
			}
		default:
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, cerrors.Trace(err)
	}

	if !uniqueSet {
		return 0, 0, cerrors.Errorf("downstream TiDB does not report %s", pdTSOUniqueIndexKey)
	}
	if !maxSet {
		return 0, 0, cerrors.Errorf("downstream TiDB does not report %s", pdTSOMaxIndexKey)
	}
	return unique, max, nil
}

// getUpstreamTSOIndexes reads the PD TSO index values from the upstream PD via
// PD HTTP API.
//
// The PD HTTP client uses the same service discovery (and TLS configuration) as
// the given gRPC PD client. It also probes leader and followers internally, so a
// single GetConfig call is sufficient here.
//
// The caller controls the request deadline through ctx.
func getUpstreamTSOIndexes(
	ctx context.Context,
	upPD pd.Client,
) (unique int64, max int64, err error) {
	if upPD == nil {
		return 0, 0, cerrors.New("pd client is nil")
	}

	httpClient, err := newPDHTTPClientFn(upPD)
	if err != nil {
		return 0, 0, cerrors.Trace(err)
	}
	defer httpClient.Close()

	cfg, err := httpClient.GetConfig(ctx)
	if err != nil {
		return 0, 0, cerrors.Trace(err)
	}

	unique, err = parsePDConfigInt64(cfg, pdTSOUniqueIndexKey)
	if err != nil {
		return 0, 0, cerrors.Trace(err)
	}
	max, err = parsePDConfigInt64(cfg, pdTSOMaxIndexKey)
	if err != nil {
		return 0, 0, cerrors.Trace(err)
	}
	return unique, max, nil
}

func parsePDConfigInt64(cfg map[string]any, key string) (int64, error) {
	v, ok := cfg[key]
	if !ok {
		return 0, cerrors.Errorf("pd config key not found: %s", key)
	}

	// PD stores `tso-unique-index` and `tso-max-index` as int64 values.
	// The PD HTTP client unmarshals the JSON response into map[string]any,
	// and encoding/json decodes JSON numbers as float64 in that case.
	// Keep the conversion strict and fail-closed.
	switch x := v.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case float64:
		// When a JSON number is decoded into an `any`, it becomes float64.
		// float64 cannot precisely represent all int64 values (it is exact only
		// for integers up to 2^53). For a strict conversion, reject values beyond
		// that range to avoid precision loss and implementation-defined behavior
		// on overflow.
		const maxExactIntInFloat64 = float64(1 << 53)
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return 0, cerrors.New("value is not a finite number")
		}
		if math.Trunc(x) != x {
			return 0, cerrors.New("value is not an integer")
		}
		if x > maxExactIntInFloat64 || x < -maxExactIntInFloat64 {
			return 0, cerrors.Errorf("value for %s exceeds exact integer range for float64", key)
		}
		return int64(x), nil
	default:
		return 0, cerrors.Errorf("unexpected value type for %s: %T", key, v)
	}
}
