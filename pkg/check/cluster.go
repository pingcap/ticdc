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
	"database/sql"
	"net/url"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/ticdc/pkg/util"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	newMySQLConfigAndDBFn   = mysql.NewMysqlConfigAndDB
	getClusterIDBySinkURIFn = getClusterIDBySinkURI
)

// IsSameUpstreamDownstream returns whether the upstream and downstream are the same TiDB logical cluster.
//
// Why this exists: TiCDC does not support using the same TiDB logical cluster as both upstream and downstream,
// because it can easily lead to self-replication loops.
//
// Compatibility and trade-offs:
//   - If the sink is not TiDB, or the downstream `cluster_id` is unavailable, this returns false (keep legacy behavior).
//   - In TiDB Next-Gen, multiple keyspaces share the same physical `cluster_id`. When the downstream keyspace
//     can be determined, we treat (cluster_id, keyspace) as the cluster identity to allow cross-keyspace replication.
//   - If the downstream keyspace cannot be determined while `cluster_id` matches, we conservatively return true
//     to avoid accidental self-replication.
func IsSameUpstreamDownstream(
	ctx context.Context, upPD pd.Client, changefeedCfg *config.ChangefeedConfig,
) (bool, error) {
	if upPD == nil {
		return false, cerrors.New("pd client is nil")
	}
	if changefeedCfg == nil {
		return false, cerrors.New("changefeed config is nil")
	}

	upID := upPD.GetClusterID(ctx)
	upKeyspace := changefeedCfg.ChangefeedID.Keyspace()
	if upKeyspace == "" {
		upKeyspace = common.DefaultKeyspaceName
	}

	downID, downKeyspace, isTiDB, err := getClusterIDBySinkURIFn(ctx, changefeedCfg.SinkURI, changefeedCfg)
	log.Debug("check upstream and downstream cluster",
		zap.Uint64("upID", upID),
		zap.String("upKeyspace", upKeyspace),
		zap.Uint64("downID", downID),
		zap.String("downKeyspace", downKeyspace),
		zap.Bool("isTiDB", isTiDB),
		zap.String("sinkURI", util.MaskSensitiveDataInURI(changefeedCfg.SinkURI)))
	if err != nil {
		log.Error("failed to get cluster ID from sink URI",
			zap.String("sinkURI", util.MaskSensitiveDataInURI(changefeedCfg.SinkURI)),
			zap.Error(err))
		return false, cerrors.Trace(err)
	}
	if !isTiDB {
		return false, nil
	}

	// In TiDB Classic, `cluster_id` identifies the whole cluster. In TiDB Next-Gen, a physical
	// cluster can contain multiple logical clusters (keyspaces), and all keyspaces share the same
	// `cluster_id`. To avoid blocking valid cross-keyspace replication, treat (cluster_id, keyspace)
	// as the cluster identity when both sides are TiDB and keyspace is available.
	if upID != downID {
		return false, nil
	}
	if downKeyspace == "" {
		// If the downstream keyspace can't be determined, keep the legacy behavior: treat the same
		// physical cluster as "same" to prevent accidental self-replication loops.
		return true, nil
	}
	return strings.EqualFold(upKeyspace, downKeyspace), nil
}

// getClusterIDBySinkURI gets the downstream TiDB cluster ID and keyspace name by the sink URI.
// Returns the cluster ID, keyspace name (empty if unavailable), whether it is a TiDB cluster,
// and an error.
func getClusterIDBySinkURI(
	ctx context.Context, sinkURI string, changefeedCfg *config.ChangefeedConfig,
) (uint64, string, bool, error) {
	uri, err := url.Parse(sinkURI)
	if err != nil {
		return 0, "", false, cerrors.WrapError(cerrors.ErrSinkURIInvalid, err, sinkURI)
	}

	scheme := config.GetScheme(uri)
	if !config.IsMySQLCompatibleScheme(scheme) {
		return 0, "", false, nil
	}

	_, db, err := newMySQLConfigAndDBFn(ctx, changefeedCfg.ChangefeedID, uri, changefeedCfg)
	if err != nil {
		return 0, "", true, cerrors.Trace(err)
	}
	defer func() { _ = db.Close() }()

	// NOTE: Do not rely on `Config.IsTiDB` only. `IsTiDB` is determined by
	// `SELECT tidb_version()`, which may return false if the query fails (for example,
	// transient errors). Instead, try to read `cluster_id` directly. If we can read it,
	// we can safely treat the sink as TiDB for this check.

	row := db.QueryRowContext(ctx,
		"SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'cluster_id'")
	var clusterIDStr string
	err = row.Scan(&clusterIDStr)
	if err != nil {
		// If the cluster ID is unavailable (for example, legacy TiDB),
		// do not block creating/updating/resuming a changefeed.
		return 0, "", false, nil
	}
	clusterID, err := strconv.ParseUint(clusterIDStr, 10, 64)
	if err != nil {
		return 0, "", true, cerrors.Trace(err)
	}

	keyspace, err := getTiDBKeyspaceName(ctx, db)
	if err != nil {
		// The keyspace is only used to distinguish logical clusters in Next-Gen.
		// If it can't be determined, fall back to the legacy physical-cluster-only check.
		log.Debug("failed to get downstream TiDB keyspace name",
			zap.String("sinkURI", util.MaskSensitiveDataInURI(sinkURI)),
			zap.Error(err))
		keyspace = ""
	}
	return clusterID, keyspace, true, nil
}

func getTiDBKeyspaceName(ctx context.Context, db *sql.DB) (string, error) {
	// TiDB exposes the current keyspace via `SHOW CONFIG`. It returns one row per TiDB instance.
	// For this check we only need the keyspace name, and we expect all instances to agree.
	rows, err := db.QueryContext(ctx,
		"show config where type = 'tidb' and name = 'keyspace-name'")
	if err != nil {
		return "", cerrors.Trace(err)
	}
	defer func() { _ = rows.Close() }()

	var keyspace string
	var hasRow bool
	for rows.Next() {
		// Columns: Type | Instance | Name | Value
		var typ, instance, name, value string
		if err := rows.Scan(&typ, &instance, &name, &value); err != nil {
			return "", cerrors.Trace(err)
		}
		if !hasRow {
			keyspace = value
			hasRow = true
			continue
		}
		if value != keyspace {
			return "", cerrors.New("downstream TiDB reports inconsistent keyspace-name across instances")
		}
	}
	if err := rows.Err(); err != nil {
		return "", cerrors.Trace(err)
	}
	if !hasRow {
		return "", cerrors.Trace(sql.ErrNoRows)
	}
	return keyspace, nil
}

// GetGetClusterIDBySinkURIFn returns the getClusterIDBySinkURIFn function. It is used for testing.
func GetGetClusterIDBySinkURIFn() func(context.Context, string, *config.ChangefeedConfig) (uint64, string, bool, error) {
	return getClusterIDBySinkURIFn
}

// SetGetClusterIDBySinkURIFnForTest sets the getClusterIDBySinkURIFn function for testing.
func SetGetClusterIDBySinkURIFnForTest(fn func(context.Context, string, *config.ChangefeedConfig) (uint64, string, bool, error)) {
	getClusterIDBySinkURIFn = fn
}

// SetNewMySQLConfigAndDBFnForTest sets the MySQL connection factory function for testing.
func SetNewMySQLConfigAndDBFnForTest(
	fn func(context.Context, common.ChangeFeedID, *url.URL, *config.ChangefeedConfig) (*mysql.Config, *sql.DB, error),
) {
	newMySQLConfigAndDBFn = fn
}
