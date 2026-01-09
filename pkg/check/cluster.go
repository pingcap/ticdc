package check

import (
	"context"
	"database/sql"
	"net/url"
	"strconv"

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

// UpstreamDownstreamNotSame checks whether the upstream and downstream are not the same cluster.
// It will return true if downstream is not a TiDB cluster, or downstream cluster ID is unavailable.
func UpstreamDownstreamNotSame(
	ctx context.Context, upPD pd.Client, changefeedCfg *config.ChangefeedConfig,
) (bool, error) {
	if upPD == nil {
		return false, cerrors.New("pd client is nil")
	}
	if changefeedCfg == nil {
		return false, cerrors.New("changefeed config is nil")
	}

	upID := upPD.GetClusterID(ctx)
	downID, isTiDB, err := getClusterIDBySinkURIFn(ctx, changefeedCfg.SinkURI, changefeedCfg)
	log.Debug("check upstream and downstream cluster",
		zap.Uint64("upID", upID),
		zap.Uint64("downID", downID),
		zap.Bool("isTiDB", isTiDB),
		zap.String("sinkURI", util.MaskSensitiveDataInURI(changefeedCfg.SinkURI)))
	if err != nil {
		log.Error("failed to get cluster ID from sink URI",
			zap.String("sinkURI", util.MaskSensitiveDataInURI(changefeedCfg.SinkURI)),
			zap.Error(err))
		return false, cerrors.Trace(err)
	}
	if !isTiDB {
		return true, nil
	}
	return upID != downID, nil
}

// getClusterIDBySinkURI gets the cluster ID by the sink URI.
// Returns the cluster ID, whether it is a TiDB cluster, and an error.
func getClusterIDBySinkURI(
	ctx context.Context, sinkURI string, changefeedCfg *config.ChangefeedConfig,
) (uint64, bool, error) {
	uri, err := url.Parse(sinkURI)
	if err != nil {
		return 0, false, cerrors.WrapError(cerrors.ErrSinkURIInvalid, err, sinkURI)
	}

	scheme := config.GetScheme(uri)
	if !config.IsMySQLCompatibleScheme(scheme) {
		return 0, false, nil
	}

	_, db, err := newMySQLConfigAndDBFn(ctx, changefeedCfg.ChangefeedID, uri, changefeedCfg)
	if err != nil {
		return 0, true, cerrors.Trace(err)
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
		return 0, false, nil
	}
	clusterID, err := strconv.ParseUint(clusterIDStr, 10, 64)
	if err != nil {
		return 0, true, cerrors.Trace(err)
	}
	return clusterID, true, nil
}

// GetGetClusterIDBySinkURIFn returns the getClusterIDBySinkURIFn function. It is used for testing.
func GetGetClusterIDBySinkURIFn() func(context.Context, string, *config.ChangefeedConfig) (uint64, bool, error) {
	return getClusterIDBySinkURIFn
}

// SetGetClusterIDBySinkURIFnForTest sets the getClusterIDBySinkURIFn function for testing.
func SetGetClusterIDBySinkURIFnForTest(fn func(context.Context, string, *config.ChangefeedConfig) (uint64, bool, error)) {
	getClusterIDBySinkURIFn = fn
}

// SetNewMySQLConfigAndDBFnForTest sets the MySQL connection factory function for testing.
func SetNewMySQLConfigAndDBFnForTest(
	fn func(context.Context, common.ChangeFeedID, *url.URL, *config.ChangefeedConfig) (*mysql.Config, *sql.DB, error),
) {
	newMySQLConfigAndDBFn = fn
}
