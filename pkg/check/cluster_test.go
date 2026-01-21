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
	"errors"
	"net/url"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	mysqlsink "github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type mockPDClient struct {
	pd.Client
	clusterID uint64
}

func (m *mockPDClient) GetClusterID(ctx context.Context) uint64 {
	return m.clusterID
}

func TestGetClusterIDBySinkURI(t *testing.T) {
	// This test uses sqlmock to emulate different downstream flavors and failure modes,
	// and verifies we can determine TiDB `cluster_id` and (when available) the keyspace name.
	oldNewFn := newMySQLConfigAndDBFn
	defer func() { newMySQLConfigAndDBFn = oldNewFn }()

	changefeedCfg := &config.ChangefeedConfig{
		ChangefeedID: common.NewChangefeedID4Test("default", "test"),
		SinkURI:      "mysql://root@127.0.0.1:3306/",
	}

	t.Run("non-mysql scheme", func(t *testing.T) {
		called := false
		newMySQLConfigAndDBFn = func(context.Context, common.ChangeFeedID, *url.URL, *config.ChangefeedConfig) (*mysqlsink.Config, *sql.DB, error) {
			called = true
			return nil, nil, errors.New("should not be called")
		}

		id, keyspace, isTiDB, err := getClusterIDBySinkURI(context.Background(), "kafka://127.0.0.1:9092/topic", changefeedCfg)
		require.NoError(t, err)
		require.False(t, isTiDB)
		require.Equal(t, uint64(0), id)
		require.Equal(t, "", keyspace)
		require.False(t, called)
	})

	t.Run("invalid uri", func(t *testing.T) {
		id, keyspace, isTiDB, err := getClusterIDBySinkURI(context.Background(), "mysql://[::1", changefeedCfg)
		require.Error(t, err)
		code, ok := cerrors.RFCCode(err)
		require.True(t, ok)
		require.Equal(t, cerrors.ErrSinkURIInvalid.RFCCode(), code)
		require.False(t, isTiDB)
		require.Equal(t, uint64(0), id)
		require.Equal(t, "", keyspace)
	})

	t.Run("connect error", func(t *testing.T) {
		newMySQLConfigAndDBFn = func(context.Context, common.ChangeFeedID, *url.URL, *config.ChangefeedConfig) (*mysqlsink.Config, *sql.DB, error) {
			return nil, nil, errors.New("connect error")
		}

		_, _, _, err := getClusterIDBySinkURI(context.Background(), "mysql://root@127.0.0.1:3306/", changefeedCfg)
		require.ErrorContains(t, err, "connect error")
	})

	t.Run("not tidb", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectQuery("SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'cluster_id'").
			WillReturnError(errors.New("unknown table"))

		newMySQLConfigAndDBFn = func(context.Context, common.ChangeFeedID, *url.URL, *config.ChangefeedConfig) (*mysqlsink.Config, *sql.DB, error) {
			return &mysqlsink.Config{IsTiDB: false}, db, nil
		}

		id, keyspace, isTiDB, err := getClusterIDBySinkURI(context.Background(), "mysql://root@127.0.0.1:3306/", changefeedCfg)
		require.NoError(t, err)
		require.False(t, isTiDB)
		require.Equal(t, uint64(0), id)
		require.Equal(t, "", keyspace)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("cluster id available even if IsTiDB false", func(t *testing.T) {
		// Even if TiDB detection via `SELECT tidb_version()` fails, being able to read
		// `mysql.tidb.cluster_id` is a reliable indicator that the sink is TiDB.
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		rows := sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("12345")
		mock.ExpectQuery("SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'cluster_id'").
			WillReturnRows(rows)

		keyspaceRows := sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("tidb", "127.0.0.1:4000", "keyspace-name", "default")
		mock.ExpectQuery(regexp.QuoteMeta("show config where type = 'tidb' and name = 'keyspace-name'")).
			WillReturnRows(keyspaceRows)

		newMySQLConfigAndDBFn = func(context.Context, common.ChangeFeedID, *url.URL, *config.ChangefeedConfig) (*mysqlsink.Config, *sql.DB, error) {
			return &mysqlsink.Config{IsTiDB: false}, db, nil
		}

		id, keyspace, isTiDB, err := getClusterIDBySinkURI(context.Background(), "mysql://root@127.0.0.1:3306/", changefeedCfg)
		require.NoError(t, err)
		require.True(t, isTiDB)
		require.Equal(t, uint64(12345), id)
		require.Equal(t, "default", keyspace)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("tidb no cluster id", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		mock.ExpectQuery("SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'cluster_id'").
			WillReturnError(sql.ErrNoRows)

		newMySQLConfigAndDBFn = func(context.Context, common.ChangeFeedID, *url.URL, *config.ChangefeedConfig) (*mysqlsink.Config, *sql.DB, error) {
			return &mysqlsink.Config{IsTiDB: true}, db, nil
		}

		id, keyspace, isTiDB, err := getClusterIDBySinkURI(context.Background(), "mysql://root@127.0.0.1:3306/", changefeedCfg)
		require.NoError(t, err)
		require.False(t, isTiDB)
		require.Equal(t, uint64(0), id)
		require.Equal(t, "", keyspace)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("success", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		rows := sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("12345")
		mock.ExpectQuery("SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'cluster_id'").
			WillReturnRows(rows)

		keyspaceRows := sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("tidb", "127.0.0.1:4000", "keyspace-name", "default")
		mock.ExpectQuery(regexp.QuoteMeta("show config where type = 'tidb' and name = 'keyspace-name'")).
			WillReturnRows(keyspaceRows)

		newMySQLConfigAndDBFn = func(context.Context, common.ChangeFeedID, *url.URL, *config.ChangefeedConfig) (*mysqlsink.Config, *sql.DB, error) {
			return &mysqlsink.Config{IsTiDB: true}, db, nil
		}

		id, keyspace, isTiDB, err := getClusterIDBySinkURI(context.Background(), "mysql://root@127.0.0.1:3306/", changefeedCfg)
		require.NoError(t, err)
		require.True(t, isTiDB)
		require.Equal(t, uint64(12345), id)
		require.Equal(t, "default", keyspace)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestIsSameUpstreamDownstream(t *testing.T) {
	// This test verifies the tuple-based identity check:
	// - TiDB Classic: compare physical `cluster_id`
	// - TiDB Next-Gen: compare (cluster_id, keyspace) when keyspace is available.
	oldFn := GetGetClusterIDBySinkURIFn()
	defer func() { SetGetClusterIDBySinkURIFnForTest(oldFn) }()

	changefeedCfg := &config.ChangefeedConfig{
		ChangefeedID: common.NewChangefeedID4Test("default", "test"),
		SinkURI:      "mysql://root@127.0.0.1:3306/",
	}

	testCases := []struct {
		name         string
		upClusterID  uint64
		changefeedID common.ChangeFeedID
		mockDownFunc func(context.Context, string, *config.ChangefeedConfig) (uint64, string, bool, error)
		wantResult   bool
		wantErr      string
	}{
		{
			name:         "same cluster",
			upClusterID:  123,
			changefeedID: common.NewChangefeedID4Test("default", "test"),
			mockDownFunc: func(context.Context, string, *config.ChangefeedConfig) (uint64, string, bool, error) {
				return 123, "default", true, nil
			},
			wantResult: true,
		},
		{
			name:         "different cluster",
			upClusterID:  123,
			changefeedID: common.NewChangefeedID4Test("default", "test"),
			mockDownFunc: func(context.Context, string, *config.ChangefeedConfig) (uint64, string, bool, error) {
				return 456, "default", true, nil
			},
			wantResult: false,
		},
		{
			name:         "same physical cluster but different keyspace",
			upClusterID:  123,
			changefeedID: common.NewChangefeedID4Test("keyspace1", "test"),
			mockDownFunc: func(context.Context, string, *config.ChangefeedConfig) (uint64, string, bool, error) {
				return 123, "keyspace2", true, nil
			},
			wantResult: false,
		},
		{
			name:         "not tidb",
			changefeedID: common.NewChangefeedID4Test("default", "test"),
			mockDownFunc: func(context.Context, string, *config.ChangefeedConfig) (uint64, string, bool, error) {
				return 0, "", false, nil
			},
			wantResult: false,
		},
		{
			name:         "error case",
			changefeedID: common.NewChangefeedID4Test("default", "test"),
			mockDownFunc: func(context.Context, string, *config.ChangefeedConfig) (uint64, string, bool, error) {
				return 0, "", false, errors.New("mock error")
			},
			wantErr: "mock error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			changefeedCfg.ChangefeedID = tc.changefeedID
			SetGetClusterIDBySinkURIFnForTest(tc.mockDownFunc)
			mockPD := &mockPDClient{clusterID: tc.upClusterID}

			result, err := IsSameUpstreamDownstream(context.Background(), mockPD, changefeedCfg)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantResult, result)
		})
	}
}
