package check

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
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

		id, isTiDB, err := getClusterIDBySinkURI(context.Background(), "kafka://127.0.0.1:9092/topic", changefeedCfg)
		require.NoError(t, err)
		require.False(t, isTiDB)
		require.Equal(t, uint64(0), id)
		require.False(t, called)
	})

	t.Run("invalid uri", func(t *testing.T) {
		id, isTiDB, err := getClusterIDBySinkURI(context.Background(), "mysql://[::1", changefeedCfg)
		require.Error(t, err)
		code, ok := cerrors.RFCCode(err)
		require.True(t, ok)
		require.Equal(t, cerrors.ErrSinkURIInvalid.RFCCode(), code)
		require.False(t, isTiDB)
		require.Equal(t, uint64(0), id)
	})

	t.Run("connect error", func(t *testing.T) {
		newMySQLConfigAndDBFn = func(context.Context, common.ChangeFeedID, *url.URL, *config.ChangefeedConfig) (*mysqlsink.Config, *sql.DB, error) {
			return nil, nil, errors.New("connect error")
		}

		_, _, err := getClusterIDBySinkURI(context.Background(), "mysql://root@127.0.0.1:3306/", changefeedCfg)
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

		id, isTiDB, err := getClusterIDBySinkURI(context.Background(), "mysql://root@127.0.0.1:3306/", changefeedCfg)
		require.NoError(t, err)
		require.False(t, isTiDB)
		require.Equal(t, uint64(0), id)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("cluster id available even if IsTiDB false", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		rows := sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("12345")
		mock.ExpectQuery("SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'cluster_id'").
			WillReturnRows(rows)

		newMySQLConfigAndDBFn = func(context.Context, common.ChangeFeedID, *url.URL, *config.ChangefeedConfig) (*mysqlsink.Config, *sql.DB, error) {
			return &mysqlsink.Config{IsTiDB: false}, db, nil
		}

		id, isTiDB, err := getClusterIDBySinkURI(context.Background(), "mysql://root@127.0.0.1:3306/", changefeedCfg)
		require.NoError(t, err)
		require.True(t, isTiDB)
		require.Equal(t, uint64(12345), id)
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

		id, isTiDB, err := getClusterIDBySinkURI(context.Background(), "mysql://root@127.0.0.1:3306/", changefeedCfg)
		require.NoError(t, err)
		require.False(t, isTiDB)
		require.Equal(t, uint64(0), id)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("success", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		rows := sqlmock.NewRows([]string{"VARIABLE_VALUE"}).AddRow("12345")
		mock.ExpectQuery("SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME = 'cluster_id'").
			WillReturnRows(rows)

		newMySQLConfigAndDBFn = func(context.Context, common.ChangeFeedID, *url.URL, *config.ChangefeedConfig) (*mysqlsink.Config, *sql.DB, error) {
			return &mysqlsink.Config{IsTiDB: true}, db, nil
		}

		id, isTiDB, err := getClusterIDBySinkURI(context.Background(), "mysql://root@127.0.0.1:3306/", changefeedCfg)
		require.NoError(t, err)
		require.True(t, isTiDB)
		require.Equal(t, uint64(12345), id)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestUpstreamDownstreamNotSame(t *testing.T) {
	oldFn := GetGetClusterIDBySinkURIFn()
	defer func() { SetGetClusterIDBySinkURIFnForTest(oldFn) }()

	changefeedCfg := &config.ChangefeedConfig{
		ChangefeedID: common.NewChangefeedID4Test("default", "test"),
		SinkURI:      "mysql://root@127.0.0.1:3306/",
	}

	testCases := []struct {
		name         string
		upClusterID  uint64
		mockDownFunc func(context.Context, string, *config.ChangefeedConfig) (uint64, bool, error)
		wantResult   bool
		wantErr      string
	}{
		{
			name:        "same cluster",
			upClusterID: 123,
			mockDownFunc: func(context.Context, string, *config.ChangefeedConfig) (uint64, bool, error) {
				return 123, true, nil
			},
			wantResult: false,
		},
		{
			name:        "different cluster",
			upClusterID: 123,
			mockDownFunc: func(context.Context, string, *config.ChangefeedConfig) (uint64, bool, error) {
				return 456, true, nil
			},
			wantResult: true,
		},
		{
			name: "not tidb",
			mockDownFunc: func(context.Context, string, *config.ChangefeedConfig) (uint64, bool, error) {
				return 0, false, nil
			},
			wantResult: true,
		},
		{
			name: "error case",
			mockDownFunc: func(context.Context, string, *config.ChangefeedConfig) (uint64, bool, error) {
				return 0, false, errors.New("mock error")
			},
			wantErr: "mock error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			SetGetClusterIDBySinkURIFnForTest(tc.mockDownFunc)
			mockPD := &mockPDClient{clusterID: tc.upClusterID}

			result, err := UpstreamDownstreamNotSame(context.Background(), mockPD, changefeedCfg)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantResult, result)
		})
	}
}
