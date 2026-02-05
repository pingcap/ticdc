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
	pdhttp "github.com/tikv/pd/client/http"
)

type mockPDClientForTSOIndexValidation struct {
	pd.Client
}

type mockPDHTTPClient struct {
	pdhttp.Client
	getConfig func(ctx context.Context) (map[string]any, error)
}

func (m *mockPDHTTPClient) GetConfig(ctx context.Context) (map[string]any, error) {
	return m.getConfig(ctx)
}

func (m *mockPDHTTPClient) Close() {}

func TestValidateActiveActiveTSOIndexes_SkipWhenDisabled(t *testing.T) {
	err := ValidateActiveActiveTSOIndexes(context.Background(), nil, &config.ChangefeedConfig{
		SinkURI:            "mysql://127.0.0.1:3306/",
		EnableActiveActive: false,
	})
	require.NoError(t, err)
}

func TestValidateActiveActiveTSOIndexes_SkipNonMySQLScheme(t *testing.T) {
	err := ValidateActiveActiveTSOIndexes(context.Background(), nil, &config.ChangefeedConfig{
		SinkURI:            "kafka://127.0.0.1:9092/topic",
		EnableActiveActive: true,
	})
	require.NoError(t, err)
}

func TestValidateActiveActiveTSOIndexes_DownstreamMissingKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(regexp.QuoteMeta(showPDConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("pd", "tidb-0", pdTSOUniqueIndexKey, "1"))

	setTestDeps(t, db, nil)

	err = ValidateActiveActiveTSOIndexes(context.Background(), nil, &config.ChangefeedConfig{
		ChangefeedID:       common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName),
		SinkURI:            "mysql://127.0.0.1:3306/",
		EnableActiveActive: true,
	})
	require.Error(t, err)
	code, ok := cerrors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerrors.ErrActiveActiveTSOIndexIncompatible.RFCCode(), code)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestValidateActiveActiveTSOIndexes_DownstreamInconsistentAcrossInstances(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(regexp.QuoteMeta(showPDConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("pd", "tidb-0", pdTSOUniqueIndexKey, "1").
			AddRow("pd", "tidb-1", pdTSOUniqueIndexKey, "2").
			AddRow("pd", "tidb-0", pdTSOMaxIndexKey, "4").
			AddRow("pd", "tidb-1", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db, nil)

	err = ValidateActiveActiveTSOIndexes(context.Background(), nil, &config.ChangefeedConfig{
		ChangefeedID:       common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName),
		SinkURI:            "mysql://127.0.0.1:3306/",
		EnableActiveActive: true,
	})
	require.Error(t, err)
	code, ok := cerrors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerrors.ErrActiveActiveTSOIndexIncompatible.RFCCode(), code)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestValidateActiveActiveTSOIndexes_UpstreamReadError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(regexp.QuoteMeta(showPDConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("pd", "tidb-0", pdTSOUniqueIndexKey, "1").
			AddRow("pd", "tidb-0", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db, func(ctx context.Context) (map[string]any, error) {
		return nil, errors.New("boom")
	})

	err = ValidateActiveActiveTSOIndexes(context.Background(), &mockPDClientForTSOIndexValidation{}, &config.ChangefeedConfig{
		ChangefeedID:       common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName),
		SinkURI:            "mysql://127.0.0.1:3306/",
		EnableActiveActive: true,
	})
	require.Error(t, err)
	code, ok := cerrors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerrors.ErrActiveActiveTSOIndexIncompatible.RFCCode(), code)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestValidateActiveActiveTSOIndexes_UpstreamMissingKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(regexp.QuoteMeta(showPDConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("pd", "tidb-0", pdTSOUniqueIndexKey, "1").
			AddRow("pd", "tidb-0", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db, func(ctx context.Context) (map[string]any, error) {
		return map[string]any{
			pdTSOUniqueIndexKey: float64(2),
		}, nil
	})

	err = ValidateActiveActiveTSOIndexes(context.Background(), &mockPDClientForTSOIndexValidation{}, &config.ChangefeedConfig{
		ChangefeedID:       common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName),
		SinkURI:            "mysql://127.0.0.1:3306/",
		EnableActiveActive: true,
	})
	require.Error(t, err)
	code, ok := cerrors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerrors.ErrActiveActiveTSOIndexIncompatible.RFCCode(), code)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestValidateActiveActiveTSOIndexes_Mismatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(regexp.QuoteMeta(showPDConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("pd", "tidb-0", pdTSOUniqueIndexKey, "1").
			AddRow("pd", "tidb-0", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db, func(ctx context.Context) (map[string]any, error) {
		return map[string]any{
			pdTSOUniqueIndexKey: float64(1),
			pdTSOMaxIndexKey:    float64(4),
		}, nil
	})

	err = ValidateActiveActiveTSOIndexes(context.Background(), &mockPDClientForTSOIndexValidation{}, &config.ChangefeedConfig{
		ChangefeedID:       common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName),
		SinkURI:            "mysql://127.0.0.1:3306/",
		EnableActiveActive: true,
	})
	require.Error(t, err)
	code, ok := cerrors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerrors.ErrActiveActiveTSOIndexIncompatible.RFCCode(), code)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestValidateActiveActiveTSOIndexes_MaxIndexMismatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(regexp.QuoteMeta(showPDConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("pd", "tidb-0", pdTSOUniqueIndexKey, "1").
			AddRow("pd", "tidb-0", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db, func(ctx context.Context) (map[string]any, error) {
		return map[string]any{
			pdTSOUniqueIndexKey: float64(2),
			pdTSOMaxIndexKey:    float64(5),
		}, nil
	})

	err = ValidateActiveActiveTSOIndexes(context.Background(), &mockPDClientForTSOIndexValidation{}, &config.ChangefeedConfig{
		ChangefeedID:       common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName),
		SinkURI:            "mysql://127.0.0.1:3306/",
		EnableActiveActive: true,
	})
	require.Error(t, err)
	code, ok := cerrors.RFCCode(err)
	require.True(t, ok)
	require.Equal(t, cerrors.ErrActiveActiveTSOIndexIncompatible.RFCCode(), code)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestValidateActiveActiveTSOIndexes_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(regexp.QuoteMeta(showPDConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("pd", "tidb-0", pdTSOUniqueIndexKey, "1").
			AddRow("pd", "tidb-0", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db, func(ctx context.Context) (map[string]any, error) {
		return map[string]any{
			pdTSOUniqueIndexKey: float64(2),
			pdTSOMaxIndexKey:    float64(4),
		}, nil
	})

	err = ValidateActiveActiveTSOIndexes(context.Background(), &mockPDClientForTSOIndexValidation{}, &config.ChangefeedConfig{
		ChangefeedID:       common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName),
		SinkURI:            "mysql://127.0.0.1:3306/",
		EnableActiveActive: true,
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func setTestDeps(
	t *testing.T,
	db *sql.DB,
	getConfig func(ctx context.Context) (map[string]any, error),
) {
	t.Helper()

	oldMySQLFn := newMySQLConfigAndDBFn
	oldNewHTTPClientFn := newPDHTTPClientFn

	newMySQLConfigAndDBFn = func(
		ctx context.Context,
		changefeedID common.ChangeFeedID,
		sinkURI *url.URL,
		cfg *config.ChangefeedConfig,
	) (*mysqlsink.Config, *sql.DB, error) {
		return &mysqlsink.Config{ReadTimeout: "1s"}, db, nil
	}
	newPDHTTPClientFn = func(upPD pd.Client) (pdhttp.Client, error) {
		if getConfig == nil {
			return &mockPDHTTPClient{
				getConfig: func(ctx context.Context) (map[string]any, error) {
					return nil, errors.New("unexpected GetConfig call")
				},
			}, nil
		}
		return &mockPDHTTPClient{getConfig: getConfig}, nil
	}

	t.Cleanup(func() {
		newMySQLConfigAndDBFn = oldMySQLFn
		newPDHTTPClientFn = oldNewHTTPClientFn
	})
}
