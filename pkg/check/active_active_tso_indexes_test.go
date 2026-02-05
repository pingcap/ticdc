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
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	mysqlsink "github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/servicediscovery"
)

type mockPDForTSOIndexValidation struct{}

func (m *mockPDForTSOIndexValidation) GetAllMembers(ctx context.Context) (*pdpb.GetMembersResponse, error) {
	return nil, nil
}

func (m *mockPDForTSOIndexValidation) GetServiceDiscovery() servicediscovery.ServiceDiscovery {
	return nil
}

type fakePDConfigFetcher struct {
	getConfig func(ctx context.Context, endpoint string) (map[string]any, error)
}

func (f *fakePDConfigFetcher) GetConfig(ctx context.Context, endpoint string) (map[string]any, error) {
	return f.getConfig(ctx, endpoint)
}

func (f *fakePDConfigFetcher) Close() {}

func TestValidateActiveActiveTSOIndexes_SkipWhenDisabled(t *testing.T) {
	err := ValidateActiveActiveTSOIndexes(context.Background(), &mockPDForTSOIndexValidation{}, &config.ChangefeedConfig{
		SinkURI:            "mysql://127.0.0.1:3306/",
		EnableActiveActive: false,
	})
	require.NoError(t, err)
}

func TestValidateActiveActiveTSOIndexes_SkipNonMySQLScheme(t *testing.T) {
	err := ValidateActiveActiveTSOIndexes(context.Background(), &mockPDForTSOIndexValidation{}, &config.ChangefeedConfig{
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

	restore := setTestHooks(t, testHooks{
		newMySQLConfigAndDB: func(ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, cfg *config.ChangefeedConfig) (*mysqlsink.Config, *sql.DB, error) {
			return &mysqlsink.Config{ReadTimeout: "1s"}, db, nil
		},
	})
	defer restore()

	err = ValidateActiveActiveTSOIndexes(context.Background(), &mockPDForTSOIndexValidation{}, &config.ChangefeedConfig{
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

func TestValidateActiveActiveTSOIndexes_UpstreamProbeSecondSucceeds(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(regexp.QuoteMeta(showPDConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("pd", "tidb-0", pdTSOUniqueIndexKey, "1").
			AddRow("pd", "tidb-0", pdTSOMaxIndexKey, "4"))

	var calls []string
	restore := setTestHooks(t, testHooks{
		newMySQLConfigAndDB: func(ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, cfg *config.ChangefeedConfig) (*mysqlsink.Config, *sql.DB, error) {
			return &mysqlsink.Config{ReadTimeout: "1s"}, db, nil
		},
		collectPDEndpoints: func(ctx context.Context, upPD pdClientForTSOIndexValidation) ([]string, error) {
			return []string{"http://pd1", "http://pd2"}, nil
		},
		newPDConfigFetcher: func(upPD pdClientForTSOIndexValidation) (pdConfigFetcher, error) {
			return &fakePDConfigFetcher{
				getConfig: func(ctx context.Context, endpoint string) (map[string]any, error) {
					calls = append(calls, endpoint)
					if endpoint == "http://pd1" {
						return nil, errors.New("boom")
					}
					return map[string]any{
						pdTSOUniqueIndexKey: float64(2),
						pdTSOMaxIndexKey:    float64(4),
					}, nil
				},
			}, nil
		},
	})
	defer restore()

	err = ValidateActiveActiveTSOIndexes(context.Background(), &mockPDForTSOIndexValidation{}, &config.ChangefeedConfig{
		ChangefeedID:       common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName),
		SinkURI:            "mysql://127.0.0.1:3306/",
		EnableActiveActive: true,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"http://pd1", "http://pd2"}, calls)
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

	restore := setTestHooks(t, testHooks{
		newMySQLConfigAndDB: func(ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, cfg *config.ChangefeedConfig) (*mysqlsink.Config, *sql.DB, error) {
			return &mysqlsink.Config{ReadTimeout: "1s"}, db, nil
		},
		collectPDEndpoints: func(ctx context.Context, upPD pdClientForTSOIndexValidation) ([]string, error) {
			return []string{"http://pd1"}, nil
		},
		newPDConfigFetcher: func(upPD pdClientForTSOIndexValidation) (pdConfigFetcher, error) {
			return &fakePDConfigFetcher{
				getConfig: func(ctx context.Context, endpoint string) (map[string]any, error) {
					return map[string]any{
						pdTSOUniqueIndexKey: float64(1),
						pdTSOMaxIndexKey:    float64(4),
					}, nil
				},
			}, nil
		},
	})
	defer restore()

	err = ValidateActiveActiveTSOIndexes(context.Background(), &mockPDForTSOIndexValidation{}, &config.ChangefeedConfig{
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

type testHooks struct {
	newMySQLConfigAndDB func(context.Context, common.ChangeFeedID, *url.URL, *config.ChangefeedConfig) (*mysqlsink.Config, *sql.DB, error)
	collectPDEndpoints  func(context.Context, pdClientForTSOIndexValidation) ([]string, error)
	newPDConfigFetcher  func(pdClientForTSOIndexValidation) (pdConfigFetcher, error)
}

func setTestHooks(t *testing.T, hooks testHooks) (restore func()) {
	t.Helper()

	oldMySQLFn := newMySQLConfigAndDBFn
	oldCollectEndpointsFn := collectPDEndpointsFn
	oldNewFetcherFn := newPDConfigFetcherFn

	if hooks.newMySQLConfigAndDB != nil {
		newMySQLConfigAndDBFn = hooks.newMySQLConfigAndDB
	}
	if hooks.collectPDEndpoints != nil {
		collectPDEndpointsFn = hooks.collectPDEndpoints
	}
	if hooks.newPDConfigFetcher != nil {
		newPDConfigFetcherFn = hooks.newPDConfigFetcher
	}

	return func() {
		newMySQLConfigAndDBFn = oldMySQLFn
		collectPDEndpointsFn = oldCollectEndpointsFn
		newPDConfigFetcherFn = oldNewFetcherFn
	}
}
