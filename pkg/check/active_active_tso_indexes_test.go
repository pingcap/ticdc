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
	"github.com/pingcap/ticdc/pkg/httputil"
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
	getConfig              func(ctx context.Context) (map[string]any, error)
	getMicroserviceMembers func(ctx context.Context, service string) ([]pdhttp.MicroserviceMember, error)
}

func (m *mockPDHTTPClient) GetConfig(ctx context.Context) (map[string]any, error) {
	return m.getConfig(ctx)
}

func (m *mockPDHTTPClient) GetMicroserviceMembers(ctx context.Context, service string) ([]pdhttp.MicroserviceMember, error) {
	return m.getMicroserviceMembers(ctx, service)
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

	mock.ExpectQuery(regexp.QuoteMeta(showTSOConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("tso", "tso-0", pdTSOUniqueIndexKey, "1"))

	setTestDeps(t, db, nil, nil, nil)

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

	mock.ExpectQuery(regexp.QuoteMeta(showTSOConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("tso", "tso-0", pdTSOUniqueIndexKey, "1").
			AddRow("tso", "tso-1", pdTSOUniqueIndexKey, "2").
			AddRow("tso", "tso-0", pdTSOMaxIndexKey, "4").
			AddRow("tso", "tso-1", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db, nil, nil, nil)

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

	mock.ExpectQuery(regexp.QuoteMeta(showTSOConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("tso", "tso-0", pdTSOUniqueIndexKey, "1").
			AddRow("tso", "tso-0", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db,
		func(ctx context.Context) (map[string]any, error) {
			return nil, errors.New("boom")
		},
		nil,
		nil,
	)

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

	mock.ExpectQuery(regexp.QuoteMeta(showTSOConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("tso", "tso-0", pdTSOUniqueIndexKey, "1").
			AddRow("tso", "tso-0", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db,
		func(ctx context.Context) (map[string]any, error) {
			return map[string]any{
				pdTSOUniqueIndexKey: float64(2),
			}, nil
		},
		nil,
		nil,
	)

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

	mock.ExpectQuery(regexp.QuoteMeta(showTSOConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("tso", "tso-0", pdTSOUniqueIndexKey, "1").
			AddRow("tso", "tso-0", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db,
		func(ctx context.Context) (map[string]any, error) {
			return map[string]any{
				pdTSOUniqueIndexKey: float64(1),
				pdTSOMaxIndexKey:    float64(4),
			}, nil
		},
		nil,
		nil,
	)

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

	mock.ExpectQuery(regexp.QuoteMeta(showTSOConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("tso", "tso-0", pdTSOUniqueIndexKey, "1").
			AddRow("tso", "tso-0", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db,
		func(ctx context.Context) (map[string]any, error) {
			return map[string]any{
				pdTSOUniqueIndexKey: float64(2),
				pdTSOMaxIndexKey:    float64(5),
			}, nil
		},
		nil,
		nil,
	)

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

	mock.ExpectQuery(regexp.QuoteMeta(showTSOConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("tso", "tso-0", pdTSOUniqueIndexKey, "1").
			AddRow("tso", "tso-0", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db,
		func(ctx context.Context) (map[string]any, error) {
			return map[string]any{
				pdTSOUniqueIndexKey: float64(2),
				pdTSOMaxIndexKey:    float64(4),
			}, nil
		},
		nil,
		nil,
	)

	err = ValidateActiveActiveTSOIndexes(context.Background(), &mockPDClientForTSOIndexValidation{}, &config.ChangefeedConfig{
		ChangefeedID:       common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName),
		SinkURI:            "mysql://127.0.0.1:3306/",
		EnableActiveActive: true,
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestValidateActiveActiveTSOIndexes_DownstreamFallsBackToPDConfig(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(regexp.QuoteMeta(showTSOConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}))
	mock.ExpectQuery(regexp.QuoteMeta(showPDConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("pd", "pd-0", pdTSOUniqueIndexKey, "1").
			AddRow("pd", "pd-0", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db,
		func(ctx context.Context) (map[string]any, error) {
			return map[string]any{
				pdTSOUniqueIndexKey: float64(2),
				pdTSOMaxIndexKey:    float64(4),
			}, nil
		},
		nil,
		nil,
	)

	err = ValidateActiveActiveTSOIndexes(context.Background(), &mockPDClientForTSOIndexValidation{}, &config.ChangefeedConfig{
		ChangefeedID:       common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName),
		SinkURI:            "mysql://127.0.0.1:3306/",
		EnableActiveActive: true,
	})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestValidateActiveActiveTSOIndexes_DownstreamMissingFromTSOAndPD(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(regexp.QuoteMeta(showTSOConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}))
	mock.ExpectQuery(regexp.QuoteMeta(showPDConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}))

	setTestDeps(t, db, nil, nil, nil)

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

func TestValidateActiveActiveTSOIndexes_UpstreamPrefersTSOMicroservice(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(regexp.QuoteMeta(showTSOConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("tso", "tso-0", pdTSOUniqueIndexKey, "1").
			AddRow("tso", "tso-0", pdTSOMaxIndexKey, "4"))

	var readTargets []string
	setTestDeps(t, db,
		func(ctx context.Context) (map[string]any, error) {
			return nil, errors.New("unexpected PD GetConfig call")
		},
		func(ctx context.Context, service string) ([]pdhttp.MicroserviceMember, error) {
			require.Equal(t, tsoServiceName, service)
			return []pdhttp.MicroserviceMember{
				{ServiceAddr: "http://tso-0:2379"},
				{ServiceAddr: "http://tso-1:2379"},
			}, nil
		},
		func(ctx context.Context, httpClient *httputil.Client, targetURL string) (map[string]any, error) {
			readTargets = append(readTargets, targetURL)
			return map[string]any{
				pdTSOUniqueIndexKey: float64(2),
				pdTSOMaxIndexKey:    float64(4),
			}, nil
		},
	)

	err = ValidateActiveActiveTSOIndexes(context.Background(), &mockPDClientForTSOIndexValidation{}, &config.ChangefeedConfig{
		ChangefeedID:       common.NewChangeFeedIDWithName("cf", common.DefaultKeyspaceName),
		SinkURI:            "mysql://127.0.0.1:3306/",
		EnableActiveActive: true,
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"http://tso-0:2379", "http://tso-1:2379"}, readTargets)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestValidateActiveActiveTSOIndexes_UpstreamEmptyTSOMicroserviceMembers(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(regexp.QuoteMeta(showTSOConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("tso", "tso-0", pdTSOUniqueIndexKey, "1").
			AddRow("tso", "tso-0", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db,
		func(ctx context.Context) (map[string]any, error) {
			return nil, errors.New("unexpected PD GetConfig call")
		},
		func(ctx context.Context, service string) ([]pdhttp.MicroserviceMember, error) {
			require.Equal(t, tsoServiceName, service)
			return []pdhttp.MicroserviceMember{}, nil
		},
		nil,
	)

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

func TestValidateActiveActiveTSOIndexes_UpstreamFallsBackWhenTSOMicroserviceUnsupported(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(regexp.QuoteMeta(showTSOConfigQuery)).
		WillReturnRows(sqlmock.NewRows([]string{"Type", "Instance", "Name", "Value"}).
			AddRow("tso", "tso-0", pdTSOUniqueIndexKey, "1").
			AddRow("tso", "tso-0", pdTSOMaxIndexKey, "4"))

	setTestDeps(t, db,
		func(ctx context.Context) (map[string]any, error) {
			return map[string]any{
				pdTSOUniqueIndexKey: float64(2),
				pdTSOMaxIndexKey:    float64(4),
			}, nil
		},
		func(ctx context.Context, service string) ([]pdhttp.MicroserviceMember, error) {
			require.Equal(t, tsoServiceName, service)
			return nil, errTSOMicroserviceUnsupported
		},
		nil,
	)

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
	getMicroserviceMembers func(ctx context.Context, service string) ([]pdhttp.MicroserviceMember, error),
	readTSOConfig func(ctx context.Context, httpClient *httputil.Client, targetURL string) (map[string]any, error),
) {
	t.Helper()

	oldMySQLFn := newMySQLConfigAndDBFn
	oldNewHTTPClientFn := newPDHTTPClientFn
	oldNewTSOConfigHTTPClientFn := newTSOConfigHTTPClientFn
	oldReadTSOConfigFn := readTSOConfigFromURLFn
	oldGetTSOMicroserviceMembersFn := getTSOMicroserviceMembersFn

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
				getMicroserviceMembers: func(ctx context.Context, service string) ([]pdhttp.MicroserviceMember, error) {
					return nil, errors.New("request pd http api failed with status: '404 Not Found', body: 'not support micro service'")
				},
			}, nil
		}
		if getMicroserviceMembers == nil {
			getMicroserviceMembers = func(ctx context.Context, service string) ([]pdhttp.MicroserviceMember, error) {
				return nil, errors.New("request pd http api failed with status: '404 Not Found', body: 'not support micro service'")
			}
		}
		return &mockPDHTTPClient{
			getConfig:              getConfig,
			getMicroserviceMembers: getMicroserviceMembers,
		}, nil
	}
	if readTSOConfig == nil {
		readTSOConfigFromURLFn = func(ctx context.Context, httpClient *httputil.Client, targetURL string) (map[string]any, error) {
			return nil, errors.New("unexpected TSO config read")
		}
	} else {
		readTSOConfigFromURLFn = readTSOConfig
	}
	newTSOConfigHTTPClientFn = func(ctx context.Context) (*httputil.Client, error) {
		return &httputil.Client{}, nil
	}
	if getMicroserviceMembers == nil {
		getTSOMicroserviceMembersFn = func(ctx context.Context, httpClient pdhttp.Client) ([]pdhttp.MicroserviceMember, error) {
			return nil, errTSOMicroserviceUnsupported
		}
	} else {
		getTSOMicroserviceMembersFn = func(ctx context.Context, httpClient pdhttp.Client) ([]pdhttp.MicroserviceMember, error) {
			return getMicroserviceMembers(ctx, tsoServiceName)
		}
	}

	t.Cleanup(func() {
		newMySQLConfigAndDBFn = oldMySQLFn
		newPDHTTPClientFn = oldNewHTTPClientFn
		newTSOConfigHTTPClientFn = oldNewTSOConfigHTTPClientFn
		readTSOConfigFromURLFn = oldReadTSOConfigFn
		getTSOMicroserviceMembersFn = oldGetTSOMicroserviceMembersFn
	})
}
