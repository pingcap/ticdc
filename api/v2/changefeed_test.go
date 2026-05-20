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

package v2

import (
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestMaskSinkURIForError(t *testing.T) {
	sinkURI := "kafka://127.0.0.1:9092/topic?protocol=canal-json" +
		"&sasl-user=ticdc&sasl-password=verysecure&secret-access-key=rawsecret"

	maskedURI := maskSinkURIForError(sinkURI)
	require.NotContains(t, maskedURI, "verysecure")
	require.NotContains(t, maskedURI, "rawsecret")
	require.Contains(t, maskedURI, "sasl-password=xxxxx")
	require.Contains(t, maskedURI, "secret-access-key=xxxxx")
	require.Contains(t, maskedURI, "sasl-user=ticdc")

	invalidURI := "mysql://root:verysecure@127.0.0.1/%zz"
	require.Equal(t, "<invalid uri>", maskSinkURIForError(invalidURI))

	err := genSinkURIInvalidError(invalidURI, mustParseURLError(t, invalidURI))
	require.NotContains(t, err.Error(), "verysecure")
	require.Contains(t, err.Error(), "<invalid uri>")
	require.Contains(t, err.Error(), `parse "<invalid uri>"`)
	require.Contains(t, err.Error(), "invalid URL escape")
}

func mustParseURLError(t *testing.T, rawURL string) error {
	t.Helper()

	_, err := url.Parse(rawURL)
	require.Error(t, err)
	return err
}

func TestVerifyRouteConflict(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName)
	replicaCfg := config.GetDefaultReplicaConfig()
	replicaCfg.Sink.DispatchRules = []*config.DispatchRule{
		{Matcher: []string{"db1.*"}, TargetSchema: "archive", TargetTable: "{table}"},
		{Matcher: []string{"db2.*"}, TargetSchema: "archive", TargetTable: "{table}"},
	}

	eligibleTables := []common.TableName{{Schema: "db1", Table: "orders"}}
	ineligibleTables := []common.TableName{{Schema: "db2", Table: "orders"}}

	replicaCfg.ForceReplicate = util.AddressOf(false)
	replicaCfg.IgnoreIneligibleTable = util.AddressOf(true)
	require.NoError(t, verifyRouteConflict(changefeedID, eligibleTables, ineligibleTables, replicaCfg))

	replicaCfg.IgnoreIneligibleTable = util.AddressOf(false)
	require.NoError(t, verifyRouteConflict(changefeedID, eligibleTables, ineligibleTables, replicaCfg))

	err := verifyRouteConflict(
		changefeedID,
		[]common.TableName{{Schema: "db1", Table: "orders"}, {Schema: "db2", Table: "orders"}},
		ineligibleTables,
		replicaCfg,
	)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))

	replicaCfg.ForceReplicate = util.AddressOf(true)
	err = verifyRouteConflict(changefeedID, eligibleTables, ineligibleTables, replicaCfg)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))
	require.Contains(t, err.Error(), "target `archive`.`orders`")
	require.Contains(t, err.Error(), "source `db1`.`orders`")
	require.Contains(t, err.Error(), "source `db2`.`orders`")

	replicaCfg.ForceReplicate = util.AddressOf(false)
	replicaCfg.Sink.DispatchRules = []*config.DispatchRule{
		{Matcher: []string{"db2.*"}, TargetSchema: "db1", TargetTable: "{table}"},
	}
	err = verifyRouteConflict(
		changefeedID,
		[]common.TableName{{Schema: "db1", Table: "orders"}, {Schema: "db2", Table: "orders"}},
		nil,
		replicaCfg,
	)
	require.Error(t, err)
	require.True(t, errors.ErrTableRouteConflict.Equal(err))
	require.Contains(t, err.Error(), "target `db1`.`orders`")
	require.Contains(t, err.Error(), "source `db1`.`orders`")
	require.Contains(t, err.Error(), "source `db2`.`orders`")
}
