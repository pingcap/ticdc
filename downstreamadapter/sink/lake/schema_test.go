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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lake

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestColumnDDLTranslationIsConservative(t *testing.T) {
	s, err := schemaFromNative(nativeTestInfo(), "", "")
	require.NoError(t, err)
	s.columns = append(s.columns, column{name: "extra", typ: "INT64", nullable: true, sourceID: 3})
	current := func() map[string]columnDef {
		return map[string]columnDef{"id": {Name: "id", Type: "INT64"}, "value": {Name: "value", Type: "VARCHAR", Nullable: true}}
	}
	steps, err := makeColumnSteps("ALTER TABLE t ADD COLUMN extra BIGINT", s, current())
	require.NoError(t, err)
	require.Len(t, steps, 1)
	require.Equal(t, "add", steps[0].Kind)
	for _, sql := range []string{"ALTER TABLE t RENAME COLUMN id TO value", "ALTER TABLE t DROP PRIMARY KEY", "ALTER TABLE t ADD COLUMN missing INT", "ALTER TABLE t TRUNCATE PARTITION p0"} {
		_, err := makeColumnSteps(sql, s, current())
		require.Error(t, err, sql)
	}
	s.columns[2].nullable = false
	_, err = makeColumnSteps("ALTER TABLE t ADD COLUMN extra BIGINT NOT NULL", s, current())
	require.Error(t, err)
	s.columns[2].defaultSQL = "'42'"
	_, err = makeColumnSteps("ALTER TABLE t ADD COLUMN extra BIGINT NOT NULL DEFAULT 42", s, current())
	require.NoError(t, err)
	s.columns[2].defaultExpr = true
	_, err = makeColumnSteps("ALTER TABLE t ADD COLUMN extra BIGINT", s, current())
	require.Error(t, err)
}

func TestPendingTruncateRecoveryAndAppliedReplay(t *testing.T) {
	f, server := newAPIFixture(t)
	s := runtimeSink(t, server, "")
	m := s.lanes[0].schemas
	info := nativeTestInfo()
	schema, err := schemaFromNative(info, "", "")
	require.NoError(t, err)
	_, skip, err := m.prepareDML(context.Background(), schema, 100)
	require.NoError(t, err)
	require.False(t, skip)
	f.mu.Lock()
	require.Zero(t, f.epoch, "bootstrap must not invent an applied epoch")
	f.failAfterTruncate = true
	f.mu.Unlock()
	ddl := &commonEvent.DDLEvent{Type: byte(model.ActionTruncateTable), TableInfo: info, FinishedTs: 150}
	require.Error(t, m.applyDDL(context.Background(), ddl))
	f.mu.Lock()
	require.EqualValues(t, 150, f.epoch)
	require.False(t, f.applied)
	require.Equal(t, 1, f.truncates)
	f.mu.Unlock()
	// Simulate a fresh sink only AFTER the old request has a verified terminal
	// state (the fixture failed before saving applied). Transport cancellation is
	// not that proof in production.
	m = newSchemaManager(s.client, s.opts)
	_, skip, err = m.prepareDML(context.Background(), schema, 120)
	require.NoError(t, err)
	require.True(t, skip)
	f.mu.Lock()
	require.True(t, f.applied)
	require.Equal(t, 2, f.truncates)
	f.mu.Unlock()
	_, skip, err = m.prepareDML(context.Background(), schema, 160)
	require.NoError(t, err)
	require.False(t, skip)
	require.NoError(t, m.applyDDL(context.Background(), ddl))
	f.mu.Lock()
	require.Equal(t, 2, f.truncates, "applied replay must not clear newer data")
	f.mu.Unlock()
}

func TestPendingColumnResumeBeforeNewDML(t *testing.T) {
	f, server := newAPIFixture(t)
	s := runtimeSink(t, server, "")
	schema, err := schemaFromNative(nativeTestInfo(), "", "")
	require.NoError(t, err)
	schema.columns = append(schema.columns, column{name: "extra", typ: "INT64", nullable: true, sourceID: 3})
	plan := schemaPlan{Kind: "alter", Schema: toSchemaDef(schema), Steps: []schemaStep{{Kind: "add", Column: columnDef{Name: "extra", Type: "INT64", Nullable: true}}}}
	raw, err := json.Marshal(plan)
	require.NoError(t, err)
	f.mu.Lock()
	f.epoch = 150
	f.applied = false
	f.plan = string(raw)
	f.mu.Unlock()
	m := s.lanes[0].schemas
	_, skip, err := m.prepareDML(context.Background(), schema, 160)
	require.NoError(t, err)
	require.False(t, skip)
	f.mu.Lock()
	require.True(t, f.applied)
	require.Len(t, f.columns, 3)
	f.mu.Unlock()
	_, skip, err = m.prepareDML(context.Background(), schema, 140)
	require.NoError(t, err)
	require.True(t, skip)
}

func TestAddNotNullTextKeepsBaselineImplicitDefault(t *testing.T) {
	s, err := schemaFromNative(nativeTestInfo(), "", "")
	require.NoError(t, err)
	s.columns = append(s.columns, column{name: "text", typ: "VARCHAR", sourceID: 3})
	current := func() map[string]columnDef {
		return map[string]columnDef{"id": {Name: "id", Type: "INT64"}, "value": {Name: "value", Type: "VARCHAR", Nullable: true}}
	}
	steps, err := makeColumnSteps("ALTER TABLE t ADD COLUMN text TEXT NOT NULL", s, current())
	require.NoError(t, err)
	require.Equal(t, "''", steps[0].Column.Default)
	_, err = makeColumnSteps("ALTER TABLE t ADD COLUMN text TEXT NOT NULL DEFAULT NULL", s, current())
	require.Error(t, err)
}

func TestChangeColumnValidatesOldTypeBeforePending(t *testing.T) {
	s, err := schemaFromNative(nativeTestInfo(), "", "")
	require.NoError(t, err)
	s.columns[1].name = "NewName"
	s.columns[1].typ = "INT8"
	current := map[string]columnDef{"id": {Name: "id", Type: "INT64"}, "value": {Name: "value", Type: "INT64", Nullable: true}}
	_, err = makeColumnSteps("ALTER TABLE t CHANGE COLUMN VALUE newname TINYINT", s, current)
	require.Error(t, err, "rename must not masquerade as a completed type change")
	s.columns[1].typ = "INT64"
	current["value"] = columnDef{Name: "value", Type: "INT32", Nullable: true}
	steps, err := makeColumnSteps("ALTER TABLE t CHANGE COLUMN VALUE newname BIGINT", s, current)
	require.NoError(t, err)
	require.Len(t, steps, 2)
	require.Equal(t, "value", steps[0].OldName)
	require.Equal(t, "NewName", steps[0].Column.Name)
	require.Equal(t, "NewName", steps[1].OldName)
}

func TestActualDDLCanCreateMissingProjectionWithoutFullImport(t *testing.T) {
	f, server := newAPIFixture(t)
	f.targetMissing = true
	s := runtimeSink(t, server, "")
	e := &commonEvent.DDLEvent{Type: byte(model.ActionModifyColumn), TableInfo: nativeTestInfo(), FinishedTs: 150, Query: "ALTER TABLE t MODIFY COLUMN value TEXT"}
	require.NoError(t, s.lanes[0].schemas.applyDDL(context.Background(), e))
	f.mu.Lock()
	defer f.mu.Unlock()
	require.False(t, f.targetMissing)
	require.True(t, f.applied)
	require.EqualValues(t, 150, f.epoch)
	var plan schemaPlan
	require.NoError(t, json.Unmarshal([]byte(f.plan), &plan))
	require.Equal(t, "create", plan.Kind)
	require.Empty(t, plan.Steps)
}

// Lake DESCRIBE uses SQL aliases rather than internal UINT* spelling.
func TestCanonicalDescribeUnsignedTypes(t *testing.T) {
	for source, want := range map[string]string{"TINYINT UNSIGNED": "UINT8", "SMALLINT UNSIGNED": "UINT16", "INT UNSIGNED": "UINT32", "BIGINT UNSIGNED": "UINT64", "Nullable(BIGINT UNSIGNED)": "UINT64", "Decimal(65, 30)": "DECIMAL(65,30)"} {
		require.Equal(t, want, canonicalType(source))
	}
}

func TestParameterizedCharacterWideningKeepsBaselineIntTransition(t *testing.T) {
	for _, tc := range []struct {
		query, from string
		allowed     bool
	}{
		{"ALTER TABLE t MODIFY COLUMN value VARCHAR(200) NOT NULL DEFAULT '7'", "INT32", true},
		{"ALTER TABLE t MODIFY COLUMN value VARCHAR(11)", "INT32", true},
		{"ALTER TABLE t MODIFY COLUMN value VARCHAR(10)", "INT32", false},
		{"ALTER TABLE t MODIFY COLUMN value CHAR(200)", "INT32", false},
		{"ALTER TABLE t MODIFY COLUMN value VARCHAR(200)", "VARCHAR", false},
		{"ALTER TABLE t MODIFY COLUMN value VARCHAR(200)", "INT64", false},
		{"ALTER TABLE t CHANGE COLUMN value label VARCHAR(200)", "INT32", true},
	} {
		t.Run(tc.query+"/"+tc.from, func(t *testing.T) {
			schema, err := schemaFromNative(nativeTestInfo(), "", "")
			require.NoError(t, err)
			if strings.Contains(tc.query, "CHANGE") {
				schema.columns[1].name = "label"
			}
			_, err = makeColumnSteps(tc.query, schema, map[string]columnDef{
				"id": {Name: "id", Type: "INT64"}, "value": {Name: "value", Type: tc.from, Nullable: true},
			})
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
