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
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	// Install TiDB AST literal constructors used by parser.New below.
	_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	"go.uber.org/zap"
)

// The only durable journal is the existing per-table schema epoch. Plan stores
// translated, bounded column operations so a pending DDL can resume before DML.
// It is not an applied DML ledger or an owner-fencing mechanism.
type columnDef struct {
	Name        string         `json:"name"`
	Type        string         `json:"type"`
	Nullable    bool           `json:"nullable"`
	Default     string         `json:"default,omitempty"`
	Encoding    columnEncoding `json:"encoding"`
	SourceID    int64          `json:"source_id"`
	DefaultExpr bool           `json:"default_expr,omitempty"`
}
type schemaDef struct {
	Database string      `json:"database"`
	Table    string      `json:"table"`
	Source   string      `json:"source"`
	Columns  []columnDef `json:"columns"`
	Keys     []string    `json:"keys"`
}
type schemaStep struct {
	Kind    string    `json:"kind"`
	OldName string    `json:"old_name,omitempty"`
	Column  columnDef `json:"column"`
}
type schemaPlan struct {
	Kind   string       `json:"kind"`
	Schema schemaDef    `json:"schema"`
	Steps  []schemaStep `json:"steps,omitempty"`
}
type epochRecord struct {
	epoch   uint64
	applied bool
	plan    schemaPlan
}
type tableState struct {
	schema  tableSchema
	epoch   uint64
	writer  *tableWriter
	dropped bool
}
type schemaManager struct {
	client *httpClient
	opts   options
	tables map[string]*tableState
}

func newSchemaManager(c *httpClient, o options) *schemaManager {
	return &schemaManager{client: c, opts: o, tables: make(map[string]*tableState)}
}
func schemaKey(s tableSchema) string { return s.database + "\x00" + s.table }
func toSchemaDef(s tableSchema) schemaDef {
	d := schemaDef{Database: s.database, Table: s.table, Source: s.source, Keys: s.primaryKey}
	for _, c := range s.columns {
		d.Columns = append(d.Columns, columnDef{c.name, c.typ, c.nullable, c.defaultSQL, c.encoding, c.sourceID, c.defaultExpr})
	}
	return d
}

func (d schemaDef) schema() tableSchema {
	s := tableSchema{database: d.Database, table: d.Table, source: d.Source, primaryKey: d.Keys}
	for _, c := range d.Columns {
		s.columns = append(s.columns, column{name: c.Name, typ: c.Type, nullable: c.Nullable, defaultSQL: c.Default, encoding: c.Encoding, sourceID: c.SourceID, defaultExpr: c.DefaultExpr})
	}
	return s
}

func (c columnDef) definition() string {
	result := quoteIdentifier(c.Name) + " " + c.Type
	if c.Nullable {
		result += " NULL"
	} else {
		result += " NOT NULL"
	}
	if c.Default != "" {
		result += " DEFAULT " + c.Default
	}
	return result
}

func (s tableSchema) createTargetSQL() string {
	cols := make([]string, 0, len(s.columns))
	for _, c := range toSchemaDef(s).Columns {
		cols = append(cols, c.definition())
	}
	return "CREATE TABLE IF NOT EXISTS " + s.targetName() + " (" + strings.Join(cols, ", ") + ")"
}

func (s tableSchema) journalName() string {
	return quoteIdentifier(s.database) + "." + quoteIdentifier(s.table+"_cdc_schema_epoch")
}

func (m *schemaManager) exec(ctx context.Context, sql string, args ...any) error {
	result, err := m.client.mutate(ctx, sql, args)
	if result.state == commitUnknown {
		if err == nil {
			err = errors.ErrLakeCommitUnknown.GenWithStackByArgs()
		} else {
			err = errors.WrapError(errors.ErrLakeCommitUnknown, err)
		}
		return errors.WrapChangefeedUnretryableErr(err)
	}
	if err != nil {
		return err
	}
	if result.state != committed {
		return errors.ErrLakeExecution.GenWithStackByArgs()
	}
	return nil
}

func (m *schemaManager) loadEpoch(ctx context.Context, s tableSchema) (epochRecord, error) {
	if err := m.exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdentifier(s.database)); err != nil {
		return epochRecord{}, err
	}
	if err := m.exec(ctx, "CREATE TABLE IF NOT EXISTS "+s.journalName()+" (epoch BIGINT, applied BOOLEAN, plan VARCHAR)"); err != nil {
		return epochRecord{}, err
	}
	rows, err := m.client.selectRows(ctx, "SELECT epoch, applied, plan FROM "+s.journalName(), nil)
	if err != nil {
		return epochRecord{}, err
	}
	if len(rows) == 0 {
		return epochRecord{}, nil
	}
	if len(rows) != 1 || len(rows[0]) != 3 || rows[0][0] == nil || rows[0][1] == nil || rows[0][2] == nil {
		return epochRecord{}, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid schema epoch journal")
	}
	e, err := strconv.ParseUint(*rows[0][0], 10, 64)
	if err != nil || e == 0 || e > math.MaxInt64 {
		return epochRecord{}, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid applied schema epoch")
	}
	applied, err := strconv.ParseBool(*rows[0][1])
	if err != nil {
		return epochRecord{}, errors.WrapError(errors.ErrLakeInvalidData, err, "invalid epoch status")
	}
	r := epochRecord{epoch: e, applied: applied}
	if err := json.Unmarshal([]byte(*rows[0][2]), &r.plan); err != nil {
		return epochRecord{}, errors.WrapError(errors.ErrLakeInvalidData, err, "invalid epoch plan")
	}
	saved := r.plan.Schema.schema()
	if err := saved.validate(); err != nil {
		return epochRecord{}, err
	}
	if saved.database != s.database || saved.table != s.table || saved.source != s.source {
		return epochRecord{}, errors.ErrLakeInvalidData.GenWithStackByArgs("target belongs to another source or schema journal")
	}
	return r, nil
}

func (m *schemaManager) saveEpoch(ctx context.Context, s tableSchema, r epochRecord) error {
	plan, err := json.Marshal(r.plan)
	if err != nil {
		return errors.WrapError(errors.ErrLakeInvalidData, err, "invalid schema plan")
	}
	return m.exec(ctx, "MERGE INTO "+s.journalName()+" t USING (SELECT ?::BIGINT AS epoch, ?::BOOLEAN AS applied, ?::VARCHAR AS plan) s ON true WHEN MATCHED THEN UPDATE * WHEN NOT MATCHED THEN INSERT *", int64(r.epoch), r.applied, string(plan))
}

func canonicalType(t string) string {
	t = strings.ToUpper(strings.ReplaceAll(t, " ", ""))
	if inner, ok := strings.CutPrefix(t, "NULLABLE("); ok {
		t = strings.TrimSuffix(inner, ")")
	}
	switch t {
	case "TINYINTUNSIGNED":
		return "UINT8"
	case "SMALLINTUNSIGNED":
		return "UINT16"
	case "INTUNSIGNED", "INTEGERUNSIGNED":
		return "UINT32"
	case "BIGINTUNSIGNED":
		return "UINT64"
	case "TINYINT":
		return "INT8"
	case "SMALLINT":
		return "INT16"
	case "INT", "INTEGER":
		return "INT32"
	case "BIGINT":
		return "INT64"
	case "STRING", "TEXT":
		return "VARCHAR"
	case "JSON":
		return "VARIANT"
	case "FLOAT":
		return "FLOAT32"
	case "DOUBLE":
		return "FLOAT64"
	}
	return t
}

func (m *schemaManager) targetExists(ctx context.Context, s tableSchema) (bool, error) {
	rows, err := m.client.selectRows(ctx, "SELECT COUNT(*)::VARCHAR FROM system.tables WHERE database = ? AND name = ?", []any{s.database, s.table})
	if err != nil {
		return false, err
	}
	count, err := parseRemoteBytes(rows)
	if err != nil {
		return false, err
	}
	if count > 1 {
		return false, errors.ErrLakeInvalidData.GenWithStackByArgs("ambiguous target table metadata")
	}
	return count == 1, nil
}

func (m *schemaManager) describe(ctx context.Context, s tableSchema) (map[string]columnDef, error) {
	rows, err := m.client.selectRows(ctx, "DESCRIBE "+s.targetName(), nil)
	if err != nil {
		return nil, err
	}
	cols := make(map[string]columnDef, len(rows))
	for _, r := range rows {
		if len(r) < 3 || r[0] == nil || r[1] == nil || r[2] == nil {
			return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("incomplete target column metadata")
		}
		name := *r[0]
		cols[name] = columnDef{Name: name, Type: canonicalType(*r[1]), Nullable: strings.EqualFold(*r[2], "YES") || strings.HasPrefix(strings.ToUpper(*r[1]), "NULLABLE(")}
	}
	return cols, nil
}

func (m *schemaManager) verifyTarget(ctx context.Context, s tableSchema) error {
	cols, err := m.describe(ctx, s)
	if err != nil {
		return err
	}
	if len(cols) != len(s.columns) {
		return errors.ErrLakeInvalidData.GenWithStackByArgs("target column set differs from source metadata")
	}
	for _, c := range s.columns {
		actual, ok := cols[c.name]
		if !ok || canonicalType(actual.Type) != canonicalType(c.typ) || actual.Nullable != c.nullable {
			return errors.ErrLakeInvalidData.GenWithStackByArgs("target type or nullability differs from source projection")
		}
	}
	return nil
}

func (m *schemaManager) initialize(ctx context.Context, s tableSchema) (*tableState, error) {
	if state := m.tables[schemaKey(s)]; state != nil {
		return state, nil
	}
	if len(m.tables) >= m.opts.maxEntries {
		return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("table metadata capacity exhausted")
	}
	r, err := m.loadEpoch(ctx, s)
	if err != nil {
		return nil, err
	}
	if r.epoch > 0 && !r.applied {
		if err := m.finishPlan(ctx, r); err != nil {
			return nil, err
		}
		r.applied = true
	}
	effective := s
	dropped := false
	if r.epoch > 0 {
		effective = r.plan.Schema.schema()
		dropped = r.plan.Kind == "drop"
	}
	if !dropped {
		if err := m.exec(ctx, effective.createTargetSQL()); err != nil {
			return nil, err
		}
		if err := m.verifyTarget(ctx, effective); err != nil {
			return nil, err
		}
		if err := m.exec(ctx, effective.createRawSQL()); err != nil {
			return nil, err
		}
	}
	writer, err := newTableWriter(effective, m.client.mutate, m.opts.batchRows, 3, r.epoch)
	if err != nil {
		return nil, err
	}
	if !dropped {
		rows, err := m.client.selectRows(ctx, "SELECT COALESCE(MAX(add_time)::VARCHAR, '') FROM "+effective.rawName(), nil)
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] == nil {
			return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid retained raw time watermark")
		}
		if text := *rows[0][0]; text != "" {
			stamp, err := time.ParseInLocation("2006-01-02 15:04:05.999999", text, time.UTC)
			if err != nil {
				return nil, errors.WrapError(errors.ErrLakeInvalidData, err, "invalid retained raw timestamp")
			}
			writer.clock.last = stamp
		}
	}
	state := &tableState{schema: effective, epoch: r.epoch, writer: writer, dropped: dropped}
	m.tables[schemaKey(s)] = state
	return state, nil
}

func (m *schemaManager) prepareDML(ctx context.Context, s tableSchema, ts uint64) (*tableState, bool, error) {
	state, err := m.initialize(ctx, s)
	if err != nil {
		return nil, false, err
	}
	if ts <= state.epoch {
		return state, true, nil
	}
	if state.dropped || !reflect.DeepEqual(toSchemaDef(state.schema), toSchemaDef(s)) {
		return nil, false, errors.ErrLakeInvalidData.GenWithStackByArgs("DML schema is not the applied target schema; replay its DDL first")
	}
	return state, false, nil
}

func allowedModify(from, to columnDef) bool {
	a, b := canonicalType(from.Type), canonicalType(to.Type)
	// Match the existing conservative widening family. No implicit narrowing or
	// binary/JSON conversions are inferred from a successful server cast.
	return a == b || a == "INT8" && (b == "INT32" || b == "INT64") || a == "INT32" && (b == "INT64" || b == "VARCHAR")
}

func makeColumnSteps(query string, s tableSchema, current map[string]columnDef) ([]schemaStep, error) {
	stmt, err := parser.New().ParseOneStmt(query, "", "")
	if err != nil {
		return nil, errors.WrapError(errors.ErrLakeInvalidData, err, "cannot parse incremental DDL")
	}
	alter, ok := stmt.(*ast.AlterTableStmt)
	if !ok {
		return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("unsupported column DDL")
	}
	desired := make(map[string]columnDef)
	for _, c := range toSchemaDef(s).Columns {
		desired[strings.ToLower(c.Name)] = c
	}
	foldedCurrent := make(map[string]columnDef, len(current))
	for name, c := range current {
		foldedCurrent[strings.ToLower(name)] = c
	}
	current = foldedCurrent
	isKey := func(name string) bool {
		for _, k := range s.primaryKey {
			if strings.EqualFold(k, name) {
				return true
			}
		}
		return false
	}
	parameterizedCharacterSafe := func(oldName string, def *ast.ColumnDef) bool {
		if desired[strings.ToLower(def.Name.Name.O)].Type != "VARCHAR" || def.Tp.GetFlen() <= 0 {
			return true
		}
		// Preserve bend-cdc's INT -> VARCHAR widening without opening arbitrary
		// character narrowing. Every signed INT32 fits in 11 decimal characters.
		// Same-target VARCHAR changes still cannot prove old source width.
		return canonicalType(current[strings.ToLower(oldName)].Type) == "INT32" &&
			def.Tp.GetType() == mysql.TypeVarchar && def.Tp.GetFlen() >= 11
	}
	var steps []schemaStep
	for _, spec := range alter.Specs {
		add := func(kind, old, name string) error {
			oldKey, nameKey := strings.ToLower(old), strings.ToLower(name)
			c := desired[nameKey]
			if (kind == "add" || kind == "modify") && c.DefaultExpr {
				return errors.ErrLakeInvalidData.GenWithStackByArgs("expression default DDL is unsupported")
			}
			if kind == "drop" {
				c = columnDef{Name: name}
			} else if c.Name == "" {
				return errors.ErrLakeInvalidData.GenWithStackByArgs("DDL references an invisible or missing column")
			}
			if (kind == "drop" || kind == "rename") && (isKey(old) || isKey(name)) {
				return errors.ErrLakeInvalidData.GenWithStackByArgs("changing a conflict key is unsupported")
			}
			prev, present := current[oldKey]
			if present {
				old = prev.Name
			}
			if kind == "drop" {
				c.Name = old
			}
			switch kind {
			case "add":
				if _, ok := current[nameKey]; ok {
					return errors.ErrLakeInvalidData.GenWithStackByArgs("column already exists before new DDL")
				}
				if !c.Nullable && c.Default == "" {
					return errors.ErrLakeInvalidData.GenWithStackByArgs("ADD NOT NULL requires a literal source default")
				}
			case "drop", "rename", "modify":
				if !present {
					return errors.ErrLakeInvalidData.GenWithStackByArgs("old column is missing before new DDL")
				}
			}
			if kind == "modify" && (!allowedModify(prev, c) || (isKey(old) && len(s.primaryKey) > 1)) {
				return errors.ErrLakeInvalidData.GenWithStackByArgs("unsupported type transition or composite key modification")
			}
			if kind == "rename" {
				if _, ok := current[nameKey]; ok {
					return errors.ErrLakeInvalidData.GenWithStackByArgs("rename target already exists")
				}
				delete(current, oldKey)
			}
			switch kind {
			case "drop":
				delete(current, oldKey)
			case "rename":
				prev.Name = c.Name
				current[nameKey] = prev // RENAME does not also change the old type.
			default:
				current[nameKey] = c
			}
			steps = append(steps, schemaStep{Kind: kind, OldName: old, Column: c})
			return nil
		}
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			for _, c := range spec.NewColumns {
				explicitDefault := false
				for _, opt := range c.Options {
					if opt.Tp == ast.ColumnOptionDefaultValue {
						explicitDefault = true
					}
				}
				col := desired[strings.ToLower(c.Name.Name.O)]
				if !explicitDefault && !col.Nullable && col.Type == "VARCHAR" && col.Default == "" {
					col.Default = "''"
					desired[strings.ToLower(c.Name.Name.O)] = col
				}
				if err := add("add", "", c.Name.Name.O); err != nil {
					return nil, err
				}
			}
		case ast.AlterTableDropColumn:
			if err := add("drop", spec.OldColumnName.Name.O, spec.OldColumnName.Name.O); err != nil {
				return nil, err
			}
		case ast.AlterTableRenameColumn:
			if err := add("rename", spec.OldColumnName.Name.O, spec.NewColumnName.Name.O); err != nil {
				return nil, err
			}
		case ast.AlterTableModifyColumn:
			if len(spec.NewColumns) != 1 {
				return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid MODIFY")
			}
			def := spec.NewColumns[0]
			if !parameterizedCharacterSafe(def.Name.Name.O, def) {
				return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("parameterized character MODIFY may narrow source data")
			}
			name := spec.NewColumns[0].Name.Name.O
			if err := add("modify", name, name); err != nil {
				return nil, err
			}
		case ast.AlterTableChangeColumn:
			if len(spec.NewColumns) != 1 {
				return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid CHANGE")
			}
			old, name := spec.OldColumnName.Name.O, spec.NewColumns[0].Name.Name.O
			if !parameterizedCharacterSafe(old, spec.NewColumns[0]) {
				return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("parameterized character CHANGE may narrow source data")
			}
			if old != name {
				if err := add("rename", old, name); err != nil {
					return nil, err
				}
			}
			if err := add("modify", name, name); err != nil {
				return nil, err
			}
		case ast.AlterTableAddConstraint:
			if spec.Constraint == nil || spec.Constraint.Tp == ast.ConstraintPrimaryKey {
				return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("changing primary keys is unsupported")
			}
		case ast.AlterTableDropIndex, ast.AlterTableRenameIndex, ast.AlterTableIndexInvisible: // Secondary indexes have no target projection.
		default:
			return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("unsupported DDL; target has not been changed")
		}
	}
	return steps, nil
}

func (m *schemaManager) finishPlan(ctx context.Context, r epochRecord) error {
	s := r.plan.Schema.schema()
	switch r.plan.Kind {
	case "drop":
		if err := m.exec(ctx, "DROP TABLE IF EXISTS "+s.rawName()); err != nil {
			return err
		}
		if err := m.exec(ctx, "DROP TABLE IF EXISTS "+s.targetName()); err != nil {
			return err
		}
	case "truncate", "create", "alter", "index":
		if err := m.exec(ctx, s.createTargetSQL()); err != nil {
			return err
		}
		if r.plan.Kind == "truncate" {
			if err := m.exec(ctx, "DROP TABLE IF EXISTS "+s.rawName()); err != nil {
				return err
			}
			if err := m.exec(ctx, "TRUNCATE TABLE "+s.targetName()); err != nil {
				return err
			}
		}
		for _, step := range r.plan.Steps {
			cols, err := m.describe(ctx, s)
			if err != nil {
				return err
			}
			old, present := cols[step.OldName]
			newCol, renamed := cols[step.Column.Name]
			suffix := ""
			switch step.Kind {
			case "add":
				if renamed {
					if canonicalType(newCol.Type) != canonicalType(step.Column.Type) || newCol.Nullable != step.Column.Nullable {
						return errors.ErrLakeInvalidData.GenWithStackByArgs("pending ADD has conflicting target representation")
					}
					continue
				}
				suffix = "ADD COLUMN " + step.Column.definition()
			case "drop":
				if !present {
					continue
				}
				suffix = "DROP COLUMN " + quoteIdentifier(step.OldName)
			case "rename":
				if !present && renamed {
					continue
				}
				if !present || renamed {
					return errors.ErrLakeInvalidData.GenWithStackByArgs("pending RENAME is ambiguous")
				}
				suffix = "RENAME COLUMN " + quoteIdentifier(step.OldName) + " TO " + quoteIdentifier(step.Column.Name)
			case "modify":
				if !present || !allowedModify(old, step.Column) {
					return errors.ErrLakeInvalidData.GenWithStackByArgs("pending MODIFY has conflicting target representation")
				}
				suffix = "MODIFY COLUMN " + step.Column.definition()
			default:
				return errors.ErrLakeInvalidData.GenWithStackByArgs("invalid durable DDL operation")
			}
			if err := m.exec(ctx, "ALTER TABLE "+s.targetName()+" "+suffix); err != nil {
				return err
			}
		}
		if err := m.verifyTarget(ctx, s); err != nil {
			return err
		}
		if err := m.exec(ctx, s.createRawSQL()); err != nil {
			return err
		}
	default:
		return errors.ErrLakeInvalidData.GenWithStackByArgs("unsupported durable DDL plan")
	}
	r.applied = true
	return m.saveEpoch(ctx, s, r)
}

func (m *schemaManager) applyDDL(ctx context.Context, e *commonEvent.DDLEvent) error {
	if e.IsBootstrap {
		return nil
	} // Metadata bootstrap is not proof of an applied DDL.
	if e.NotSync || e.TiDBOnly {
		return nil
	}
	if e.FinishedTs == 0 || e.FinishedTs > math.MaxInt64 {
		return errors.ErrLakeInvalidData.GenWithStackByArgs("invalid DDL FinishedTs")
	}
	switch model.ActionType(e.Type) {
	case model.ActionRenameTable, model.ActionRenameTables, model.ActionDropSchema:
		// Compatibility with bend-cdc's non-projecting path. The outer sink
		// has already drained prior DML; only these explicitly opted-out DDLs
		// succeed without a target mutation or an invented applied epoch.
		log.Warn("Lake sink skipped source DDL and kept target objects unchanged",
			zap.String("ddlType", model.ActionType(e.Type).String()),
			zap.String("schema", e.SchemaName), zap.String("table", e.TableName),
			zap.Uint64("finishedTs", e.FinishedTs))
		return nil
	}
	if model.ActionType(e.Type) == model.ActionCreateSchema {
		db := e.SchemaName
		if m.opts.database != "" {
			db = m.opts.database
		}
		if db == "" || strings.ContainsRune(db, 0) {
			return errors.ErrLakeInvalidData.GenWithStackByArgs("invalid schema name")
		}
		return m.exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdentifier(db))
	}
	if e.TableInfo == nil {
		return errors.ErrLakeInvalidData.GenWithStackByArgs("DDL without table metadata is outside the incremental projection")
	}
	s, err := schemaFromNative(e.TableInfo, m.opts.database, m.opts.conflictKey)
	if err != nil {
		return err
	}
	r, err := m.loadEpoch(ctx, s)
	if err != nil {
		return err
	}
	if r.epoch > e.FinishedTs || r.epoch == e.FinishedTs && r.applied {
		return nil
	}
	if r.epoch > 0 && !r.applied {
		if err := m.finishPlan(ctx, r); err != nil {
			return err
		}
		delete(m.tables, schemaKey(s))
		if r.epoch == e.FinishedTs {
			return nil
		}
	}
	plan := schemaPlan{Schema: toSchemaDef(s)}
	switch model.ActionType(e.Type) {
	case model.ActionCreateTable, model.ActionRecoverTable:
		plan.Kind = "create"
	case model.ActionTruncateTable:
		plan.Kind = "truncate"
	case model.ActionDropTable:
		plan.Kind = "drop"
	case model.ActionAddIndex, model.ActionDropIndex, model.ActionRenameIndex, model.ActionAlterIndexVisibility:
		plan.Kind = "index"
	case model.ActionAddColumn, model.ActionDropColumn, model.ActionModifyColumn, model.ActionMultiSchemaChange:
		plan.Kind = "alter"
		exists, err := m.targetExists(ctx, s)
		if err != nil {
			return err
		}
		if !exists {
			// An actual DDL is sufficient authoritative metadata to create an
			// empty projection; this is not historical-table discovery.
			stmt, parseErr := parser.New().ParseOneStmt(e.Query, "", "")
			if parseErr != nil {
				return errors.WrapError(errors.ErrLakeInvalidData, parseErr, "cannot parse incremental DDL")
			}
			alter, ok := stmt.(*ast.AlterTableStmt)
			if !ok {
				return errors.ErrLakeInvalidData.GenWithStackByArgs("unsupported incremental column DDL")
			}
			for _, spec := range alter.Specs {
				switch spec.Tp {
				case ast.AlterTableAddColumns, ast.AlterTableDropColumn, ast.AlterTableModifyColumn, ast.AlterTableChangeColumn, ast.AlterTableRenameColumn:
				default:
					return errors.ErrLakeInvalidData.GenWithStackByArgs("unsupported DDL on missing projection")
				}
			}
			plan.Kind = "create"
			break
		}
		current, err := m.describe(ctx, s)
		if err != nil {
			return err
		}
		plan.Steps, err = makeColumnSteps(e.Query, s, current)
		if err != nil {
			return err
		}
	default:
		return errors.ErrLakeInvalidData.GenWithStackByArgs(fmt.Sprintf("unsupported DDL action %d", e.Type))
	}
	// No new mutation is issued for rejected DDLs. Callers have already drained
	// every affected lane; all old SQL must be terminated before a manual resume.
	r = epochRecord{epoch: e.FinishedTs, plan: plan}
	if err := m.saveEpoch(ctx, s, r); err != nil {
		return err
	}
	if err := m.finishPlan(ctx, r); err != nil {
		return err
	}
	delete(m.tables, schemaKey(s))
	return nil
}
