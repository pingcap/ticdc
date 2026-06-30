package main

import (
	"fmt"
	"math/rand"
	"strings"
)

type ddlKind struct {
	// name is used for logging and selector tracking.
	name       string
	domain     domain
	baseWeight float64
	// lossy marks DDLs that can drop data or schema information (e.g., DROP/TRUNCATE/DROP COLUMN).
	// These are still useful for coverage, but are typically constrained to churn domain.
	lossy bool

	// gen returns:
	//   - sql: the DDL statement to execute on upstream.
	//   - apply: a callback that mutates the in-memory model when and only when the DDL succeeds.
	gen func(rng *rand.Rand, t *table) (sql string, apply func())
}

func defaultDDLKinds() []ddlKind {
	// DDL kinds are grouped by domain:
	//   - stable: schema changes that are relatively friendly to snapshot-based diffing.
	//   - churn: destructive or fragile DDLs that can invalidate snapshot reads and diff configs.
	//   - split_candidate: a subset used to stress split/region pressure with larger tables.
	return []ddlKind{
		{
			name:       "add_column",
			domain:     domainStable,
			baseWeight: 5,
			gen:        genAddColumn,
		},
		{
			name:       "add_index",
			domain:     domainStable,
			baseWeight: 4,
			gen:        genAddIndex,
		},
		{
			name:       "convert_charset",
			domain:     domainStable,
			baseWeight: 1,
			gen:        genConvertCharset,
		},
		{
			name:       "add_partition",
			domain:     domainStable,
			baseWeight: 1,
			gen:        genAddPartition,
		},

		// Note: Periodic MySQL syncpoint diffs rely on snapshot reads via sync_diff_inspector.
		// Some schema-changing DDLs (e.g., DROP COLUMN / MODIFY COLUMN / DROP INDEX) are
		// known to be fragile for snapshot-based diffing. Keep them in churn domain to
		// preserve periodic diff stability, while still exercising these DDLs in the workload.
		{
			name:       "drop_column",
			domain:     domainChurn,
			baseWeight: 2,
			lossy:      true,
			gen:        genDropColumn,
		},
		{
			name:       "modify_column_type",
			domain:     domainChurn,
			baseWeight: 1,
			lossy:      true,
			gen:        genModifyColumnType,
		},
		{
			name:       "drop_index",
			domain:     domainChurn,
			baseWeight: 1,
			gen:        genDropIndex,
		},

		// Split candidate tables: use the same DDL set as stable, but target split domain.
		{
			name:       "split_add_index",
			domain:     domainSplit,
			baseWeight: 2,
			gen:        genAddIndex,
		},
		{
			name:       "split_drop_index",
			domain:     domainSplit,
			baseWeight: 1,
			gen:        genDropIndex,
		},
		{
			name:       "split_add_partition",
			domain:     domainSplit,
			baseWeight: 1,
			gen:        genAddPartition,
		},

		// Churn domain: destructive operations.
		{
			name:       "truncate_table",
			domain:     domainChurn,
			baseWeight: 2,
			lossy:      true,
			gen:        genTruncateTable,
		},
		{
			name:       "drop_table",
			domain:     domainChurn,
			baseWeight: 1,
			lossy:      true,
			gen:        genDropTable,
		},
		{
			name:       "recover_table",
			domain:     domainChurn,
			baseWeight: 1,
			gen:        genRecoverTable,
		},
		{
			name:       "drop_and_recreate_table",
			domain:     domainChurn,
			baseWeight: 1,
			lossy:      true,
			gen:        genDropAndRecreateTable,
		},
		{
			name:       "rename_table",
			domain:     domainChurn,
			baseWeight: 1,
			gen:        genRenameTable,
		},
	}
}

func genAddColumn(rng *rand.Rand, t *table) (string, func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.exists {
		return "", nil
	}
	if len(t.schema.columns) > 32 {
		return "", nil
	}
	newName := fmt.Sprintf("c_%d", rng.Intn(10_000_000))
	for _, c := range t.schema.columns {
		if c.name == newName {
			return "", nil
		}
	}
	typ := randDDLColType(rng)
	def := "0"
	if strings.EqualFold(typ.base, "VARCHAR") {
		def = "''"
	}
	sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN `%s` %s NOT NULL DEFAULT %s",
		t.fqName(), newName, typ.sql(), def)
	apply := func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.schema.columns = append(t.schema.columns, column{
			name:       newName,
			typ:        typ,
			nullable:   false,
			defaultSQL: def,
		})
	}
	return sql, apply
}

func genDropColumn(rng *rand.Rand, t *table) (string, func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.exists {
		return "", nil
	}
	var candidates []int
	for i, c := range t.schema.columns {
		if c.generated != "" {
			continue
		}
		if c.name == "id" {
			continue
		}
		if containsString(t.schema.primaryKey, c.name) {
			continue
		}
		candidates = append(candidates, i)
	}
	if len(candidates) == 0 {
		return "", nil
	}
	idx := candidates[rng.Intn(len(candidates))]
	colName := t.schema.columns[idx].name
	sql := fmt.Sprintf("ALTER TABLE %s DROP COLUMN `%s`", t.fqName(), colName)
	apply := func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		if idx >= len(t.schema.columns) || t.schema.columns[idx].name != colName {
			// Best-effort: column order may have changed due to concurrent successful DDL.
			for i, c := range t.schema.columns {
				if c.name == colName {
					idx = i
					break
				}
			}
		}
		if idx >= len(t.schema.columns) || t.schema.columns[idx].name != colName {
			return
		}
		t.schema.columns = append(t.schema.columns[:idx], t.schema.columns[idx+1:]...)
		// Drop indexes referencing the column.
		var newIdx []index
		for _, ix := range t.schema.indexes {
			if containsString(ix.columns, colName) {
				continue
			}
			newIdx = append(newIdx, ix)
		}
		t.schema.indexes = newIdx
	}
	return sql, apply
}

func genModifyColumnType(rng *rand.Rand, t *table) (string, func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.exists {
		return "", nil
	}
	var candidates []int
	for i, c := range t.schema.columns {
		if c.generated != "" {
			continue
		}
		if containsString(t.schema.primaryKey, c.name) {
			continue
		}
		if strings.EqualFold(c.typ.base, "JSON") || strings.EqualFold(c.typ.base, "DATETIME") {
			continue
		}
		candidates = append(candidates, i)
	}
	if len(candidates) == 0 {
		return "", nil
	}
	idx := candidates[rng.Intn(len(candidates))]
	colName := t.schema.columns[idx].name
	newTyp := randDDLColType(rng)
	sql := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN `%s` %s NOT NULL",
		t.fqName(), colName, newTyp.sql())
	apply := func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		for i := range t.schema.columns {
			if t.schema.columns[i].name == colName {
				t.schema.columns[i].typ = newTyp
				return
			}
		}
	}
	return sql, apply
}

func genAddIndex(rng *rand.Rand, t *table) (string, func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.exists {
		return "", nil
	}
	var cols []string
	for _, c := range t.schema.columns {
		if c.generated != "" {
			continue
		}
		if strings.EqualFold(c.typ.base, "JSON") {
			continue
		}
		cols = append(cols, c.name)
	}
	if len(cols) == 0 {
		return "", nil
	}
	col := cols[rng.Intn(len(cols))]
	name := fmt.Sprintf("idx_%s_%d", col, rng.Intn(10_000))
	for _, ix := range t.schema.indexes {
		if ix.name == name {
			return "", nil
		}
	}
	sql := fmt.Sprintf("CREATE INDEX `%s` ON %s (`%s`)", name, t.fqName(), col)
	apply := func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.schema.indexes = append(t.schema.indexes, index{name: name, columns: []string{col}, unique: false})
	}
	return sql, apply
}

func genDropIndex(rng *rand.Rand, t *table) (string, func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.exists {
		return "", nil
	}
	if len(t.schema.indexes) == 0 {
		return "", nil
	}
	ix := t.schema.indexes[rng.Intn(len(t.schema.indexes))]
	sql := fmt.Sprintf("DROP INDEX `%s` ON %s", ix.name, t.fqName())
	apply := func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		var newIdx []index
		for _, x := range t.schema.indexes {
			if x.name == ix.name {
				continue
			}
			newIdx = append(newIdx, x)
		}
		t.schema.indexes = newIdx
	}
	return sql, apply
}

func genConvertCharset(rng *rand.Rand, t *table) (string, func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.exists {
		return "", nil
	}
	targetCharset := "utf8mb4"
	targetCollate := "utf8mb4_bin"
	if !strings.EqualFold(t.schema.charset, "gbk") && rng.Intn(2) == 0 {
		targetCharset = "gbk"
		targetCollate = "gbk_bin"
	}
	sql := fmt.Sprintf("ALTER TABLE %s CONVERT TO CHARACTER SET %s COLLATE %s", t.fqName(), targetCharset, targetCollate)
	apply := func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.schema.charset = targetCharset
		t.schema.collation = targetCollate
	}
	return sql, apply
}

func genAddPartition(rng *rand.Rand, t *table) (string, func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.exists {
		return "", nil
	}
	if !strings.Contains(strings.ToUpper(t.schema.partitionSQL), "RANGE") {
		return "", nil
	}
	if t.rangePartitionNextID <= 0 || t.rangePartitionNextBound <= 0 {
		return "", nil
	}
	pid := t.rangePartitionNextID
	nextBound := t.rangePartitionNextBound + 1_000_000_000_000
	name := fmt.Sprintf("p%d", pid)
	sql := fmt.Sprintf("ALTER TABLE %s ADD PARTITION (PARTITION %s VALUES LESS THAN (%d))",
		t.fqName(), name, nextBound)
	apply := func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.rangePartitionNextID++
		t.rangePartitionNextBound = nextBound
	}
	return sql, apply
}

func genTruncateTable(_ *rand.Rand, t *table) (string, func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.exists {
		return "", nil
	}
	sql := fmt.Sprintf("TRUNCATE TABLE %s", t.fqName())
	return sql, func() {}
}

func genDropTable(_ *rand.Rand, t *table) (string, func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.exists {
		return "", nil
	}
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", t.fqName())
	apply := func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.exists = false
	}
	return sql, apply
}

func genRecoverTable(_ *rand.Rand, t *table) (string, func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.exists {
		return "", nil
	}
	sql := fmt.Sprintf("RECOVER TABLE %s", t.fqName())
	apply := func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.exists = true
	}
	return sql, apply
}

func genDropAndRecreateTable(_ *rand.Rand, t *table) (string, func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s; %s",
		t.fqName(),
		t.initialSchema.createTableSQL(t.db, t.name),
	)
	apply := func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.exists = true
		t.schema = t.initialSchema.clone()
	}
	return sql, apply
}

func genRenameTable(rng *rand.Rand, t *table) (string, func()) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.exists {
		return "", nil
	}

	// Keep the rename logic simple: rename each churn table at most once.
	// This avoids creating long chains of renamed tables and reduces the chance of
	// duplicate rename DDLs under failover.
	if strings.Contains(t.name, "_r_") {
		return "", nil
	}

	newName := fmt.Sprintf("%s_r_%d", t.name, rng.Intn(10_000_000))
	// The max length of a TiDB table name is 64. Keep a small margin.
	if len(newName) > 60 {
		return "", nil
	}
	sql := fmt.Sprintf("RENAME TABLE %s TO `%s`.`%s`", t.fqName(), t.db, newName)
	apply := func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.name = newName
	}
	return sql, apply
}

func randDDLColType(rng *rand.Rand) colType {
	switch rng.Intn(3) {
	case 0:
		return colType{base: "INT"}
	case 1:
		return colType{base: "BIGINT"}
	default:
		return colType{base: "VARCHAR", varcharN: 64}
	}
}

func containsString(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
