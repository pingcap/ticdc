package main

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type domain string

const (
	domainStable domain = "stable"
	domainChurn  domain = "churn"
	domainSplit  domain = "split_candidate"
)

type colType struct {
	base       string
	varcharN   int
	decimalP   int
	decimalS   int
	varbinaryN int
}

func (t colType) sql() string {
	switch strings.ToUpper(t.base) {
	case "VARCHAR":
		return fmt.Sprintf("VARCHAR(%d)", t.varcharN)
	case "VARBINARY":
		return fmt.Sprintf("VARBINARY(%d)", t.varbinaryN)
	case "DECIMAL":
		return fmt.Sprintf("DECIMAL(%d,%d)", t.decimalP, t.decimalS)
	default:
		return strings.ToUpper(t.base)
	}
}

type column struct {
	name       string
	typ        colType
	nullable   bool
	defaultSQL string
	generated  string
	stored     bool
}

func (c column) sql() string {
	var b strings.Builder
	b.WriteString("`")
	b.WriteString(c.name)
	b.WriteString("` ")
	b.WriteString(c.typ.sql())
	if c.generated != "" {
		kind := "VIRTUAL"
		if c.stored {
			kind = "STORED"
		}
		b.WriteString(" GENERATED ALWAYS AS (")
		b.WriteString(c.generated)
		b.WriteString(") ")
		b.WriteString(kind)
	}
	if !c.nullable {
		b.WriteString(" NOT NULL")
	}
	if c.defaultSQL != "" && c.generated == "" {
		b.WriteString(" DEFAULT ")
		b.WriteString(c.defaultSQL)
	}
	return b.String()
}

type index struct {
	name    string
	columns []string
	unique  bool
}

func (idx index) sql() string {
	var b strings.Builder
	if idx.unique {
		b.WriteString("UNIQUE KEY ")
	} else {
		b.WriteString("KEY ")
	}
	b.WriteString("`")
	b.WriteString(idx.name)
	b.WriteString("` (")
	for i, c := range idx.columns {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("`")
		b.WriteString(c)
		b.WriteString("`")
	}
	b.WriteString(")")
	return b.String()
}

type tableSchema struct {
	columns      []column
	primaryKey   []string
	indexes      []index
	charset      string
	collation    string
	partitionSQL string
}

func (s tableSchema) clone() tableSchema {
	cp := s
	cp.columns = append([]column(nil), s.columns...)
	cp.primaryKey = append([]string(nil), s.primaryKey...)
	cp.indexes = append([]index(nil), s.indexes...)
	return cp
}

func (s tableSchema) createTableSQL(dbName, tableName string) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS `")
	b.WriteString(dbName)
	b.WriteString("`.`")
	b.WriteString(tableName)
	b.WriteString("` (")
	for i, c := range s.columns {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("\n  ")
		b.WriteString(c.sql())
	}
	if len(s.primaryKey) > 0 {
		b.WriteString(",\n  PRIMARY KEY (")
		for i, c := range s.primaryKey {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString("`")
			b.WriteString(c)
			b.WriteString("`")
		}
		b.WriteString(")")
	}
	for _, idx := range s.indexes {
		b.WriteString(",\n  ")
		b.WriteString(idx.sql())
	}
	b.WriteString("\n) ")
	if s.charset != "" {
		b.WriteString("DEFAULT CHARSET=")
		b.WriteString(s.charset)
		b.WriteString(" ")
	}
	if s.collation != "" {
		b.WriteString("COLLATE=")
		b.WriteString(s.collation)
		b.WriteString(" ")
	}
	if s.partitionSQL != "" {
		b.WriteString(s.partitionSQL)
		b.WriteString(" ")
	}
	return b.String()
}

type tableFamily struct {
	id      int
	name    string
	domain  domain
	schema  tableSchema
	isMotif bool
}

func defaultDatabaseNames() []string {
	return []string{"db1", "db2", "db3", "db4", "db5"}
}

func familyName(i int) string {
	return fmt.Sprintf("t%02d", i)
}

func defaultTableFamilies() []tableFamily {
	base := func() tableSchema {
		return tableSchema{
			columns: []column{
				{name: "id", typ: colType{base: "BIGINT"}, nullable: false},
				{name: "a", typ: colType{base: "INT"}, nullable: false},
				{name: "b", typ: colType{base: "VARCHAR", varcharN: 64}, nullable: false},
				{name: "c", typ: colType{base: "DECIMAL", decimalP: 10, decimalS: 2}, nullable: false, defaultSQL: "0"},
				{name: "d", typ: colType{base: "DATETIME"}, nullable: false},
				{name: "e", typ: colType{base: "JSON"}, nullable: true},
				{name: "bin", typ: colType{base: "VARBINARY", varbinaryN: 64}, nullable: true},
			},
			primaryKey: []string{"id"},
		}
	}

	// Motif table family starts with a not-null unique key and will evolve schema during workload.
	motif := tableSchema{
		columns: []column{
			{name: "a", typ: colType{base: "INT"}, nullable: false},
			{name: "b", typ: colType{base: "INT"}, nullable: false},
		},
		indexes: []index{{name: "uk_a", columns: []string{"a"}, unique: true}},
	}

	gbk := base()
	gbk.charset = "gbk"
	gbk.collation = "gbk_bin"

	// Avoid generated columns in baseline schemas because the current storage sink CSV pipeline
	// (cloud storage sink + storage consumer) does not fully support generated columns.
	gen := tableSchema{
		columns: []column{
			{name: "id", typ: colType{base: "BIGINT"}, nullable: false},
			{name: "a", typ: colType{base: "INT"}, nullable: false},
			{name: "b", typ: colType{base: "VARBINARY", varbinaryN: 64}, nullable: false},
			{name: "c", typ: colType{base: "VARCHAR", varcharN: 64}, nullable: false},
		},
		primaryKey: []string{"id"},
	}

	keyless := tableSchema{
		columns: []column{
			{name: "id", typ: colType{base: "BIGINT"}, nullable: false},
			{name: "a", typ: colType{base: "INT"}, nullable: false},
			{name: "b", typ: colType{base: "VARCHAR", varcharN: 64}, nullable: false},
			{name: "c", typ: colType{base: "VARCHAR", varcharN: 128}, nullable: false},
		},
		primaryKey: []string{"id"},
	}

	rangePart := base()
	rangePart.partitionSQL = "PARTITION BY RANGE (`id`) (PARTITION p0 VALUES LESS THAN (1000000000000), PARTITION p1 VALUES LESS THAN (2000000000000), PARTITION p2 VALUES LESS THAN (3000000000000))"

	hashPart := base()
	hashPart.partitionSQL = "PARTITION BY HASH (`id`) PARTITIONS 4"

	split := tableSchema{
		columns: []column{
			{name: "id", typ: colType{base: "BIGINT"}, nullable: false},
			{name: "v", typ: colType{base: "INT"}, nullable: false},
			{name: "pad", typ: colType{base: "VARCHAR", varcharN: 1024}, nullable: false},
			{name: "ts", typ: colType{base: "DATETIME"}, nullable: false},
		},
		primaryKey:   []string{"id"},
		partitionSQL: "PARTITION BY RANGE (`id`) (PARTITION p0 VALUES LESS THAN (1000000000000), PARTITION p1 VALUES LESS THAN (2000000000000), PARTITION p2 VALUES LESS THAN (3000000000000))",
	}

	families := make([]tableFamily, 0, 20)
	for i := 0; i < 20; i++ {
		f := tableFamily{
			id:     i,
			name:   familyName(i),
			domain: domainStable,
			schema: base(),
		}
		switch {
		case i <= 9:
			f.domain = domainStable
		case i <= 15:
			f.domain = domainChurn
		default:
			f.domain = domainSplit
		}

		switch i {
		case 0:
			f.schema = base()
		case 1:
			s := base()
			s.indexes = append(s.indexes, index{name: "uk_b", columns: []string{"b"}, unique: true})
			f.schema = s
		case 2:
			s := base()
			s.indexes = append(s.indexes, index{name: "idx_a", columns: []string{"a"}, unique: false})
			f.schema = s
		case 3:
			f.schema = motif
			f.isMotif = true
		case 4:
			s := base()
			s.indexes = append(s.indexes, index{name: "uk_a_b", columns: []string{"a", "b"}, unique: true})
			f.schema = s
		case 5:
			f.schema = keyless
		case 6:
			f.schema = gen
		case 7:
			f.schema = rangePart
		case 8:
			f.schema = hashPart
		case 9:
			f.schema = gbk
		case 16, 17, 18, 19:
			f.schema = split
		default:
			f.schema = base()
		}
		families = append(families, f)
	}
	return families
}

type table struct {
	// mu guards mutable state (schema, name, exists, nextID, and motif markers).
	mu            sync.Mutex
	db            string
	name          string
	domain        domain
	family        int
	isMotif       bool
	initialSchema tableSchema
	schema        tableSchema
	exists        bool

	nextID int64
	frozen map[int64]struct{}

	hot bool

	rangePartitionNextID    int
	rangePartitionNextBound int64

	motifUnifiedStart int64
}

func (t *table) fqName() string {
	return fmt.Sprintf("`%s`.`%s`", t.db, t.name)
}

type clusterModel struct {
	// clusterModel is an in-memory approximation of the workload surface used for
	// generating DML and DDL. It is updated only when a DDL succeeds on upstream.
	dbs          []string
	tables       []*table
	hotTables    []*table
	coldTables   []*table
	stableTables []*table
	churnTables  []*table
	splitTables  []*table
}

func buildInitialModel(cfg *config) *clusterModel {
	// buildInitialModel constructs the initial schema model used by both bootstrap and workload.
	// It must remain deterministic for a given config so tests can be reproduced by seed.
	dbs := defaultDatabaseNames()
	families := defaultTableFamilies()

	var tables []*table
	for _, dbName := range dbs {
		for _, fam := range families {
			schema := fam.schema.clone()
			tbl := &table{
				db:            dbName,
				name:          fam.name,
				domain:        fam.domain,
				family:        fam.id,
				isMotif:       fam.isMotif,
				initialSchema: schema.clone(),
				schema:        schema,
				exists:        true,
				frozen:        make(map[int64]struct{}),
			}
			if strings.Contains(strings.ToUpper(schema.partitionSQL), "RANGE") {
				// The initial schemas using RANGE partitioning in this runner define p0/p1/p2.
				tbl.rangePartitionNextID = 3
				tbl.rangePartitionNextBound = 3_000_000_000_000
			}
			// Use deterministic initial row ranges.
			baseRows := int64(cfg.Bootstrap.BaseRowsPerTable)
			splitRows := int64(cfg.Bootstrap.SplitRowsPerTable)
			switch fam.domain {
			case domainSplit:
				tbl.nextID = baseRows + splitRows + 1
			default:
				tbl.nextID = baseRows + 1
			}

			// Mark per-db hot tables: one stable + one split candidate.
			if fam.name == "t00" || fam.name == "t16" {
				tbl.hot = true
			}
			tables = append(tables, tbl)
		}
	}

	m := &clusterModel{dbs: dbs, tables: tables}
	for _, t := range tables {
		if t.hot {
			m.hotTables = append(m.hotTables, t)
		} else {
			m.coldTables = append(m.coldTables, t)
		}
		switch t.domain {
		case domainStable:
			m.stableTables = append(m.stableTables, t)
		case domainChurn:
			m.churnTables = append(m.churnTables, t)
		case domainSplit:
			m.splitTables = append(m.splitTables, t)
		}
	}
	return m
}

func (m *clusterModel) pickTableForDML(rng *rand.Rand, hotspotRatio float64) *table {
	// Prefer "hot" tables with a configurable probability to create hotspot pressure.
	if len(m.hotTables) == 0 || len(m.coldTables) == 0 {
		return m.tables[rng.Intn(len(m.tables))]
	}
	if rng.Float64() < hotspotRatio {
		return m.hotTables[rng.Intn(len(m.hotTables))]
	}
	return m.coldTables[rng.Intn(len(m.coldTables))]
}

func (m *clusterModel) pickTableForDomain(rng *rand.Rand, d domain) *table {
	var candidates []*table
	switch d {
	case domainStable:
		candidates = m.stableTables
	case domainChurn:
		candidates = m.churnTables
	case domainSplit:
		candidates = m.splitTables
	default:
		candidates = m.tables
	}
	if len(candidates) == 0 {
		return nil
	}
	// Retry a few times to avoid frequently selecting dropped churn tables.
	for i := 0; i < 10; i++ {
		t := candidates[rng.Intn(len(candidates))]
		t.mu.Lock()
		exists := t.exists
		t.mu.Unlock()
		if exists {
			return t
		}
	}
	return candidates[rng.Intn(len(candidates))]
}

func deterministicInt64(x int64) int64 {
	// A cheap LCG step to mix inputs deterministically.
	const a = 6364136223846793005
	const c = 1442695040888963407
	return a*x + c
}

func asciiStringFromID(prefix string, id int64) string {
	// Stable ASCII-only payload.
	return fmt.Sprintf("%s_%d", prefix, id)
}

func deterministicDecimal(id int64) float64 {
	return math.Mod(float64(deterministicInt64(id)%100000), 10000) / 100.0
}

func deterministicTime(id int64) time.Time {
	// Keep within a reasonable range for MySQL/TiDB.
	base := int64(1700000000)
	return time.Unix(base+(id%86400), 0).UTC()
}
