package main

import (
	"math/rand"
	"strings"
	"testing"
)

func TestGenDropColumn_DoesNotDropPrimaryKey(t *testing.T) {
	tbl := &table{
		db:   "db1",
		name: "t00",
		schema: tableSchema{
			columns: []column{
				{name: "id", typ: colType{base: "BIGINT"}, nullable: false},
				{name: "a", typ: colType{base: "INT"}, nullable: false},
				{name: "b", typ: colType{base: "VARCHAR", varcharN: 64}, nullable: false},
			},
			primaryKey: []string{"id"},
		},
		exists: true,
	}
	rng := rand.New(rand.NewSource(1))
	sqlText, _ := genDropColumn(rng, tbl)
	if sqlText == "" {
		t.Fatalf("expected a ddl statement")
	}
	if strings.Contains(sqlText, "`id`") {
		t.Fatalf("expected not to drop pk column, sql=%s", sqlText)
	}
}

func TestGenAddPartition_RequiresRangePartition(t *testing.T) {
	tbl := &table{
		db:   "db1",
		name: "t07",
		schema: tableSchema{
			columns: []column{{name: "id", typ: colType{base: "BIGINT"}, nullable: false}},
		},
		exists: true,
	}
	rng := rand.New(rand.NewSource(1))
	sqlText, _ := genAddPartition(rng, tbl)
	if sqlText != "" {
		t.Fatalf("expected empty ddl for non-partitioned table, got %s", sqlText)
	}
}
