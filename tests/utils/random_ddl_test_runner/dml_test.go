package main

import (
	"math/rand"
	"strings"
	"testing"
)

func TestRandASCII_IsASCIIAlphaNum(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	s := randASCII(rng, 128)
	for i := 0; i < len(s); i++ {
		b := s[i]
		isNum := b >= '0' && b <= '9'
		isLower := b >= 'a' && b <= 'z'
		isUpper := b >= 'A' && b <= 'Z'
		if !(isNum || isLower || isUpper) {
			t.Fatalf("unexpected byte %q in %q", b, s)
		}
	}
}

func TestMotifInsert_OmitsSiteCode(t *testing.T) {
	tbl := &table{
		db:      "db1",
		name:    "t03",
		isMotif: true,
		schema: tableSchema{
			columns: []column{
				{name: "a", typ: colType{base: "INT"}, nullable: false},
				{name: "b", typ: colType{base: "INT"}, nullable: false},
				{name: "site_code", typ: colType{base: "VARCHAR", varcharN: 64}, nullable: false, defaultSQL: "'100'"},
			},
			primaryKey: []string{"a", "site_code"},
		},
		exists: true,
		nextID: 100,
		frozen: map[int64]struct{}{},
	}
	rng := rand.New(rand.NewSource(1))
	stmt, _, err := buildMotifDML(rng, tbl, 3)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if strings.Contains(stmt, "site_code") {
		t.Fatalf("expected site_code to be omitted, stmt=%s", stmt)
	}
}
