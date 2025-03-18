// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package bank2

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"workload/schema"
)

const createTableSQL = `CREATE TABLE info (
	col_1 bigint(20) NOT NULL,
	col2 bigint(20) NOT NULL,
	col3 date NOT NULL,
	col4 int(11) NOT NULL DEFAULT '0',
	col5 varchar(16) NOT NULL DEFAULT '',
	col6 varchar(3) NOT NULL DEFAULT '',
	col7 varchar(3) NOT NULL DEFAULT '',
	col8 varchar(3) NOT NULL DEFAULT '',
	col9 varchar(16) NOT NULL DEFAULT '',
	col10 bigint(20) NOT NULL DEFAULT '0',
	col11 date DEFAULT NULL,
	col12 date DEFAULT NULL,
	col13 date DEFAULT NULL,
	col14 decimal(13,2) NOT NULL DEFAULT '0.00',
	col15 decimal(13,2) NOT NULL DEFAULT '0.00',
	col16 decimal(13,2) NOT NULL DEFAULT '0.00',
	col17 decimal(13,2) NOT NULL DEFAULT '0.00',
	col18 decimal(13,2) NOT NULL DEFAULT '0.00',
	col19 decimal(13,2) NOT NULL DEFAULT '0.00',
	col20 decimal(13,2) NOT NULL DEFAULT '0.00',
	col21 decimal(13,2) NOT NULL DEFAULT '0.00',
	col22 decimal(13,2) NOT NULL DEFAULT '0.00',
	col23 int(11) NOT NULL DEFAULT '0',
	col24 smallint(6) NOT NULL,
	col25 decimal(13,0) NOT NULL DEFAULT '0',
	col26 decimal(13,2) NOT NULL DEFAULT '0.00',
	col27 int(11) NOT NULL DEFAULT '0',
	col28 decimal(13,2) NOT NULL DEFAULT '0.00',
	col29 decimal(13,2) NOT NULL DEFAULT '0.00',
	col30 varchar(1) NOT NULL DEFAULT '',
	col31 varchar(1) NOT NULL DEFAULT '',
	col32 varchar(3) NOT NULL DEFAULT '',
	col33 varchar(1) NOT NULL DEFAULT '',
	col34 varchar(1) NOT NULL DEFAULT '',
	col35 decimal(13,2) NOT NULL DEFAULT '0.00',
	col36 decimal(13,2) NOT NULL DEFAULT '0.00',
	col37 varchar(1) NOT NULL DEFAULT '',
	col38 varchar(3) NOT NULL DEFAULT '',
	col39 int(11) NOT NULL DEFAULT '0',
	col40 int(11) NOT NULL DEFAULT '0',
	col41 decimal(13,2) NOT NULL DEFAULT '0.00',
	col42 decimal(13,2) NOT NULL DEFAULT '0.00',
	col43 smallint(6) NOT NULL DEFAULT '0',
	col44 decimal(13,2) NOT NULL DEFAULT '0.00',
	col45 decimal(13,2) NOT NULL DEFAULT '0.00',
	col46 decimal(13,2) NOT NULL DEFAULT '0.00',
	col47 int(11) NOT NULL DEFAULT '0',
	col48 decimal(13,2) NOT NULL DEFAULT '0.00',
	col49 int(11) NOT NULL DEFAULT '0',
	col50 decimal(13,2) NOT NULL DEFAULT '0.00',
	col51 decimal(13,2) NOT NULL DEFAULT '0.00',
	col52 decimal(13,2) NOT NULL DEFAULT '0.00',
	col53 decimal(13,2) NOT NULL DEFAULT '0.00',
	col54 decimal(13,2) NOT NULL DEFAULT '0.00',
	col55 decimal(13,2) NOT NULL DEFAULT '0.00',
	col56 decimal(13,2) NOT NULL DEFAULT '0.00',
	col57 decimal(13,2) NOT NULL DEFAULT '0.00',
	col58 decimal(13,2) NOT NULL DEFAULT '0.00',
	col59 int(11) NOT NULL DEFAULT '0',
	col60 decimal(13,2) NOT NULL DEFAULT '0.00',
	col61 decimal(13,2) NOT NULL DEFAULT '0.00',
	col62 decimal(13,2) NOT NULL DEFAULT '0.00',
	col63 int(11) NOT NULL DEFAULT '0',
	col64 decimal(13,2) NOT NULL DEFAULT '0.00',
	col65 int(11) NOT NULL DEFAULT '0',
	col66 decimal(13,2) NOT NULL DEFAULT '0.00',
	col67 decimal(13,2) NOT NULL DEFAULT '0.00',
	col68 decimal(13,2) NOT NULL DEFAULT '0.00',
	col69 decimal(13,2) NOT NULL DEFAULT '0.00',
	col70 decimal(13,2) NOT NULL DEFAULT '0.00',
	col71 decimal(13,2) NOT NULL DEFAULT '0.00',
	col72 decimal(13,2) NOT NULL DEFAULT '0.00',
	col73 int(11) NOT NULL DEFAULT '0',
	col74 varchar(1) NOT NULL DEFAULT '',
	col75 decimal(13,2) NOT NULL DEFAULT '0.00',
	col76 varchar(1) NOT NULL DEFAULT '',
	col77 decimal(13,2) NOT NULL DEFAULT '0.00',
	col78 decimal(13,2) NOT NULL DEFAULT '0.00',
	col79 int(11) NOT NULL DEFAULT '0',
	col80 date DEFAULT NULL,
	col81 decimal(8,7) NOT NULL DEFAULT '0.0000000',
	col82 decimal(13,2) NOT NULL DEFAULT '0.00',
	col83 decimal(8,7) NOT NULL DEFAULT '0.0000000',
	col84 varchar(37) DEFAULT NULL,
	col85 varchar(4) DEFAULT NULL,
	col86 varchar(32) DEFAULT NULL,
	col87 date DEFAULT NULL,
	col88 date DEFAULT NULL,
	col89 time DEFAULT NULL,
	col90 varchar(10) DEFAULT NULL,
	col91 varchar(10) NOT NULL DEFAULT '',
	col92 varchar(32) NOT NULL DEFAULT '',
	col93 varchar(32) NOT NULL DEFAULT '',
	col94 varchar(10) NOT NULL DEFAULT '',
	col95 varchar(10) NOT NULL DEFAULT '',
	col96 datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	col97 int(11) NOT NULL DEFAULT '0',
	col98 datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	col99 datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
	col100 varchar(4) NOT NULL DEFAULT '',
	col101 varchar(5) NOT NULL DEFAULT '',
	col102 varchar(1) NOT NULL DEFAULT '0',
	col103 varchar(1) NOT NULL DEFAULT '',
	col104 datetime(6) NOT NULL DEFAULT '1900-01-01 00:00:00.000000',
	col105 varchar(1) NOT NULL DEFAULT 'N',
	col106 varchar(256) DEFAULT NULL,
	col107 decimal(13,2) DEFAULT NULL,
	col108 decimal(13,2) DEFAULT NULL,
	col109 decimal(13,2) DEFAULT NULL,
	PRIMARY KEY (col5,col3,col6,col2) /*T![clustered_index] CLUSTERED */,
	KEY col99_col105_idx (col99,col91,col105),
	KEY idx_1 (col9,col3),
	KEY idx_2 (col2,col3)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`

const createLogTableSQL = `CREATE TABLE log (
  col110 bigint(20) NOT NULL,
  col_1 bigint(20) NOT NULL,
  col111 date NOT NULL,
  col4 int(11) NOT NULL DEFAULT '0',
  col5 varchar(16) NOT NULL DEFAULT '',
  col6 varchar(3) NOT NULL DEFAULT '',
  col7 varchar(3) NOT NULL DEFAULT '',
  col8 varchar(3) NOT NULL DEFAULT '',
  col9 varchar(16) NOT NULL DEFAULT '',
  col112 bigint(20) NOT NULL DEFAULT '0',
  col113 varchar(4) NOT NULL DEFAULT '',
  col114 date DEFAULT NULL,
  col115 decimal(13,2) NOT NULL DEFAULT '0.00',
  col116 int(11) NOT NULL DEFAULT '0',
  col117 decimal(8,7) NOT NULL DEFAULT '0.0000000',
  col118 varchar(6) NOT NULL DEFAULT '',
  col119 decimal(13,6) NOT NULL DEFAULT '0.000000',
  col120 decimal(13,6) NOT NULL DEFAULT '0.000000',
  col121 date DEFAULT NULL,
  col122 decimal(13,6) NOT NULL DEFAULT '0.000000',
  col123 varchar(6) NOT NULL DEFAULT '',
  col3 date DEFAULT NULL,
  col84 varchar(37) DEFAULT NULL,
  col85 varchar(4) DEFAULT NULL,
  col86 varchar(32) DEFAULT NULL,
  col87 date NOT NULL,
  col88 date DEFAULT NULL,
  col89 time DEFAULT NULL,
  col90 varchar(10) NOT NULL DEFAULT '',
  col91 varchar(10) NOT NULL DEFAULT '',
  col92 varchar(32) NOT NULL DEFAULT '',
  col93 varchar(32) NOT NULL DEFAULT '',
  col94 varchar(10) NOT NULL DEFAULT '',
  col95 varchar(10) NOT NULL DEFAULT '',
  col96 datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  col97 int(11) NOT NULL DEFAULT '0',
  col98 datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  col99 datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  col124 varchar(4) NOT NULL DEFAULT '',
  col102 varchar(1) NOT NULL DEFAULT '0',
  col125 varchar(1) NOT NULL DEFAULT '',
  col126 varchar(1) NOT NULL DEFAULT '',
  col104 datetime(6) NOT NULL DEFAULT '1900-01-01 00:00:00.000000',
  col105 varchar(1) NOT NULL DEFAULT 'N',
  col106 varchar(256) DEFAULT NULL,
  PRIMARY KEY (col5,col111,col110) /*T![clustered_index] NONCLUSTERED */,
  KEY idx_1 (col5,col3,id),
  KEY idx_2 (col99,col91,col105)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=9 PRE_SPLIT_REGIONS=9 */`

type Bank2Workload struct {
	infoTableInsertSQL string
	logTableInsertSQL  string
}

func NewBank2Workload() schema.Workload {
	var builder strings.Builder
	builder.WriteString("insert into info (col5 , col3 , col6 , col2 ,col35 , col12 , col16 , col107 , col101 , col98 , col73 , col27 , col59 , col26 , col72 , col23 , col81 , col34 , col14 , col95 , col60 , col38 , col33 , col_1 , col31 , col104 , col32 , col85 , col4 , col74 , col102 , col7 , col47 , col29 , col69 , col76 , col75 , col94 , col99 , col20 , col68 , col42 , col25 , col57 , col87 , col82 , col50 , col30 , col83 , col19 , col78 , col43 , col62 , col28 , col11 , col49 , col90 , col56 , col109 , col71 , col15 , col51 , col106 , col53 , col22 , col61 , col91 , col46 , col55 , col108 , col105 , col103 , col63 , col36 , col44 , col88 , col18 , col86 , col10 , col70 , col45 , col64 , col24 , col40 , col84 , col67 , col66 , col17 , col9 ,  col41 , col77 , col54 , col100 , col37 , col21 , col8 , col52 , col48 , col39 , col96 , col80 , col13 , col89 , col79 , col65 , col93 , col97 , col92 , col58 ) values ")
	for r := 0; r < 200; r++ {
		if r != 0 {
			builder.WriteString(",")
		}
		builder.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	}
	infoTableInsertSQL := builder.String()

	builder.Reset()

	builder.WriteString("insert into log (col5 , col111 , id , col84 , col122 , col112 , col121 , col124 , col106 , col125 , col98 , col115 , col116 , col114 , col9 , col94 , col99 , col118 , col95 , col91 , col113 , col87 , col105 , col119 , col_1 , col104 , col8 , col3 , col88 , col96 , col117 ,  col86 , col85 , col4 , col6 , col126 , col102 , col89 , col7 , col120 , col93 , col90 , col97 , col92 , col123 ) values ")
	for r := 0; r < 200; r++ {
		if r != 0 {
			builder.WriteString(",")
		}
		builder.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	}
	logTableInsertSQL := builder.String()

	workload := &Bank2Workload{infoTableInsertSQL: infoTableInsertSQL, logTableInsertSQL: logTableInsertSQL}
	return workload
}

func (c *Bank2Workload) BuildCreateTableStatement(n int) string {
	switch n {
	case 0: // info
		return createTableSQL
	case 1: // log
		return createLogTableSQL
	default:
		panic("unknown table")
	}
}

func (c *Bank2Workload) BuildInsertSql(tableN int, batchSize int) string {
	panic("unimplemented")
}

func (c *Bank2Workload) BuildUpdateSql(opts schema.UpdateOption) string {
	panic("unimplemented")
}

var valuesPool = sync.Pool{
	New: func() interface{} {
		return make([]interface{}, 0, 4)
	},
}

type SqlValue struct {
	Sql    string
	Values []interface{}
}

var largeValuesPool = sync.Pool{
	New: func() interface{} {
		return make([]interface{}, 0, 120*200)
	},
}

func (c *Bank2Workload) BuildInsertSqlWithValues(tableN int, batchSize int) (string, []interface{}) {
	switch tableN {
	case 0: // info
		nonPrimaryKeyValues := generateNonPrimaryValuesForTable() // to reduce time, these field we keep same for
		sql := c.infoTableInsertSQL
		rand.Seed(time.Now().UnixNano())

		values := valuesPool.Get().([]interface{})
		defer valuesPool.Put(values[:0])
		// 200 rows one txn
		for r := 0; r < 200; r++ {
			values = append(values, generatePrimaryValuesForTable()...)
			values = append(values, nonPrimaryKeyValues...)
		}

		return sql, values
	case 1: // log
		sql := c.logTableInsertSQL
		nonPrimaryKeyValues := generateNonPrimaryValuesForLogTable()
		rand.Seed(time.Now().UnixNano())

		values := valuesPool.Get().([]interface{})
		defer valuesPool.Put(values[:0])
		// 200 rows one txn
		for r := 0; r < 200; r++ {
			values = append(values, generatePrimaryValuesForLogTable()...)
			values = append(values, nonPrimaryKeyValues...)
		}

		return sql, values
	default:
		panic("unknown table")
	}
	// return sql, values
}

func (c *Bank2Workload) BuildUpdateSqlWithValues(opts schema.UpdateOption) (string, []interface{}) {
	rand.Seed(time.Now().UnixNano())
	var sql string
	values := make([]interface{}, 0, 120)
	switch opts.Table {
	case 0: // info
		sql = "insert into info (col5 , col3 , col6 , col2 ,col35 , col12 , col16 , col107 , col101 , col98 , col73 , col27 , col59 , col26 , col72 , col23 , col81 , col34 , col14 , col95 , col60 , col38 , col33 , col_1 , col31 , col104 , col32 , col85 , col4 , col74 , col102 , col7 , col47 , col29 , col69 , col76 , col75 , col94 , col99 , col20 , col68 , col42 , col25 , col57 , col87 , col82 , col50 , col30 , col83 , col19 , col78 , col43 , col62 , col28 , col11 , col49 , col90 , col56 , col109 , col71 , col15 , col51 , col106 , col53 , col22 , col61 , col91 , col46 , col55 , col108 , col105 , col103 , col63 , col36 , col44 , col88 , col18 , col86 , col10 , col70 , col45 , col64 , col24 , col40 , col84 , col67 , col66 , col17 , col9 ,  col41 , col77 , col54 , col100 , col37 , col21 , col8 , col52 , col48 , col39 , col96 , col80 , col13 , col89 , col79 , col65 , col93 , col97 , col92 , col58 ) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE col5=VALUES(col5), col3=VALUES(col3), col6=VALUES(col6), col2=VALUES(col2)"
		values = append(values, generatePrimaryValuesForTable()...)
		values = append(values, generateNonPrimaryValuesForTable()...)
	case 1: // log
		sql = "insert into log (col5 , col111 , id , col84 , col122 , col112 , col121 , col124 , col106 , col125 , col98 , col115 , col116 , col114 , col9 , col94 , col99 , col118 , col95 , col91 , col113 , col87 , col105 , col119 , col_1 , col104 , col8 , col3 , col88 , col96 , col117 , col86 , col85 , col4 , col6 , col126 , col102 , col89 , col7 , col120 , col93 , col90 , col97 , col92 , col123 ) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE col5=VALUES(col5), col111=VALUES(col111), id=VALUES(id)"
		values = append(values, generatePrimaryValuesForLogTable()...)
		values = append(values, generateNonPrimaryValuesForLogTable()...)
	default:
		panic("unknown table")
	}
	return sql, values
}

func generateNonPrimaryValuesForTable() []interface{} {
	values := make([]interface{}, 0, 104)
	values = append(values, randomDecimal(13, 2)) // col35
	values = append(values, randomDate())         // col12
	values = append(values, randomDecimal(13, 2)) // col16
	values = append(values, randomDecimal(13, 2)) // col107
	values = append(values, randomString(5))      // col101
	values = append(values, randomDatetime(6))    // col98
	values = append(values, randomInt())          // col73
	values = append(values, randomInt())          // col27
	values = append(values, randomInt())          // col59
	values = append(values, randomDecimal(13, 2)) // col26
	values = append(values, randomDecimal(13, 2)) // col72
	values = append(values, randomInt())          // col23
	values = append(values, randomDecimal(8, 7))  // col81
	values = append(values, randomString(1))      // col34
	values = append(values, randomDecimal(13, 2)) // col14
	values = append(values, randomString(10))     // col95
	values = append(values, randomDecimal(13, 2)) // col60
	values = append(values, randomString(3))      // col38
	values = append(values, randomString(1))      // col33
	values = append(values, randomBigInt())       // col_1
	values = append(values, randomString(1))      // col31
	values = append(values, randomDatetime(6))    // col104
	values = append(values, randomString(3))      // col32
	values = append(values, randomString(4))      // col85
	values = append(values, randomInt())          // col4)
	values = append(values, randomString(1))      // col74
	values = append(values, randomString(1))      // col102
	values = append(values, randomString(3))      // col7
	values = append(values, randomInt())          // col47
	values = append(values, randomDecimal(13, 2)) // col29
	values = append(values, randomDecimal(13, 2)) // col69
	values = append(values, randomString(1))      // col76
	values = append(values, randomDecimal(13, 2)) // col75
	values = append(values, randomString(10))     // col94
	values = append(values, randomDatetime(6))    // col99
	values = append(values, randomDecimal(13, 2)) // col20
	values = append(values, randomDecimal(13, 2)) // col68
	values = append(values, randomDecimal(13, 2)) // col42
	values = append(values, randomDecimal(13, 0)) // col25
	values = append(values, randomDecimal(13, 2)) // col57
	values = append(values, randomDate())         // col87
	values = append(values, randomDecimal(13, 2)) // col82
	values = append(values, randomDecimal(13, 2)) // col50
	values = append(values, randomString(1))      // col30
	values = append(values, randomDecimal(8, 7))  // col83
	values = append(values, randomDecimal(13, 2)) // col19
	values = append(values, randomDecimal(13, 2)) // col78
	values = append(values, randomSmallInt())     // col43
	values = append(values, randomDecimal(13, 2)) // col62
	values = append(values, randomDecimal(13, 2)) // col28
	values = append(values, randomDate())         // col11
	values = append(values, randomInt())          // col49
	values = append(values, randomString(10))     // col90
	values = append(values, randomDecimal(13, 2)) // col56
	values = append(values, randomDecimal(13, 2)) // col109
	values = append(values, randomDecimal(13, 2)) // col71
	values = append(values, randomDecimal(13, 2)) // col15
	values = append(values, randomDecimal(13, 2)) // col51
	values = append(values, randomString(256))    // col106
	values = append(values, randomDecimal(13, 2)) // col53
	values = append(values, randomDecimal(13, 2)) // col22
	values = append(values, randomDecimal(13, 2)) // col61
	values = append(values, randomString(10))     // col91
	values = append(values, randomDecimal(13, 2)) // col46
	values = append(values, randomDecimal(13, 2)) // col55
	values = append(values, randomDecimal(13, 2)) // col108
	values = append(values, randomString(1))      // col105
	values = append(values, randomString(1))      // col103
	values = append(values, randomInt())          // col63
	values = append(values, randomDecimal(13, 2)) // col36
	values = append(values, randomDecimal(13, 2)) // col44
	values = append(values, randomDate())         // col88
	values = append(values, randomDecimal(13, 2)) // col18
	values = append(values, randomString(32))     // col86
	values = append(values, randomBigInt())       // col10
	values = append(values, randomDecimal(13, 2)) // col70
	values = append(values, randomDecimal(13, 2)) // col45
	values = append(values, randomDecimal(13, 2)) // col64
	values = append(values, randomSmallInt())     // col24
	values = append(values, randomInt())          // col40
	values = append(values, randomString(37))     // col84
	values = append(values, randomDecimal(13, 2)) // col67
	values = append(values, randomDecimal(13, 2)) // col66
	values = append(values, randomDecimal(13, 2)) // col17
	values = append(values, randomString(16))     // col9
	values = append(values, randomDecimal(13, 2)) // col41
	values = append(values, randomDecimal(13, 2)) // col77
	values = append(values, randomDecimal(13, 2)) // col54
	values = append(values, randomString(4))      // col100
	values = append(values, randomString(1))      // col37
	values = append(values, randomDecimal(13, 2)) // col21
	values = append(values, randomString(3))      // col8
	values = append(values, randomDecimal(13, 2)) // col52
	values = append(values, randomDecimal(13, 2)) // col48
	values = append(values, randomInt())          // col39
	values = append(values, randomDatetime(6))    // col96
	values = append(values, randomDate())         // col80
	values = append(values, randomDate())         // col13
	values = append(values, randomTime())         // col89
	values = append(values, randomInt())          // col79
	values = append(values, randomInt())          // col65
	values = append(values, randomString(32))     // col93
	values = append(values, randomInt())          // col97
	values = append(values, randomString(32))     // col92
	values = append(values, randomDecimal(13, 2)) // col58

	return values
}

func generatePrimaryValuesForTable() []interface{} {
	values := valuesPool.Get().([]interface{})
	defer valuesPool.Put(values[:0])

	values = append(values, randomString(16)) // col5
	values = append(values, randomDate())     // col3
	values = append(values, randomString(3))  // col6
	values = append(values, randomBigInt())   // col2
	return values
}

func generatePrimaryValuesForLogTable() []interface{} {
	values := make([]interface{}, 0, 3)
	values = append(values, randomString(16)) // col5
	values = append(values, randomDate())     // col111
	values = append(values, randomBigInt())   // id
	return values
}

func generateNonPrimaryValuesForLogTable() []interface{} {
	values := make([]interface{}, 0, 42)
	values = append(values, randomString(37))     // col84
	values = append(values, randomDecimal(13, 6)) // col122
	values = append(values, randomBigInt())       // col112
	values = append(values, randomDate())         // col121
	values = append(values, randomString(4))      // col124
	values = append(values, randomString(256))    // col106
	values = append(values, randomString(1))      // col125
	values = append(values, randomDatetime(6))    // col98
	values = append(values, randomDecimal(13, 2)) // col115
	values = append(values, randomInt())          // col116
	values = append(values, randomDate())         // col114
	values = append(values, randomString(16))     // col9
	values = append(values, randomString(10))     // col94
	values = append(values, randomDatetime(6))    // col99
	values = append(values, randomString(6))      // col118
	values = append(values, randomString(10))     // col95
	values = append(values, randomString(10))     // col91
	values = append(values, randomString(4))      // col113
	values = append(values, randomDate())         // col87
	values = append(values, randomString(1))      // col105
	values = append(values, randomDecimal(13, 6)) // col119
	values = append(values, randomBigInt())       // col_1
	values = append(values, randomDatetime(6))    // col104
	values = append(values, randomString(3))      // col8
	values = append(values, randomDate())         // col3
	values = append(values, randomDate())         // col88
	values = append(values, randomDatetime(6))    // col96
	values = append(values, randomDecimal(8, 7))  // col117
	values = append(values, randomString(32))     // col86
	values = append(values, randomString(4))      // col85
	values = append(values, randomInt())          // col4
	values = append(values, randomString(3))      // col6
	values = append(values, randomString(1))      // col126
	values = append(values, randomString(1))      // col102
	values = append(values, randomTime())         // col89
	values = append(values, randomString(3))      // col7
	values = append(values, randomDecimal(13, 6)) // col120
	values = append(values, randomString(32))     // col93
	values = append(values, randomString(10))     // col90
	values = append(values, randomInt())          // col97
	values = append(values, randomString(32))     // col92
	values = append(values, randomString(6))      // col123

	return values
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func randomDate() string {
	min := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Now().Unix()
	delta := max - min
	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0).Format("2006-01-02")
}

func randomBigInt() int64 {
	min := int64(0)
	max := int64(1<<63 - 1)
	return rand.Int63n(max-min) + min
}

func randomInt() int32 {
	min := int32(0)
	max := int32(1<<31 - 1)
	return rand.Int31n(max-min) + min
}

func randomSmallInt() int16 {
	raw := rand.Int31n(65536)
	smallintValue := int16(raw - 32768)
	return int16(smallintValue)
}

func randomDecimal(precision, scale int) string {
	integerDigits := precision - scale
	maxInteger := int64(1)
	for i := 0; i < integerDigits; i++ {
		maxInteger *= 10
	}
	integerPart := rand.Int63n(maxInteger)

	maxFraction := int64(1)
	for i := 0; i < scale; i++ {
		maxFraction *= 10
	}
	fractionPart := rand.Int63n(maxFraction)

	decimalValue := fmt.Sprintf("%d.%0*d", integerPart, scale, fractionPart)
	return decimalValue
}

func randomDatetime(precision int) string {
	min := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Now().Unix()
	delta := max - min
	sec := rand.Int63n(delta) + min

	format := "2006-01-02 15:04:05"
	switch precision {
	case 1:
		format += ".0"
	case 2:
		format += ".00"
	case 3:
		format += ".000"
	case 4:
		format += ".0000"
	case 5:
		format += ".00000"
	case 6:
		format += ".000000"
	default:
		format += ".000000"
	}

	return time.Unix(sec, 0).Format(format)
}

func randomTime() string {
	start := time.Now().AddDate(-40, 0, 0)
	end := time.Now()
	delta := end.Sub(start)
	randomDuration := time.Duration(rand.Int63n(int64(delta)))
	return start.Add(randomDuration).Format("15:04:05")
}
