package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	upstreamURIStr   string
	downstreamURIStr string
	tableCount       int
	tableStartIndex  int
	tablePrefix      string
	queryCommitTsSql = `select commitTs,id from table %s%d`
	queryStartTsSql  = `select startTs,id from table %s%d`
)

type Column struct {
	commitTs int64
	startTs  int64
}

type Pair struct {
	id    int64
	table int
}

func main() {
	flag.StringVar(&upstreamURIStr, "upstream-uri", "root@tcp(127.0.0.1:4000)/test", "upstream uri")
	flag.StringVar(&downstreamURIStr, "downstream-uri", "root@tcp(127.0.0.1:3306)/test", "downstream uri")
	flag.StringVar(&tablePrefix, "tablePrefix", "sbtest", "table name prefix")
	flag.IntVar(&tableCount, "table-count", tableCount, "table count of the workload")
	flag.IntVar(&tableStartIndex, "table-start-index", tableStartIndex, "table start index, sbtest<index>")
	flag.Parse()

	upstream := openDB(upstreamURIStr)
	downstream := openDB(downstreamURIStr)

	defer upstream.Close()
	defer downstream.Close()
	m := make(map[int]map[int64]*Column)
	upMap := make(map[int64][]Pair)
	downMap := make(map[int64][]Pair)
	for i := 0; i < tableCount; i++ {
		table := i + tableStartIndex
		query(upstream, fmt.Sprintf(queryCommitTsSql, tablePrefix, table), func(tso, id int64) {
			if _, ok := m[table]; !ok {
				m[table] = make(map[int64]*Column)
			}
			m[table][id] = &Column{}
			m[table][id].commitTs = tso
			upMap[tso] = append(upMap[tso], Pair{id: id, table: table})
		})
		query(downstream, fmt.Sprintf(queryStartTsSql, tablePrefix, table), func(tso, id int64) {
			if _, ok := m[table]; !ok {
				m[table] = make(map[int64]*Column)
			}
			m[table][id] = &Column{}
			m[table][id].startTs = tso
			downMap[tso] = append(downMap[tso], Pair{id: id, table: table})
		})
	}

	// compare
	for _, ids := range upMap {
		for k := 1; k < len(ids); k++ {
			if m[ids[k].table][ids[k].id].startTs != m[ids[k-1].table][ids[k-1].id].startTs {
				log.Panic("compare failed",
					zap.Any("preCommitTs", m[ids[k-1].table][ids[k-1].id].commitTs),
					zap.Any("curCommitTs", m[ids[k].table][ids[k].id].commitTs),
					zap.Any("preStartTs", m[ids[k-1].table][ids[k-1].id].startTs),
					zap.Any("curStartTs", m[ids[k].table][ids[k].id].startTs),
					zap.Any("preid", ids[k-1].id),
					zap.Any("curid", ids[k]),
					zap.Any("pretable", ids[k-1].table),
					zap.Any("curtable", ids[k].table),
				)
			}
		}
	}
	for _, ids := range downMap {
		for k := 1; k < len(ids); k++ {
			if m[ids[k].table][ids[k].id].commitTs != m[ids[k-1].table][ids[k-1].id].commitTs {
				log.Panic("compare failed",
					zap.Any("preCommitTs", m[ids[k-1].table][ids[k-1].id].commitTs),
					zap.Any("curCommitTs", m[ids[k].table][ids[k].id].commitTs),
					zap.Any("preStartTs", m[ids[k-1].table][ids[k-1].id].startTs),
					zap.Any("curStartTs", m[ids[k].table][ids[k].id].startTs),
					zap.Any("preid", ids[k-1].id),
					zap.Any("curid", ids[k].id),
					zap.Any("pretable", ids[k-1].table),
					zap.Any("curtable", ids[k].table),
				)
			}
		}
	}
	log.Info("compare success")
}

func openDB(dsn string) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Error("open mysql failed", zap.Error(err), zap.Any("dsn", dsn))
		os.Exit(1)
		return nil
	}

	if err := db.Ping(); err != nil {
		log.Error("mysql ping failed", zap.Error(err))
		os.Exit(1)
		return nil
	}
	return db
}

func query(db *sql.DB, query string, fn func(tso, id int64)) {
	rows, err := db.Query(query)
	if err != nil {
		log.Error("mysql query failed", zap.Error(err))
		return
	}
	defer rows.Close()

	var tso, id int64
	for rows.Next() {
		err = rows.Scan(&tso, &id)
		if err != nil {
			log.Error("mysql scan failed", zap.Error(err))
			return
		}
		fn(tso, id)
	}
}
