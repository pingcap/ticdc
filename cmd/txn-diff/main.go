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
	m := make(map[int64]*Column)
	upMap := make(map[int64][]int64)
	downMap := make(map[int64][]int64)
	for i := 0; i < tableCount; i++ {
		table := tableCount + tableStartIndex
		query(upstream, fmt.Sprintf(queryCommitTsSql, tablePrefix, table), func(tso, id int64) {
			if _, ok := m[id]; !ok {
				m[id] = &Column{}
			}
			m[id].commitTs = tso
			upMap[tso] = append(upMap[tso], id)
		})
		query(downstream, fmt.Sprintf(queryStartTsSql, tablePrefix, table), func(tso, id int64) {
			if _, ok := m[id]; !ok {
				m[id] = &Column{}
			}
			m[id].startTs = tso
			downMap[tso] = append(downMap[tso], id)
		})
	}

	// compare
	for _, ids := range upMap {
		for k := 1; k < len(ids); k++ {
			if m[ids[k]].startTs != m[ids[k-1]].startTs {
				log.Panic("compare failed", zap.Any("col", m[ids[k]]), zap.Any("id", ids[k]))
			}
		}
	}
	for _, ids := range downMap {
		for k := 1; k < len(ids); k++ {
			if m[ids[k]].commitTs != m[ids[k-1]].commitTs {
				log.Panic("compare failed", zap.Any("col", m[ids[k]]), zap.Any("id", ids[k]))
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
	var tso, id int64
	err = rows.Scan(&tso, &id)
	if err != nil {
		log.Error("mysql scan failed", zap.Error(err))
		return
	}
	fn(tso, id)
	for rows.Next() {
		err = rows.Scan(&tso, &id)
		if err != nil {
			log.Error("mysql scan failed", zap.Error(err))
			return
		}
		fn(tso, id)
	}
}
