package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"go.uber.org/zap"
)

var (
	upstreamURIStr   string
	downstreamURIStr string
	tableCount       int
	tableStartIndex  int
	queryCommitTsSql = `select commitTs,id from table sbtest%d`
	queryStartTsSql  = `select startTs,id from table sbtest%d`
)

type Column struct {
	commitTs int64
	startTs  int64
}

func validURL(URIStr string) {
	uri, err := url.Parse(URIStr)
	if err != nil {
		log.Error("invalid upstream-uri", zap.Error(err))
		os.Exit(1)
	}
	scheme := strings.ToLower(uri.Scheme)
	if !config.IsMQScheme(scheme) {
		log.Error("invalid scheme, the scheme of upstream-uri must be mysql")
		os.Exit(1)
	}
}
func main() {
	flag.StringVar(&upstreamURIStr, "upstream-uri", "", "storage uri")
	flag.StringVar(&downstreamURIStr, "downstream-uri", "", "downstream sink uri")
	flag.IntVar(&tableCount, "table-count", tableCount, "table count of the workload")
	flag.IntVar(&tableStartIndex, "table-start-index", tableStartIndex, "table start index, sbtest<index>")
	flag.Parse()

	validURL(upstreamURIStr)
	validURL(downstreamURIStr)

	upstream := openDB(upstreamURIStr)
	downstream := openDB(downstreamURIStr)

	m := make(map[int64]*Column)
	upMap := make(map[int64][]int64)
	for i := 0; i < tableCount; i++ {
		table := tableCount + tableStartIndex
		query(upstream, fmt.Sprintf(queryCommitTsSql, table), func(tso, id int64) {
			if _, ok := m[id]; !ok {
				m[id] = &Column{}
			}
			m[id].commitTs = tso
			upMap[tso] = append(upMap[tso], id)
		})
		query(downstream, fmt.Sprintf(queryStartTsSql, table), func(tso, id int64) {
			if _, ok := m[id]; !ok {
				m[id] = &Column{}
			}
			m[id].startTs = tso
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
	log.Info("compare success")
}

func openDB(uri string) *sql.DB {
	db, err := sql.Open("mysql", uri)
	if err != nil {
		log.Error("open mysql failed", zap.Error(err))
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
