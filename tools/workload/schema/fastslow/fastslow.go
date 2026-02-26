package fastslow

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"workload/schema"
)

const createTableTemplate = `
CREATE TABLE if not exists %s (
id bigint NOT NULL,
k bigint NOT NULL DEFAULT '0',
c char(30) NOT NULL DEFAULT '',
pad char(20) NOT NULL DEFAULT '',
PRIMARY KEY (id),
KEY k_1 (k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

const (
	padStringLength = 20
	cacheSize       = 100000
)

var (
	cachePadString = make(map[int]string, cacheSize)
	cacheIdx       atomic.Int64
)

func initPadStringCache() {
	if len(cachePadString) >= cacheSize {
		return
	}
	for i := 0; i < cacheSize; i++ {
		cachePadString[i] = genRandomPadString(padStringLength)
	}
	log.Info("Initialized pad string cache",
		zap.Int("cacheSize", cacheSize),
		zap.Int("stringLength", padStringLength))
}

func getPadString() string {
	idx := cacheIdx.Add(1) % int64(cacheSize)
	return cachePadString[int(idx)]
}

type FastSlowWorkload struct {
	tableStartIndex int
	fastTableCount  int
	slowTableCount  int
}

func NewFastSlowWorkload(tableStartIndex, fastTableCount, slowTableCount int) schema.Workload {
	initPadStringCache()
	return &FastSlowWorkload{
		tableStartIndex: tableStartIndex,
		fastTableCount:  fastTableCount,
		slowTableCount:  slowTableCount,
	}
}

func (w *FastSlowWorkload) TableName(tableIndex int) string {
	relative := tableIndex - w.tableStartIndex
	if relative < 0 || relative >= w.fastTableCount+w.slowTableCount {
		return fmt.Sprintf("unknown_%d", tableIndex)
	}
	if relative < w.fastTableCount {
		return fmt.Sprintf("fast_sbtest%d", tableIndex)
	}
	return fmt.Sprintf("slow_sbtest%d", tableIndex)
}

func (w *FastSlowWorkload) BuildCreateTableStatement(n int) string {
	return fmt.Sprintf(createTableTemplate, w.TableName(n))
}

func (w *FastSlowWorkload) BuildInsertSql(tableIndex int, batchSize int) string {
	tableName := w.TableName(tableIndex)
	n := rand.Int63()
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("insert into %s (id, k, c, pad) values(%d, %d, 'abcdefghijklmnopsrstuvwxyzabcd', '%s')",
		tableName, n, n, getPadString()))

	for r := 1; r < batchSize; r++ {
		n = rand.Int63()
		buf.WriteString(fmt.Sprintf(",(%d, %d, 'abcdefghijklmnopsrstuvwxyzabcd', '%s')",
			n, n, getPadString()))
	}
	return buf.String()
}

func (w *FastSlowWorkload) BuildUpdateSql(opt schema.UpdateOption) string {
	if opt.Batch <= 0 {
		return ""
	}
	tableName := w.TableName(opt.TableIndex)
	startID := rand.Int63()
	endID := startID + int64(opt.Batch*100)
	return fmt.Sprintf("update %s set pad = '%s' where id between %d and %d limit %d",
		tableName,
		getPadString(),
		startID,
		endID,
		opt.Batch,
	)
}

func (w *FastSlowWorkload) BuildDeleteSql(opt schema.DeleteOption) string {
	if opt.Batch <= 0 {
		return ""
	}
	tableName := w.TableName(opt.TableIndex)

	switch rand.Intn(3) {
	case 0:
		var buf strings.Builder
		for i := 0; i < opt.Batch; i++ {
			id := rand.Int63()
			if i > 0 {
				buf.WriteString(";")
			}
			buf.WriteString(fmt.Sprintf("DELETE FROM %s WHERE id = %d", tableName, id))
		}
		return buf.String()
	case 1:
		startID := rand.Int63()
		endID := startID + int64(opt.Batch*100)
		return fmt.Sprintf("DELETE FROM %s WHERE id BETWEEN %d AND %d LIMIT %d",
			tableName, startID, endID, opt.Batch)
	case 2:
		kValue := rand.Int63()
		kEnd := kValue + int64(opt.Batch*10)
		return fmt.Sprintf("DELETE FROM %s WHERE k BETWEEN %d AND %d LIMIT %d",
			tableName, kValue, kEnd, opt.Batch)
	default:
		return ""
	}
}

func genRandomPadString(length int) string {
	buf := make([]byte, length)
	for i := 0; i < length; i++ {
		buf[i] = byte(rand.Intn(26) + 97)
	}
	return string(buf)
}
