// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
// This verifier consumes a bounded, single-partition demo topic from offset zero.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
)

type key struct {
	TS         uint64 `json:"ts"`
	Type       int    `json:"t"`
	Table      string `json:"tbl"`
	ID         string `json:"snapshot_record_id"`
	Snapshot   string `json:"snapshot_id"`
	SnapshotTS uint64 `json:"snapshot_ts"`
	Schema     string `json:"snapshot_schema"`
}
type column struct {
	Value any `json:"v"`
}
type value struct {
	Upsert map[string]column `json:"u"`
	Delete map[string]column `json:"d"`
	Query  string            `json:"q"`
}

func frame(b []byte) ([]byte, []byte) {
	if len(b) < 8 {
		log.Fatal("truncated frame")
	}
	n := binary.BigEndian.Uint64(b)
	if n > uint64(len(b)-8) {
		log.Fatal("invalid frame length")
	}
	return b[8 : 8+n], b[8+n:]
}

func number(v any) int64 {
	n, err := v.(json.Number).Int64()
	if err != nil {
		log.Fatal(err)
	}
	return n
}

func decode(b []byte, out any) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(out); err != nil {
		log.Fatal(err)
	}
}

func main() {
	broker := flag.String("broker", "127.0.0.1:9092", "Kafka broker")
	topic := flag.String("topic", "", "single partition topic")
	out := flag.String("output", "verify.json", "result JSON")
	seed := flag.Int("seed", 3000, "expected unique snapshot rows")
	flag.Parse()
	config := sarama.NewConfig()
	config.Version = sarama.V2_4_0_0
	config.Consumer.Return.Errors = true
	client, err := sarama.NewClient([]string{*broker}, config)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	partitions, err := client.Partitions(*topic)
	if err != nil || len(partitions) != 1 {
		log.Fatalf("expected one partition: %v %v", partitions, err)
	}
	end, err := client.GetOffset(*topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal(err)
	}
	if end == 0 {
		log.Fatal("empty topic")
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	pc, err := consumer.ConsumePartition(*topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatal(err)
	}
	defer pc.Close()
	rows := map[int64]map[string]any{}
	seen := map[string]int64{}
	snapshotRows := 0
	duplicates := 0
	incremental := 0
	ddls := 0
	var boundary uint64
	lastOffset := int64(-1)
	for lastOffset < end-1 {
		select {
		case err := <-pc.Errors():
			log.Fatal(err)
		case <-time.After(60 * time.Second):
			log.Fatal("consumer timeout")
		case msg := <-pc.Messages():
			lastOffset = msg.Offset
			if len(msg.Key) < 8 || binary.BigEndian.Uint64(msg.Key) != 1 {
				log.Fatal("unexpected open protocol version")
			}
			keys, values := msg.Key[8:], msg.Value
			for len(keys) > 0 {
				var kb, vb []byte
				kb, keys = frame(keys)
				vb, values = frame(values)
				var k key
				decode(kb, &k)
				if k.Type == 3 {
					continue
				}
				var v value
				decode(vb, &v)
				if k.Type == 2 {
					ddls++
					// The fixture adds one column with a constant default. Apply
					// its schema effect to already materialized snapshot rows.
					if strings.Contains(strings.ToLower(v.Query), "revision") {
						for _, row := range rows {
							row["revision"] = json.Number("0")
						}
					}
					if boundary != 0 && k.TS <= boundary {
						log.Fatal("DDL at/below B")
					}
					continue
				}
				if k.Table != "accounts" {
					log.Fatalf("unexpected row table %s", k.Table)
				}
				cols := v.Upsert
				if cols == nil {
					cols = v.Delete
				}
				id := number(cols["id"].Value)
				if k.ID != "" {
					if incremental > 0 || ddls > 0 {
						log.Fatal("snapshot output after incremental activation")
					}
					if k.TS != k.SnapshotTS || k.SnapshotTS == 0 || boundary != 0 && boundary != k.TS {
						log.Fatal("snapshot boundary changed")
					}
					boundary = k.TS
					schema, err := base64.StdEncoding.DecodeString(k.Schema)
					if err != nil {
						log.Fatal(err)
					}
					ti, err := common.UnmarshalJSONToTableInfo(schema)
					if err != nil {
						log.Fatal(err)
					}
					rawKey := tablecodec.EncodeRowKeyWithHandle(ti.TableName.TableID, kv.IntHandle(id))
					if protocol.RecordID(k.Snapshot, ti.TableName.TableID, rawKey) != k.ID {
						log.Fatal("record ID differs from domain hash")
					}
					if id < 1 || id > int64(*seed) || number(cols["balance"].Value) != id*10 || cols["note"].Value != fmt.Sprintf("snapshot-%d", id) || len(cols) != 3 {
						log.Fatal("snapshot differs from frozen SQL fixture")
					}
					if old, ok := seen[k.ID]; ok {
						if old != id {
							log.Fatal("record ID collision")
						}
						duplicates++
						continue
					}
					seen[k.ID] = id
					snapshotRows++
				} else {
					if boundary == 0 || k.TS <= boundary {
						log.Fatal("incremental before/at B")
					}
					incremental++
				}
				if v.Delete != nil {
					delete(rows, id)
					continue
				}
				row := map[string]any{}
				for name, c := range v.Upsert {
					row[name] = c.Value
				}
				rows[id] = row
			}
			if len(values) != 0 {
				log.Fatal("extra values")
			}
		}
	}
	if snapshotRows != *seed {
		log.Fatalf("snapshot rows %d != %d", snapshotRows, *seed)
	}
	result := map[string]any{"snapshot_unique_rows": snapshotRows, "snapshot_duplicates": duplicates, "incremental_rows": incremental, "ddl_events": ddls, "snapshot_ts": boundary, "kafka_end_offset": end, "final_rows": rows}
	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err = enc.Encode(result); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("snapshot=%d duplicates=%d incremental=%d ddl=%d final=%d end_offset=%d\n", snapshotRows, duplicates, incremental, ddls, len(rows), end)
}
