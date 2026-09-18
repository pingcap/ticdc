// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := flag.String("brokers", "127.0.0.1:9092", "Comma-separated Kafka broker addresses.")
	topic := flag.String("topic", "", "Kafka topic name.")
	timeout := flag.Duration("timeout", 90*time.Second, "How long to wait for matching messages.")
	untilTable := flag.String("until-table", "", "Stop after seeing this many DML messages for the table.")
	untilCount := flag.Int("until-count", 1, "Number of matching table messages required to stop.")
	registryURL := flag.String("schema-registry-uri", "", "Decode Confluent Avro keys and values using this Schema Registry.")
	flag.Parse()
	avroDecoder := &avroMessageDecoder{
		registryURL: strings.TrimRight(*registryURL, "/"),
		client:      &http.Client{Timeout: 10 * time.Second},
		codecs:      make(map[uint32]*goavro.Codec),
	}

	if *topic == "" {
		log.Fatal("topic must not be empty")
	}
	if *untilTable == "" {
		log.Fatal("until-table must not be empty")
	}
	if *untilCount <= 0 {
		log.Fatal("until-count must be greater than zero")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ClientID("ticdc-integration-test-kafka-dump"),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("create Kafka consumer: %v", err)
	}
	defer client.Close()

	// Wait until the cluster is reachable and the topic is visible before
	// consuming, so a missing cluster or topic fails with a specific error
	// instead of the generic timeout.
	if err := waitFor(ctx, func() error { return client.Ping(ctx) }); err != nil {
		log.Fatalf("create Kafka consumer: %v", err)
	}
	admin := kadm.NewClient(client)
	if err := waitFor(ctx, func() error {
		details, err := admin.ListTopics(ctx, *topic)
		if err != nil {
			return err
		}
		detail, ok := details[*topic]
		if !ok {
			return fmt.Errorf("topic %s not found", *topic)
		}
		if detail.Err != nil {
			return detail.Err
		}
		if len(detail.Partitions) == 0 {
			return fmt.Errorf("topic %s has no partitions", *topic)
		}
		return nil
	}); err != nil {
		log.Fatalf("list partitions for %s: %v", *topic, err)
	}

	matched := 0
	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			log.Fatalf("timeout: saw %d DML messages for table %s, want %d", matched, *untilTable, *untilCount)
		}
		for _, fetchErr := range fetches.Errors() {
			log.Printf("consume error: %v", fetchErr)
		}

		done := false
		fetches.EachRecord(func(record *kgo.Record) {
			if done {
				return
			}
			key, value := record.Key, record.Value
			if *registryURL != "" {
				decoded, err := avroDecoder.decode(ctx, value)
				if err != nil {
					log.Fatalf("decode Avro value: %v", err)
				}
				if len(key) > 0 {
					decodedKey, err := avroDecoder.decode(ctx, key)
					if err != nil {
						log.Fatalf("decode Avro key: %v", err)
					}
					decoded["key"] = decodedKey
				}
				value, err = json.Marshal(decoded)
				if err != nil {
					log.Fatalf("marshal Avro dump: %v", err)
				}
			}
			if _, err := os.Stdout.Write(value); err != nil {
				log.Fatalf("write message: %v", err)
			}
			if _, err := os.Stdout.Write([]byte("\n")); err != nil {
				log.Fatalf("write newline: %v", err)
			}
			if tableOf(value) == *untilTable {
				matched++
				if matched >= *untilCount {
					done = true
				}
			}
		})
		if done {
			return
		}
	}
}

// waitFor retries attempt once per second until it succeeds or ctx expires,
// returning the error from the last attempt.
func waitFor(ctx context.Context, attempt func() error) error {
	for {
		err := attempt()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(time.Second):
		}
	}
}

func tableOf(raw []byte) string {
	var msg struct {
		Table   string `json:"table"`
		Payload struct {
			Op     string `json:"op"`
			Source struct {
				Table string `json:"table"`
			} `json:"source"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return ""
	}
	if msg.Table != "" {
		return msg.Table
	}
	// Debezium also includes source.table in DDL messages. Only count row events.
	switch msg.Payload.Op {
	case "c", "u", "d", "r":
		return msg.Payload.Source.Table
	default:
		return ""
	}
}
