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
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	brokers := flag.String("brokers", "127.0.0.1:9092", "Comma-separated Kafka broker addresses.")
	topic := flag.String("topic", "", "Kafka topic name.")
	timeout := flag.Duration("timeout", 90*time.Second, "How long to wait for matching messages.")
	untilTable := flag.String("until-table", "", "Stop after seeing this many DML messages for the table.")
	untilCount := flag.Int("until-count", 1, "Number of matching table messages required to stop.")
	flag.Parse()

	if *topic == "" {
		log.Fatal("topic must not be empty")
	}
	if *untilTable == "" {
		log.Fatal("until-table must not be empty")
	}
	if *untilCount <= 0 {
		log.Fatal("until-count must be greater than zero")
	}

	config := sarama.NewConfig()
	config.ClientID = "ticdc-integration-test-kafka-dump"
	config.Consumer.Return.Errors = true

	var consumer sarama.Consumer
	deadline := time.Now().Add(*timeout)
	for {
		var err error
		consumer, err = sarama.NewConsumer(strings.Split(*brokers, ","), config)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			log.Fatalf("create Kafka consumer: %v", err)
		}
		time.Sleep(time.Second)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("close Kafka consumer: %v", err)
		}
	}()

	var partitions []int32
	for {
		var err error
		partitions, err = consumer.Partitions(*topic)
		if err == nil && len(partitions) > 0 {
			break
		}
		if time.Now().After(deadline) {
			log.Fatalf("list partitions for %s: %v", *topic, err)
		}
		time.Sleep(time.Second)
	}

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	type rec struct {
		value []byte
	}
	ch := make(chan rec, 32)
	var wg sync.WaitGroup
	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(*topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Fatalf("consume partition %d: %v", partition, err)
		}
		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			defer pc.Close()
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-pc.Messages():
					if !ok {
						return
					}
					select {
					case ch <- rec{value: msg.Value}:
					case <-ctx.Done():
						return
					}
				case err, ok := <-pc.Errors():
					if !ok {
						return
					}
					log.Printf("consume error: %v", err)
				}
			}
		}(pc)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	matched := 0
	for {
		select {
		case <-ctx.Done():
			log.Fatalf("timeout: saw %d DML messages for table %s, want %d", matched, *untilTable, *untilCount)
		case r, ok := <-ch:
			if !ok {
				log.Fatalf("consumers exited: saw %d DML messages for table %s, want %d", matched, *untilTable, *untilCount)
			}
			if _, err := os.Stdout.Write(r.value); err != nil {
				log.Fatalf("write message: %v", err)
			}
			if _, err := os.Stdout.Write([]byte("\n")); err != nil {
				log.Fatalf("write newline: %v", err)
			}
			if tableOf(r.value) == *untilTable {
				matched++
				if matched >= *untilCount {
					return
				}
			}
		}
	}
}

func tableOf(raw []byte) string {
	var msg struct {
		Table string `json:"table"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return ""
	}
	return msg.Table
}
