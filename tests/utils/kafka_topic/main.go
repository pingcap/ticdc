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
	"flag"
	"log"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
)

func main() {
	brokers := flag.String("brokers", "127.0.0.1:9092", "Comma-separated Kafka broker addresses.")
	topic := flag.String("topic", "", "Kafka topic name.")
	maxMessageBytes := flag.Int("max-message-bytes", 0, "Topic max.message.bytes value.")
	alter := flag.Bool("alter", false, "Alter an existing topic instead of creating it.")
	flag.Parse()

	if *topic == "" {
		log.Fatal("topic must not be empty")
	}
	if *maxMessageBytes <= 0 {
		log.Fatal("max-message-bytes must be greater than zero")
	}

	value := strconv.Itoa(*maxMessageBytes)
	config := sarama.NewConfig()
	config.ClientID = "ticdc-integration-test-kafka-topic"
	admin, err := sarama.NewClusterAdmin(strings.Split(*brokers, ","), config)
	if err != nil {
		log.Fatalf("create Kafka admin client: %v", err)
	}
	defer func() {
		if err := admin.Close(); err != nil {
			log.Printf("close Kafka admin client: %v", err)
		}
	}()

	configEntries := map[string]*string{"max.message.bytes": &value}
	if *alter {
		if err := admin.AlterConfig(sarama.TopicResource, *topic, configEntries, false); err != nil {
			log.Fatalf("alter Kafka topic %s: %v", *topic, err)
		}
		return
	}

	detail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
		ConfigEntries:     configEntries,
	}
	if err := admin.CreateTopic(*topic, detail, false); err != nil {
		log.Fatalf("create Kafka topic %s: %v", *topic, err)
	}
}
