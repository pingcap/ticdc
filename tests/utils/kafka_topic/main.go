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
	"flag"
	"log"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
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

	ctx := context.Background()
	value := strconv.Itoa(*maxMessageBytes)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ClientID("ticdc-integration-test-kafka-topic"),
	)
	if err != nil {
		log.Fatalf("create Kafka admin client: %v", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	if *alter {
		responses, err := admin.AlterTopicConfigsState(ctx, []kadm.AlterConfig{{
			Name:  "max.message.bytes",
			Value: &value,
		}}, *topic)
		if err != nil {
			log.Fatalf("alter Kafka topic %s: %v", *topic, err)
		}
		response, err := responses.On(*topic, nil)
		if err != nil {
			log.Fatalf("find altered Kafka topic %s response: %v", *topic, err)
		}
		if response.Err != nil {
			log.Fatalf("alter Kafka topic %s: %v", *topic, response.Err)
		}
		return
	}

	responses, err := admin.CreateTopics(ctx, 1, 1, map[string]*string{
		"max.message.bytes": &value,
	}, *topic)
	if err != nil {
		log.Fatalf("create Kafka topic %s: %v", *topic, err)
	}
	response, err := responses.On(*topic, nil)
	if err != nil {
		log.Fatalf("find created Kafka topic %s response: %v", *topic, err)
	}
	if response.Err != nil {
		log.Fatalf("create Kafka topic %s: %v", *topic, response.Err)
	}
}
