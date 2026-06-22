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
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/linkedin/goavro/v2"
)

var requiredChecks = []string{
	"op_insert",
	"op_update",
	"op_delete",
	"tp_int_insert",
	"tp_int_update",
	"tp_int_delete",
	"tp_unsigned_normal_insert",
	"tp_unsigned_max_insert",
	"tp_real_insert",
	"tp_real_update",
	"tp_time_insert",
	"tp_time_update",
	"tp_text_update",
	"tp_blob_update",
	"tp_char_binary_update",
	"tp_other_update",
	"tp_account_delete",
}

type schemaRegistryClient struct {
	baseURL string
	client  *http.Client
	codecs  map[int]*goavro.Codec
}

type rowEvent struct {
	offset int64
	op     string
	table  string
	key    map[string]any
	before map[string]any
	after  map[string]any
}

type kafkaMessage struct {
	offset int64
	key    []byte
	value  []byte
}

type coverage struct {
	checks map[string]bool
	tables map[string]bool
	count  int
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "verify debezium avro failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		topic          = flag.String("topic", "", "Kafka topic to verify")
		kafkaAddr      = flag.String("kafka-addr", "127.0.0.1:9092", "Kafka broker address")
		schemaRegistry = flag.String("schema-registry", "http://127.0.0.1:8088", "Confluent Schema Registry URI")
		timeout        = flag.Duration("timeout", 60*time.Second, "Time to wait for the row events")
	)
	flag.Parse()

	if *topic == "" {
		return errors.New("topic is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	registry := &schemaRegistryClient{
		baseURL: strings.TrimRight(*schemaRegistry, "/"),
		client:  &http.Client{Timeout: 10 * time.Second},
		codecs:  make(map[int]*goavro.Codec),
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_4_0_0
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Net.DialTimeout = 10 * time.Second
	saramaConfig.Net.ReadTimeout = 10 * time.Second
	saramaConfig.Net.WriteTimeout = 10 * time.Second

	consumer, err := sarama.NewConsumer([]string{*kafkaAddr}, saramaConfig)
	if err != nil {
		return fmt.Errorf("create kafka consumer: %w", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(*topic, 0, sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("consume kafka partition: %w", err)
	}
	defer partitionConsumer.Close()

	result := &coverage{
		checks: make(map[string]bool),
		tables: make(map[string]bool),
	}
	var lastErr error
	messagesCh := partitionConsumer.Messages()
	errorsCh := partitionConsumer.Errors()
	for {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("timed out waiting for Debezium Avro row events on topic %s; coverage: %s; last error: %w", *topic, result.summary(), lastErr)
			}
			return fmt.Errorf("timed out waiting for Debezium Avro row events on topic %s; coverage: %s", *topic, result.summary())
		case err, ok := <-errorsCh:
			if ok {
				lastErr = err
			} else {
				errorsCh = nil
			}
		case message, ok := <-messagesCh:
			if !ok {
				if lastErr != nil {
					return fmt.Errorf("kafka message channel closed for topic %s; coverage: %s; last error: %w", *topic, result.summary(), lastErr)
				}
				return fmt.Errorf("kafka message channel closed for topic %s; coverage: %s", *topic, result.summary())
			}

			event, err := decodeRowEvent(ctx, registry, kafkaMessage{
				offset: message.Offset,
				key:    message.Key,
				value:  message.Value,
			})
			if err != nil {
				lastErr = err
				continue
			}
			if event == nil {
				continue
			}
			if err := result.observe(event); err != nil {
				return err
			}
			if result.done() {
				if err := registry.ensureSubject(ctx, *topic+"-key"); err != nil {
					return err
				}
				if err := registry.ensureSubject(ctx, *topic+"-value"); err != nil {
					return err
				}
				fmt.Printf("verified %d Debezium Confluent Avro row events from topic %s; coverage: %s\n", result.count, *topic, result.summary())
				return nil
			}
		}
	}
}

func decodeRowEvent(
	ctx context.Context,
	registry *schemaRegistryClient,
	message kafkaMessage,
) (*rowEvent, error) {
	key, err := decodeConfluentAvro(ctx, registry, message.key)
	if err != nil {
		return nil, fmt.Errorf("decode key at offset %d: %w", message.offset, err)
	}
	value, err := decodeConfluentAvro(ctx, registry, message.value)
	if err != nil {
		return nil, fmt.Errorf("decode value at offset %d: %w", message.offset, err)
	}

	op, ok := value["op"].(string)
	if !ok || (op != "c" && op != "u" && op != "d") {
		return nil, nil
	}

	source, ok := asMap(value["source"])
	if !ok {
		return nil, fmt.Errorf("source is not a record at offset %d: %T", message.offset, value["source"])
	}
	if source["db"] != "test" {
		return nil, fmt.Errorf("unexpected source db at offset %d: %v", message.offset, source["db"])
	}
	table, ok := stringValue(source["table"])
	if !ok {
		return nil, fmt.Errorf("source table is not a string at offset %d: %v", message.offset, source["table"])
	}

	event := &rowEvent{
		offset: message.offset,
		op:     op,
		table:  table,
		key:    key,
	}
	event.before, _ = asMap(value["before"])
	event.after, _ = asMap(value["after"])
	return event, nil
}

func (c *coverage) observe(event *rowEvent) error {
	c.count++
	c.tables[event.table] = true

	switch event.op {
	case "c":
		c.checks["op_insert"] = true
		if event.before != nil || event.after == nil {
			return fmt.Errorf("invalid insert shape at offset %d", event.offset)
		}
	case "u":
		c.checks["op_update"] = true
		if event.before == nil || event.after == nil {
			return fmt.Errorf("invalid update shape at offset %d", event.offset)
		}
	case "d":
		c.checks["op_delete"] = true
		if event.before == nil || event.after != nil {
			return fmt.Errorf("invalid delete shape at offset %d", event.offset)
		}
	}

	id, ok := intValue(event.key["id"])
	if !ok {
		return fmt.Errorf("key id is not an integer at offset %d: %v", event.offset, event.key["id"])
	}

	switch event.table {
	case "tp_int":
		return c.observeInt(event, id)
	case "tp_unsigned_int":
		return c.observeUnsignedInt(event, id)
	case "tp_real":
		return c.observeReal(event, id)
	case "tp_time":
		return c.observeTime(event, id)
	case "tp_text":
		return c.observeText(event, id)
	case "tp_blob":
		return c.observeBlob(event, id)
	case "tp_char_binary":
		return c.observeCharBinary(event, id)
	case "tp_other":
		return c.observeOther(event, id)
	case "tp_account":
		return c.observeAccount(event, id)
	default:
		return fmt.Errorf("unexpected table %s at offset %d", event.table, event.offset)
	}
}

func (c *coverage) observeInt(event *rowEvent, id int64) error {
	switch {
	case event.op == "c" && id == 2:
		if err := expectInt(event.after, "c_tinyint", 1); err != nil {
			return err
		}
		if err := expectInt(event.after, "c_smallint", 2); err != nil {
			return err
		}
		if err := expectInt(event.after, "c_mediumint", 3); err != nil {
			return err
		}
		if err := expectInt(event.after, "c_int", 4); err != nil {
			return err
		}
		if err := expectInt(event.after, "c_bigint", 5); err != nil {
			return err
		}
		c.checks["tp_int_insert"] = true
	case event.op == "u" && id == 2:
		if err := expectInt(event.before, "c_int", 4); err != nil {
			return err
		}
		if err := expectInt(event.after, "c_int", 0); err != nil {
			return err
		}
		c.checks["tp_int_update"] = true
	case event.op == "d" && id == 2:
		if err := expectInt(event.before, "c_int", 0); err != nil {
			return err
		}
		c.checks["tp_int_delete"] = true
	}
	return nil
}

func (c *coverage) observeUnsignedInt(event *rowEvent, id int64) error {
	if event.op != "c" {
		return nil
	}
	switch id {
	case 2:
		if err := expectInt(event.after, "c_unsigned_tinyint", 1); err != nil {
			return err
		}
		if err := expectInt(event.after, "c_unsigned_bigint", 5); err != nil {
			return err
		}
		c.checks["tp_unsigned_normal_insert"] = true
	case 3:
		if err := expectInt(event.after, "c_unsigned_tinyint", 255); err != nil {
			return err
		}
		if err := expectInt(event.after, "c_unsigned_int", 4294967295); err != nil {
			return err
		}
		if err := expectInt(event.after, "c_unsigned_bigint", -1); err != nil {
			return err
		}
		c.checks["tp_unsigned_max_insert"] = true
	}
	return nil
}

func (c *coverage) observeReal(event *rowEvent, id int64) error {
	switch {
	case event.op == "c" && id == 2:
		if err := expectFloat(event.after, "c_double", 2020.0303, 0.000001); err != nil {
			return err
		}
		if err := expectFloat(event.after, "c_decimal", 2020, 0.000001); err != nil {
			return err
		}
		if err := expectFloat(event.after, "c_decimal_2", 2021.1208, 0.000001); err != nil {
			return err
		}
		c.checks["tp_real_insert"] = true
	case event.op == "u" && id == 2:
		if err := expectFloat(event.before, "c_double", 2020.0303, 0.000001); err != nil {
			return err
		}
		if err := expectFloat(event.after, "c_double", 2.333, 0.000001); err != nil {
			return err
		}
		c.checks["tp_real_update"] = true
	}
	return nil
}

func (c *coverage) observeTime(event *rowEvent, id int64) error {
	switch {
	case event.op == "c" && id == 2:
		if err := expectInt(event.after, "c_date", 18312); err != nil {
			return err
		}
		if err := expectString(event.after, "c_timestamp", "2020-02-20T02:20:20Z"); err != nil {
			return err
		}
		if err := expectInt(event.after, "c_year", 2020); err != nil {
			return err
		}
		c.checks["tp_time_insert"] = true
	case event.op == "u" && id == 2:
		if err := expectInt(event.before, "c_year", 2020); err != nil {
			return err
		}
		if err := expectInt(event.after, "c_year", 2022); err != nil {
			return err
		}
		c.checks["tp_time_update"] = true
	}
	return nil
}

func (c *coverage) observeText(event *rowEvent, id int64) error {
	if event.op == "u" && id == 2 {
		if err := expectString(event.after, "c_text", "89504E470D0A1A0B"); err != nil {
			return err
		}
		c.checks["tp_text_update"] = true
	}
	return nil
}

func (c *coverage) observeBlob(event *rowEvent, id int64) error {
	if event.op == "u" && id == 2 {
		if err := expectString(event.after, "c_blob", "iVBORw0KGgs="); err != nil {
			return err
		}
		c.checks["tp_blob_update"] = true
	}
	return nil
}

func (c *coverage) observeCharBinary(event *rowEvent, id int64) error {
	if event.op == "u" && id == 2 {
		if err := expectString(event.after, "c_varchar", "89504E470D0A1A0B"); err != nil {
			return err
		}
		c.checks["tp_char_binary_update"] = true
	}
	return nil
}

func (c *coverage) observeOther(event *rowEvent, id int64) error {
	if event.op == "u" && id == 3 {
		if err := expectString(event.before, "c_enum", "b"); err != nil {
			return err
		}
		if err := expectString(event.after, "c_enum", "c"); err != nil {
			return err
		}
		if err := expectString(event.after, "c_set", "b,c"); err != nil {
			return err
		}
		jsonValue, ok := stringValue(event.after["c_json"])
		if !ok {
			return fmt.Errorf("unexpected c_json value: %v", event.after["c_json"])
		}
		var jsonObject map[string]any
		if err := json.Unmarshal([]byte(jsonValue), &jsonObject); err != nil {
			return fmt.Errorf("decode c_json value: %w", err)
		}
		if jsonObject["key3"] != "123" {
			return fmt.Errorf("unexpected c_json key3: %v", jsonObject["key3"])
		}
		c.checks["tp_other_update"] = true
	}
	return nil
}

func (c *coverage) observeAccount(event *rowEvent, id int64) error {
	if event.op == "d" && id == 12 {
		if err := expectInt(event.before, "account_id", 35); err != nil {
			return err
		}
		c.checks["tp_account_delete"] = true
	}
	return nil
}

func (c *coverage) done() bool {
	for _, check := range requiredChecks {
		if !c.checks[check] {
			return false
		}
	}
	return true
}

func (c *coverage) summary() string {
	var checks []string
	for check := range c.checks {
		checks = append(checks, check)
	}
	sort.Strings(checks)
	var tables []string
	for table := range c.tables {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return fmt.Sprintf("events=%d tables=%v checks=%v", c.count, tables, checks)
}

func decodeConfluentAvro(
	ctx context.Context,
	registry *schemaRegistryClient,
	data []byte,
) (map[string]any, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("message is shorter than Confluent Avro header: %d bytes", len(data))
	}
	if data[0] != 0 {
		return nil, fmt.Errorf("unexpected Confluent Avro magic byte: %d", data[0])
	}

	schemaID := int(binary.BigEndian.Uint32(data[1:5]))
	codec, err := registry.codecByID(ctx, schemaID)
	if err != nil {
		return nil, err
	}
	native, _, err := codec.NativeFromBinary(data[5:])
	if err != nil {
		return nil, err
	}
	result, ok := native.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("decoded Avro payload is %T, expected record", native)
	}
	return result, nil
}

func (c *schemaRegistryClient) codecByID(ctx context.Context, id int) (*goavro.Codec, error) {
	if codec, ok := c.codecs[id]; ok {
		return codec, nil
	}

	body, err := c.get(ctx, fmt.Sprintf("/schemas/ids/%d", id))
	if err != nil {
		return nil, err
	}
	var response struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, err
	}
	if response.Schema == "" {
		return nil, fmt.Errorf("schema registry returned empty schema for id %d", id)
	}

	codec, err := goavro.NewCodecWithOptions(response.Schema, &goavro.CodecOption{EnableStringNull: false})
	if err != nil {
		return nil, err
	}
	c.codecs[id] = codec
	return codec, nil
}

func (c *schemaRegistryClient) ensureSubject(ctx context.Context, subject string) error {
	if _, err := c.get(ctx, "/subjects/"+url.PathEscape(subject)+"/versions/latest"); err != nil {
		return fmt.Errorf("schema registry subject %s is missing: %w", subject, err)
	}
	return nil
}

func (c *schemaRegistryClient) get(ctx context.Context, path string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}

	response, err := c.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned %s: %s", path, response.Status, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func expectInt(row map[string]any, field string, expected int64) error {
	value, ok := intValue(row[field])
	if !ok {
		return fmt.Errorf("%s is not an integer: %v", field, row[field])
	}
	if value != expected {
		return fmt.Errorf("unexpected %s: got %d, want %d", field, value, expected)
	}
	return nil
}

func expectFloat(row map[string]any, field string, expected float64, tolerance float64) error {
	value, ok := floatValue(row[field])
	if !ok {
		return fmt.Errorf("%s is not a float: %v", field, row[field])
	}
	if math.Abs(value-expected) > tolerance {
		return fmt.Errorf("unexpected %s: got %f, want %f", field, value, expected)
	}
	return nil
}

func expectString(row map[string]any, field string, expected string) error {
	value, ok := stringValue(row[field])
	if !ok {
		return fmt.Errorf("%s is not a string: %v", field, row[field])
	}
	if value != expected {
		return fmt.Errorf("unexpected %s: got %q, want %q", field, value, expected)
	}
	return nil
}

func asMap(value any) (map[string]any, bool) {
	unwrapped := unwrapUnion(value)
	result, ok := unwrapped.(map[string]any)
	return result, ok
}

func stringValue(value any) (string, bool) {
	v, ok := unwrapUnion(value).(string)
	return v, ok
}

func intValue(value any) (int64, bool) {
	switch v := unwrapUnion(value).(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint64:
		return int64(v), true
	default:
		return 0, false
	}
}

func floatValue(value any) (float64, bool) {
	switch v := unwrapUnion(value).(type) {
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}

func unwrapUnion(value any) any {
	result, ok := value.(map[string]any)
	if !ok || len(result) != 1 {
		return value
	}
	for _, branchValue := range result {
		return branchValue
	}
	return value
}
